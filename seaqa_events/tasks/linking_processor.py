import datetime
import json
import logging
import time
from threading import Thread, Event

from seaqa_events.app.event_redis import RedisClient
from seaqa_events.db import init_db_session_class
from seaqa_events.utils.seadb_api import SeaDBAPI
from seaqa_events.utils.seaqa_db import SeaqaDB
from seaqa_events.utils.constant import ConnectionType
from seaqa_events.seadb_models import SchemaTableNames, get_table_name_from_schema
from seaqa_events.utils import uuid_str_to_32_chars

__all__ = ['LinkingProcessor']

logger = logging.getLogger(__name__)

DATA_EVENTS_CHANNEL = 'data_events'

NO_MSG_TIMEOUT = 60 * 60 * 24
RECONNECT_SLEEP = 1

# Fields from a GitHub issue row worth surfacing in the activity log
GITHUB_ISSUE_DISPLAY_FIELDS = ['title', 'state', 'labels', 'assignees', 'issue_type', 'state_reason']

# Fields from a Discourse topic row worth surfacing in the activity log
DISCOURSE_TOPIC_DISPLAY_FIELDS = ['title', 'category_id', 'resolved']



class LinkingProcessor:
    """Listens to the data_events channel and records activity logs on linked tickets.

    Currently handles:
    - GitHub issue changes → find the linked ticket → insert a ticket activity log.
    - Discourse topic changes → find the linked ticket → insert a ticket activity log.
    - Email thread changes → find the linked ticket → insert a ticket activity log.
    """

    def __init__(self, configs):
        self._configs = configs
        self._enabled = True
        self._parse_config(configs)

    def _parse_config(self, configs):
        proc_config = configs.get('LINKING_PROCESSOR', {})
        if not proc_config:
            return
        self._enabled = proc_config.get('enabled', True)

    def is_enabled(self):
        return self._enabled

    def start(self):
        if not self.is_enabled():
            logger.warning('LinkingProcessor is disabled, skipping start.')
            return
        worker = LinkingWorker(self._configs)
        worker.daemon = True
        worker.start()
        logger.info('LinkingProcessor started.')


class LinkingWorker(Thread):

    def __init__(self, configs):
        super().__init__(name='linking_processor_worker')
        self._redis_client = RedisClient(configs, socket_timeout=10)
        self.seadb_api = SeaDBAPI()
        db_session_class = init_db_session_class(configs)
        self.seaqa_db = SeaqaDB(db_session_class)
        self.finished = Event()

    def run(self):
        self._listen_data_events()

    def cancel(self):
        self.finished.set()

    def _listen_data_events(self):
        subscriber = None
        last_message_time = time.time()

        while not self.finished.is_set():
            if subscriber is None:
                try:
                    subscriber = self._redis_client.get_subscriber(DATA_EVENTS_CHANNEL)
                    last_message_time = time.time()
                    logger.info('LinkingProcessor subscribed to channel: %s', DATA_EVENTS_CHANNEL)
                except Exception as e:
                    logger.warning('Failed to subscribe to %s: %s', DATA_EVENTS_CHANNEL, e)
                    time.sleep(RECONNECT_SLEEP)
                    continue

            try:
                message = subscriber.get_message()
                if not message:
                    if time.time() - last_message_time > NO_MSG_TIMEOUT:
                        logger.warning(
                            'No message received for %s seconds on %s, reconnecting.',
                            NO_MSG_TIMEOUT, DATA_EVENTS_CHANNEL,
                        )
                        try:
                            subscriber.close()
                        except Exception:
                            pass
                        subscriber = None
                    else:
                        time.sleep(RECONNECT_SLEEP)
                    continue

                last_message_time = time.time()
                raw = message.get('data')
                if not isinstance(raw, str):
                    continue
                try:
                    payload = json.loads(raw)
                except Exception:
                    logger.warning('LinkingProcessor: invalid JSON payload, skipping.')
                    continue
                self._handle_event(payload)

            except Exception as e:
                logger.warning('LinkingProcessor listener error: %s', e)
                try:
                    subscriber.close()
                except Exception:
                    pass
                subscriber = None
                time.sleep(RECONNECT_SLEEP)


    def _handle_event(self, data):
        if not isinstance(data, dict):
            return
        source_type = data.get('source_type')
        event = data.get('event')
        if not isinstance(event, dict):
            return

        project_uuid = data.get('project_uuid')
        record_id = data.get('record_id')

        if not project_uuid or record_id is None:
            return

        project_uuid = uuid_str_to_32_chars(project_uuid)
        if source_type == ConnectionType.GITHUB_ISSUE.value:
            connection_id = data.get('connection_id')
            if connection_id is None:
                return
            try:
                self._handle_github_issue_event(project_uuid, record_id, connection_id, event)
            except Exception as e:
                logger.error('failed to handle github_issue event (project=%s record=%s): %s', project_uuid, record_id, e)

        elif source_type == ConnectionType.DISCOURSE_FORUM.value:
            connection_id = data.get('connection_id')
            if connection_id is None:
                return
            try:
                self._handle_discourse_topic_event(project_uuid, record_id, connection_id, event)
            except Exception as e:
                logger.error('failed to handle discourse_topic event (project=%s record=%s): %s', project_uuid, record_id, e)

        elif source_type == ConnectionType.EMAIL.value:
            connection_id = data.get('connection_id')
            if connection_id is None:
                return
            try:
                self._handle_email_thread_event(project_uuid, record_id, connection_id, event)
            except Exception as e:
                logger.error('failed to handle email event (project=%s record=%s): %s', project_uuid, record_id, e)

    def _load_linked_record_event_context(self, project_uuid, record_id, table_name, select_fields, event):
        try:
            record_id = int(record_id)
        except (ValueError, TypeError):
            logger.warning('invalid record_id %r for project=%s, skipping.', record_id, project_uuid)
            return None

        sql = (
            f"SELECT {select_fields} "
            f"FROM `{table_name}` WHERE `_pk` = {record_id} LIMIT 1"
        )
        results = self.seadb_api.query_rows(project_uuid, sql).get('results', [])
        if not results:
            logger.warning('record not found for record_id=%s in %s', record_id, table_name)
            return None

        row_data = results[0]
        linked_ticket = row_data.get('linked_ticket')
        if not linked_ticket:
            return None
        try:
            linked_ticket = int(linked_ticket)
        except (ValueError, TypeError):
            logger.warning('invalid linked_ticket %r for record_id=%s, skipping.', linked_ticket, record_id)
            return None

        event_type = event.get('type', '')
        old_all, new_all = self._extract_display_values(event_type, event)
        return record_id, row_data, linked_ticket, event_type, old_all, new_all

    def _insert_ticket_activity_rows(self, project_uuid, linked_ticket, event_type, old_all, new_all, base_detail):
        now = datetime.datetime.now(datetime.UTC).isoformat()
        rows = self._build_activity_rows(linked_ticket, event_type, old_all, new_all, base_detail, now)
        if not rows:
            return 0
        self.seadb_api.insert_rows(project_uuid, get_table_name_from_schema(SchemaTableNames.TICKET_ACTIVITIES), rows)
        return len(rows)

    def _handle_github_issue_event(self, project_uuid, record_id, connection_id, event):
        issues_table = get_table_name_from_schema(SchemaTableNames.GITHUB_ISSUES, connection_id)
        select_fields = '`linked_ticket`, `issue_number`, `title`, `url`'
        context = self._load_linked_record_event_context(project_uuid, record_id, issues_table, select_fields, event)
        if not context:
            return

        record_id, row_data, linked_ticket, event_type, old_all, new_all = context
        base_detail = {
            'connection_id': connection_id,
            'record_id': record_id,
            'issue_number': row_data.get('issue_number'),
            'issue_title': row_data.get('title', ''),
            'issue_url': row_data.get('url', ''),
        }
        inserted = self._insert_ticket_activity_rows(
            project_uuid, linked_ticket, event_type, old_all, new_all, base_detail
        )
        if not inserted:
            return
        logger.info('inserted %d activity rows for ticket %s', inserted, linked_ticket)

    def _handle_discourse_topic_event(self, project_uuid, record_id, connection_id, event):
        topics_table = get_table_name_from_schema(SchemaTableNames.DISCOURSE_TOPICS, connection_id)
        select_fields = '`linked_ticket`, `topic_id`, `title`, `slug`'
        context = self._load_linked_record_event_context(project_uuid, record_id, topics_table, select_fields, event)
        if not context:
            return
        record_id, row_data, linked_ticket, event_type, old_all, new_all = context

        # Build topic_url from connection config
        topic_url = ''
        try:
            connection = self.seaqa_db.get_project_connection_by_id(connection_id)
            if connection:
                config = json.loads(connection.config) if connection.config else {}
                forum_url = config.get('url', '')
                slug = row_data.get('slug', '')
                topic_id = row_data.get('topic_id', '')
                if forum_url and slug and topic_id:
                    topic_url = forum_url.rstrip('/') + '/t/' + slug + '/' + str(topic_id)
        except Exception as e:
            logger.warning('failed to build topic_url for connection_id=%s: %s', connection_id, e)

        base_detail = {
            'connection_id': connection_id,
            'record_id': record_id,
            'topic_id': row_data.get('topic_id'),
            'topic_title': row_data.get('title', ''),
            'slug': row_data.get('slug', ''),
            'topic_url': topic_url,
        }
        inserted = self._insert_ticket_activity_rows(
            project_uuid, linked_ticket, event_type, old_all, new_all, base_detail
        )
        if not inserted:
            return
        logger.info('inserted %d activity rows for "%s" on ticket %s', inserted, event_type, linked_ticket)

    def _handle_email_thread_event(self, project_uuid, record_id, connection_id, event):
        threads_table = get_table_name_from_schema(SchemaTableNames.THREAD, connection_id)
        select_fields = '`linked_ticket`, `_pk`, `title`'
        context = self._load_linked_record_event_context(project_uuid, record_id, threads_table, select_fields, event)
        if not context:
            return

        record_id, row_data, linked_ticket, event_type, old_all, new_all = context
        base_detail = {
            'connection_id': connection_id,
            'record_id': record_id,
            'thread_id': row_data.get('_pk'),
            'thread_title': row_data.get('title', ''),
        }
        inserted = self._insert_ticket_activity_rows(
            project_uuid, linked_ticket, event_type, old_all, new_all, base_detail
        )
        if not inserted:
            return
        logger.info('inserted %d activity rows for "%s" on ticket %s', inserted, event_type, linked_ticket)

    def _build_activity_rows(self, linked_ticket, event_type, old_all, new_all, base_detail, now):
        def _make_row(old_value, new_value):
            return {
                'ticket_id': linked_ticket,
                'activity_type': event_type,
                'detail': json.dumps({
                    **base_detail,
                    'field_name': event_type,
                    'old_value': old_value,
                    'new_value': new_value,
                }),
                'creator': 'system',
                'created_time': now,
            }

        # only github has closed/reopened status changes, need special handling
        if event_type in ('github_issue_closed', 'github_issue_reopened'):
            # state/state_reason use original event_type; other fields use github_issue_updated.
            old_obj = old_all or {}
            new_obj = new_all or {}
            state_fields = {'state', 'state_reason'}
            rows = []
            # Other fields use github_issue_updated type
            other_fields = (set(old_obj) | set(new_obj)) - state_fields
            for field in sorted(other_fields):
                field_event_type = 'github_issue_updated'
                rows.append({
                    'ticket_id': linked_ticket,
                    'activity_type': field_event_type,
                    'detail': json.dumps({
                        **base_detail,
                        'field_name': field,
                        'old_value': {field: old_obj.get(field)},
                        'new_value': {field: new_obj.get(field)},
                    }),
                    'creator': 'system',
                    'created_time': now,
                })
            # State fields use original event_type (github_issue_closed/reopened)
            old_state = {k: v for k, v in old_obj.items() if k in state_fields}
            new_state = {k: v for k, v in new_obj.items() if k in state_fields}
            if old_state or new_state:
                rows.append(_make_row(old_state or None, new_state or None))
            return rows

        if event_type in ('github_issue_updated', 'discourse_topic_updated'):
            # one row per changed field
            old_obj = old_all or {}
            new_obj = new_all or {}
            rows = []
            for field in sorted(set(old_obj) | set(new_obj)):
                rows.append(_make_row({field: old_obj.get(field)}, {field: new_obj.get(field)}))
            return rows

        if event_type == 'email_message_added':
            return [{
                'ticket_id': linked_ticket,
                'activity_type': event_type,
                'detail': json.dumps({
                    **base_detail,
                    'field_name': event_type,
                }),
                'creator': 'system',
                'created_time': now,
            }]

        # other events
        return [_make_row(old_all, new_all)]

    def _extract_display_values(self, event_type, event):
        old_raw = event.get('old_value') or {}
        new_raw = event.get('new_value') or {}

        # github
        if event_type == 'github_issue_comment_added':
            return None, {
                'count': 1,
                'author': new_raw.get('author'),
            }

        # discourse
        if event_type == 'discourse_topic_comment_added':
            return None, {
                'count': 1,
                'author': new_raw.get('author'),
            }

        # email
        if event_type == 'email_message_added':
            return None, {'count': 1}

        if event_type.startswith('github_issue_'):
            display_fields = GITHUB_ISSUE_DISPLAY_FIELDS
        elif event_type.startswith('discourse_topic_'):
            display_fields = DISCOURSE_TOPIC_DISPLAY_FIELDS
        else:
            return None, None

        old_display = (
            {k: v for k, v in old_raw.items() if k in display_fields and v is not None}
            if old_raw else None
        )
        new_display = (
            {k: v for k, v in new_raw.items() if k in display_fields and v is not None}
            if new_raw else None
        )
        return old_display or None, new_display or None
