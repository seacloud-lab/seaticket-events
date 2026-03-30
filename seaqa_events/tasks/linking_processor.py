import datetime
import json
import logging
import time
from threading import Thread, Event

from seaqa_events.app.event_redis import RedisClient
from seaqa_events.utils.seadb_api import SeaDBAPI
from seaqa_events.utils.constant import ConnectionType
from seaqa_events.utils import uuid_str_to_32_chars

__all__ = ['LinkingProcessor']

logger = logging.getLogger(__name__)

DATA_EVENTS_CHANNEL = 'data_events'
TICKET_ACTIVITIES_TABLE = 'ticket_activities'

NO_MSG_TIMEOUT = 60 * 60 * 24
RECONNECT_SLEEP = 1

# Fields from a GitHub issue row worth surfacing in the activity log
GITHUB_ISSUE_DISPLAY_FIELDS = ['title', 'state', 'labels', 'assignees', 'issue_type', 'state_reason']



class LinkingProcessor:
    """Listens to the data_events channel and records activity logs on linked tickets.

    Currently handles:
    - GitHub issue changes → find the linked ticket → insert a ticket activity log.
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
        self.seadb_api = SeaDBAPI(username='seaqa-events')
        self.finished = Event()

    def run(self):
        self._listen_data_events()

    def cancel(self):
        self.finished.set()

    # ── Redis listener ──────────────────────────────────────────────

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
        print(data, '----data')
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
                # Find the ticket linked to this GitHub issue and insert an activity log.
                self._handle_github_issue_event(project_uuid, record_id, connection_id, event)
            except Exception as e:
                logger.error('failed to handle github_issue event (project=%s record=%s): %s', project_uuid, record_id, e)

    def _handle_github_issue_event(self, project_uuid, record_id, connection_id, event):
        try:
            record_id = int(record_id)
        except (ValueError, TypeError):
            logger.warning('invalid record_id %r for project=%s, skipping.',record_id, project_uuid)
            return

        issues_table = f'{ConnectionType.GITHUB_ISSUE.value}_{connection_id}'
        sql = (
            f"SELECT `linked_ticket`, `issue_number`, `title`, `url` "
            f"FROM `{issues_table}` WHERE `_pk` = {record_id} LIMIT 1"
        )
        results = self.seadb_api.query_rows(project_uuid, sql).get('results', [])
        if not results:
            logger.warning('record not found for record_id=%s in %s', record_id, issues_table)
            return

        row_data = results[0]
        linked_ticket = row_data.get('linked_ticket')
        if not linked_ticket:
            return
        try:
            linked_ticket = int(linked_ticket)
        except (ValueError, TypeError):
            logger.warning('invalid linked_ticket %r for record_id=%s, skipping.',linked_ticket, record_id)
            return

        event_type = event.get('type', '')
        old_all, new_all = self._extract_display_values(event_type, event)
        base_detail = {
            'connection_id': connection_id,
            'record_id': record_id,
            'issue_number': row_data.get('issue_number'),
            'issue_title': row_data.get('title', ''),
            'issue_url': row_data.get('url', ''),
        }
        now = datetime.datetime.now(datetime.UTC).isoformat()
        rows = self._build_activity_rows(
            linked_ticket, event_type, old_all, new_all, base_detail, now
        )
        if not rows:
            logger.debug(
                'LinkingProcessor: no activity rows to insert for "%s" ticket=%s.',
                event_type, linked_ticket,
            )
            return
        self.seadb_api.insert_rows(project_uuid, TICKET_ACTIVITIES_TABLE, rows)
        logger.info(
            'LinkingProcessor: inserted %d activity row(s) for "%s" on ticket %s '
            '(project=%s, record_id=%s, issue_number=%s)',
            len(rows), event_type, linked_ticket, project_uuid,
            record_id, row_data.get('issue_number'),
        )

    def _build_activity_rows(self, linked_ticket, event_type, old_all, new_all, base_detail, now):
        """Return a list of activity row dicts ready for insert_rows.

        For github_issue_updated: one row per changed field (mirrors record_ticket_activities).
        For all other event types: a single row.
        """
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

        if event_type != 'github_issue_updated':
            return [_make_row(old_all, new_all)]

        # github_issue_updated: one row per changed field
        old_obj = old_all or {}
        new_obj = new_all or {}
        all_fields = sorted(set(old_obj) | set(new_obj))
        rows = []
        for field in all_fields:
            old_val = old_obj.get(field)
            new_val = new_obj.get(field)
            rows.append(_make_row({field: old_val}, {field: new_val}))
        return rows

    def _extract_display_values(self, event_type, event):
        old_raw = event.get('old_value') or {}
        new_raw = event.get('new_value') or {}

        if event_type in ('github_issue_closed', 'github_issue_reopened'):
            closed_fields = {'state', 'state_reason'}
            old_display = {k: v for k, v in old_raw.items() if k in closed_fields and v is not None}
            new_display = {k: v for k, v in new_raw.items() if k in closed_fields and v is not None}
            return old_display or None, new_display or None

        if event_type == 'github_issue_comment_added':
            comments = new_raw.get('comments') or []
            return None, len(comments)

        # github_issue_updated / github_issue_added
        old_display = (
            {k: v for k, v in old_raw.items() if k in GITHUB_ISSUE_DISPLAY_FIELDS and v is not None}
            if old_raw else None
        )
        new_display = (
            {k: v for k, v in new_raw.items() if k in GITHUB_ISSUE_DISPLAY_FIELDS and v is not None}
            if new_raw else None
        )
        return old_display or None, new_display or None
