import logging
import datetime
import json
import time
from collections import defaultdict
from copy import deepcopy
from threading import Thread, Event, Lock

from seaqa_events.app.event_redis import RedisClient
from seaqa_events.db import init_db_session_class
from seaqa_events.utils import uuid_str_to_32_chars
from seaqa_events.utils.seaqa_db import SeaqaDB
from seaqa_events.utils.seadb_api import SeaDBAPI

from seaqa_events.utils.constant import ISSUE_CONNECTION_TYPES

__all__ = [
    'IssuesCounter',
]

logger = logging.getLogger(__name__)


ISSUE_COUNTER_CHANNEL = 'connection_update'
# 1 day in seconds, if no message received for this duration, trigger redis reconnection logic
NO_MSG_TIMEOUT = 60 * 60 * 24 
# Redis reconnection sleep time: 1 second to avoid frequent reconnection attempts
RECONNECT_SLEEP = 1


class NoMessageException(Exception):
    pass


class IssuesCounter(object):
    def __init__(self, configs):
        self._configs = configs
        self._db_session_class = init_db_session_class(configs)
        self._enabled = True
        self._interval = 10 * 60
        self._parse_config(configs)

    def _parse_config(self, configs):
        counter_config = configs.get('ISSUES_COUNTER', {})
        if not counter_config:
            return
        self._enabled = counter_config.get('enabled', True)
        try:
            self._interval = int(counter_config.get('interval', self._interval))
        except Exception:
            self._interval = 10 * 60

    def start(self):
        if not self.is_enabled():
            logger.warning('Can not start issues counter: it is not enabled!')
            return
        logger.info('Start issues counter, interval=%s sec', self._interval)
        CountIssuesTimer(self._interval, self._db_session_class, self._configs).start()

    def is_enabled(self):
        return self._enabled


class CountIssuesTimer(Thread):
    def __init__(self, interval, db_session_class, configs):
        Thread.__init__(self)
        self._interval = interval
        self._db_session_class = db_session_class
        self._redis_client = RedisClient(configs, socket_timeout=10)
        self.seaqa_db = SeaqaDB(self._db_session_class)
        self.seadb_api = SeaDBAPI(username='seaqa-events')
        self._upsert_batch_size = 50
        self._pending_projects = defaultdict(lambda: {
            'connection_count': 0,
            'ticket_count': 0,
        })
        self._pending_lock = Lock()
        self.finished = Event()

    def run(self):
        Thread(target=self._listen_issue_stat_events, daemon=True).start()
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            self._count_event_projects()

    def _listen_issue_stat_events(self):
        subscriber = None
        message_check_time = 0
        while not self.finished.is_set():
            if subscriber is None:
                try:
                    subscriber = self._redis_client.get_subscriber(ISSUE_COUNTER_CHANNEL)
                    message_check_time = time.time()
                except Exception as e:
                    logger.warning('Failed to subscribe %s event channel: %s', ISSUE_COUNTER_CHANNEL, e)
                    time.sleep(RECONNECT_SLEEP)
                    continue

            try:
                message = subscriber.get_message()
                if not message:
                    no_msg_duration = time.time() - message_check_time
                    if no_msg_duration > NO_MSG_TIMEOUT:
                        logger.warning('No message received for %s seconds, triggering redis reconnection', NO_MSG_TIMEOUT)
                        raise NoMessageException('No message received for %s seconds' % NO_MSG_TIMEOUT)
                    time.sleep(RECONNECT_SLEEP)
                    continue

                message_check_time = time.time()
                payload = json.loads(message.get('data') or '{}')
                self._enqueue_project_delta(payload)

            except NoMessageException as e:
                logger.warning('Failed to handle %s event: %s', ISSUE_COUNTER_CHANNEL, e)
                try:
                    subscriber.close()
                except Exception:
                    pass
                subscriber = None
            except Exception as e:
                logger.warning('Failed to handle %s event: %s', ISSUE_COUNTER_CHANNEL, e)
                try:
                    subscriber.close()
                except Exception:
                    pass
                subscriber = None
                time.sleep(RECONNECT_SLEEP)


    def cancel(self):
        self.finished.set()

    def _enqueue_project_delta(self, payload):
        project_uuid = payload.get('project_uuid')
        issue_type = payload.get('type')

        if not project_uuid:
            return
        if issue_type not in ISSUE_CONNECTION_TYPES and issue_type != 'ticket':
            return

        try:
            add_count = int(payload.get('add', 0))
            delete_count = int(payload.get('delete', 0))
        except Exception:
            return

        if add_count == 0 and delete_count == 0:
            return

        with self._pending_lock:
            project_pending = self._pending_projects[uuid_str_to_32_chars(project_uuid)]
            delta = add_count - delete_count
            if issue_type == 'ticket':
                project_pending['ticket_count'] += delta
            else:
                project_pending['connection_count'] += delta

    def _get_tickets_count(self, project_uuid):
        sql = "SELECT COUNT(*) as count FROM `tickets` WHERE (`deleted` = False OR `deleted` IS NULL)"
        try:
            response = self.seadb_api.query_rows(project_uuid, sql)
            if response and 'results' in response and response['results']:
                return response['results'][0].get('count', 0)
            return 0
        except Exception as e:
            logger.warning('Failed to count tickets for project %s: %s', project_uuid, e)
            return 0

    def _get_email_threads_count(self, project_uuid, connection_id):
        table_name = f"email_thread_{connection_id}"
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        try:
            response = self.seadb_api.query_rows(project_uuid, sql)
            if response and 'results' in response and response['results']:
                return int(response['results'][0].get('count', 0))
            return 0
        except Exception as e:
            logger.warning(
                'Failed to count email threads for project %s, connection %s: %s',
                project_uuid, connection_id, e
            )
            return 0

    def _count_event_projects(self):
        with self._pending_lock:
            pending_projects = deepcopy(self._pending_projects)
            self._pending_projects.clear()

        if not pending_projects:
            logger.info('No projects need issues statistics update')
            return

        project_uuids = list(pending_projects.keys())
        try:
            existed_stats_rows = self.seaqa_db.get_project_issues_statistics_by_uuids(project_uuids)
        except Exception as e:
            logger.error('Failed to count issue statistics for updated projects: %s', e)
            return

        existed_stats_map = {
            uuid_str_to_32_chars(row.project_uuid): row for row in existed_stats_rows
        }

        projects_need_connection_recount = []
        for project_uuid, pending in pending_projects.items():
            row = existed_stats_map.get(project_uuid)
            if int(pending.get('connection_count', 0)) != 0 or row is None:
                projects_need_connection_recount.append(project_uuid)

        connection_stats_map = self._build_project_connection_stats(projects_need_connection_recount)

        self._upsert_project_issue_stats(pending_projects, existed_stats_map, connection_stats_map)

    def _build_project_connection_stats(self, project_uuids):
        if not project_uuids:
            return {}
        try:
            rows = self.seaqa_db.list_projects_with_issue_connections_by_uuids(project_uuids)
        except Exception as e:
            logger.error('Failed to initialize issue statistics for projects: %s', e)
            return {}

        projects = defaultdict(lambda: {
            'org_id': -1,
            'connection_issues_count': 0,
        })

        for row in rows:
            proj = projects[uuid_str_to_32_chars(row.project_uuid)]
            proj['org_id'] = row.org_id if row.org_id is not None else -1

            conn_type = row.connection_type
            conn_id = row.connection_id
            status = row.connection_status
            conn_total_records = 0
            if status:
                try:
                    status_data = json.loads(status)
                    conn_total_records = int(status_data.get('total_records', 0))
                except Exception:
                    conn_total_records = 0

            if conn_type == 'email' and conn_id:
                proj['connection_issues_count'] += self._get_email_threads_count(row.project_uuid, conn_id)
            elif conn_type in ISSUE_CONNECTION_TYPES:
                proj['connection_issues_count'] += conn_total_records

        connection_stats = {}
        for project_uuid, proj in projects.items():
            connection_stats[project_uuid] = {
                'org_id': proj['org_id'],
                'connection_issues_count': int(proj['connection_issues_count']),
            }

        return connection_stats

    def _upsert_project_issue_stats(self, pending_projects, existed_stats_map, connection_stats_map):
        updated_at = datetime.datetime.now(datetime.UTC)
        project_stats = []

        for project_uuid, pending in pending_projects.items():
            row = existed_stats_map.get(project_uuid)
            need_connection_recount = int(pending.get('connection_count', 0)) != 0 or row is None
            need_ticket_recount = int(pending.get('ticket_count', 0)) != 0 or row is None

            if not need_connection_recount and not need_ticket_recount:
                continue
                
            connection_stats = connection_stats_map.get(project_uuid, {})
            org_id = -1

            if row:
                org_id = row.org_id if row.org_id is not None else -1
                connection_count = int(getattr(row, 'connection_issues_count', 0))
                ticket_count = int(getattr(row, 'ticket_issues_count', 0))
            else:
                connection_count = 0
                ticket_count = 0

            if need_connection_recount:
                org_id = connection_stats.get('org_id', -1)
                connection_count = int(connection_stats.get('connection_issues_count', 0))

            if need_ticket_recount:
                ticket_count = int(self._get_tickets_count(project_uuid))

            total_count = connection_count + ticket_count

            project_stats.append({
                'project_uuid': project_uuid,
                'org_id': org_id,
                'connection_issues_count': connection_count,
                'ticket_issues_count': ticket_count,
                'total_issues_count': total_count,
                'updated_at': updated_at,
            })

            if len(project_stats) >= self._upsert_batch_size:
                try:
                    self.seaqa_db.upsert_project_issues_statistics_batch(project_stats)
                    project_stats = []
                except Exception as e:
                    logger.error('Failed to batch upsert issues statistics: %s', e)
                    return

        if project_stats:
            try:
                self.seaqa_db.upsert_project_issues_statistics_batch(project_stats)
            except Exception as e:
                logger.error('Failed to batch upsert issues statistics: %s', e)
                return

        logger.info('Issues statistics updated for %d projects', len(pending_projects))
