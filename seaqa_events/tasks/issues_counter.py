import logging
import datetime
import json
import time
from collections import defaultdict
from threading import Thread, Event, Lock
from copy import deepcopy

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
        self._last_count_time = None
        self._pending_project_uuids = set()
        self._pending_lock = Lock()
        self.finished = Event()

    def run(self):
        Thread(target=self._listen_issue_stat_events, args=('issue_statistics_update',), daemon=True).start()
        Thread(target=self._listen_issue_stat_events, args=('ticket_update',), daemon=True).start()

        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self._last_count_time:
                self._count_all_projects()
            else:
                self._count_event_projects()

            self._last_count_time = datetime.datetime.now(datetime.UTC)

    def cancel(self):
        self.finished.set()

    def _listen_issue_stat_events(self, channel):
        subscriber = self._redis_client.get_subscriber(channel)
        while not self.finished.is_set():
            try:
                message = subscriber.get_message()
                if message is None:
                    time.sleep(0.5)
                    continue

                payload = json.loads(message.get('data') or '{}')
                project_uuid = payload.get('project_uuid')
                if not project_uuid:
                    continue

                with self._pending_lock:
                    self._pending_project_uuids.add(uuid_str_to_32_chars(project_uuid))
            except Exception as e:
                logger.warning('Failed to handle %s event: %s', channel, e)
                subscriber = self._redis_client.get_subscriber(channel)

    def _get_tickets_count(self, project_uuid):
        sql = "SELECT COUNT(*) as count FROM `tickets`"
        try:
            response = self.seadb_api.query_rows(project_uuid, sql)
            if response and 'results' in response and response['results']:
                return response['results'][0].get('count', 0)
            return 0
        except Exception as e:
            logger.warning('Failed to count tickets for project %s: %s', project_uuid, e)
            return 0

    def _count_all_projects(self):
        try:
            rows = self.seaqa_db.list_projects_with_issue_connections()
        except Exception as e:
            logger.error('Failed to list projects with issue connections: %s', e)
            return

        self._count_projects(rows)

    def _count_event_projects(self):
        with self._pending_lock:
            project_uuids = deepcopy(self._pending_project_uuids)
            self._pending_project_uuids.clear()
        if not project_uuids:
            logger.info('No projects need issues statistics update')
            return
        try:
            project_rows = self.seaqa_db.list_projects_with_issue_connections_by_uuids(project_uuids)
        except Exception as e:
            logger.error('Failed to count issue statistics for updated projects: %s', e)
            return

        self._count_projects(project_rows)

    def _count_projects(self, rows):
        if not rows:
            logger.info('No projects need issues statistics update')
            return

        projects = defaultdict(lambda: {
            'org_id': -1,
            'connection_records_total': 0,
        })

        for row in rows:
            project_uuid = row.project_uuid
            proj = projects[project_uuid]
            proj['org_id'] = row.org_id if row.org_id is not None else -1

            conn_type = row.connection_type
            status = row.connection_status
            conn_total_records = 0
            if status:
                try:
                    status_data = json.loads(status)
                    conn_total_records = int(status_data.get('total_records', 0))
                except Exception:
                    conn_total_records = 0

            if conn_type in ISSUE_CONNECTION_TYPES:
                proj['connection_records_total'] += conn_total_records

        updated = 0
        updated_at = datetime.datetime.now(datetime.UTC)
        project_stats = []
        for project_uuid, proj in projects.items():
            try:
                tickets_count = self._get_tickets_count(project_uuid)
                total = tickets_count + proj['connection_records_total']
                project_stats.append({
                    'project_uuid': project_uuid,
                    'org_id': proj['org_id'],
                    'total_issues_count': total,
                    'updated_at': updated_at,
                })
                updated += 1
            except Exception as e:
                logger.error('Failed to count issues for project %s: %s', project_uuid, e)
            
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
        
        logger.info('Issues statistics updated for %d/%d projects', updated, len(projects))
