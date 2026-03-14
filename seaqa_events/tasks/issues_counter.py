# -*- coding: utf-8 -*-
"""
Periodic task: count issues per project and update project_issues_statistics.

Issue types counted (per project):
  - Built-in tickets: `tickets` table
  - GitHub issues: `github_issue_{connection_id}` tables
  - Forum threads: `discourse_forum_{connection_id}` tables
  - Email threads: `email_thread_{connection_id}` tables
"""
import logging
from collections import defaultdict
from threading import Thread, Event

from seaqa_events.db import init_db_session_class
from seaqa_events.utils import get_connection_table_name
from seaqa_events.utils.seadb_api import SeaDBAPI
from seaqa_events.utils.seaqa_db import SeaqaDB

__all__ = [
    'IssuesCounter',
]

logger = logging.getLogger(__name__)


class IssuesCounter(object):
    def __init__(self, configs):
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
        CountIssuesTimer(self._interval, self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


class CountIssuesTimer(Thread):
    def __init__(self, interval, db_session_class):
        Thread.__init__(self)
        self._interval = interval
        self._db_session_class = db_session_class
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self._count_all_projects()
            self.finished.wait(self._interval)

    def cancel(self):
        self.finished.set()

    def _get_table_count(self, seadb_api, project_uuid, table_name):
        """Count rows in a SeaDB table. Follows the same pattern as seaqa-indexer
        (e.g. github_seadb_api.get_github_issues_count, discourse_seadb_api.get_topic_count)."""
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        try:
            response = seadb_api.query_rows(project_uuid, sql)
            if response and 'results' in response and response['results']:
                return response['results'][0].get('count', 0)
            return 0
        except Exception as e:
            logger.warning('Failed to count %s for project %s: %s', table_name, project_uuid, e)
            return 0

    def _count_all_projects(self):
        seaqa_db = SeaqaDB(self._db_session_class)
        seadb_api = SeaDBAPI(username='seaqa-events')

        try:
            rows = seaqa_db.list_active_projects_with_issue_connections()
        except Exception as e:
            logger.error('Failed to list active projects: %s', e)
            return

        projects = defaultdict(lambda: {
            'org_id': -1,
            'github_issue_connections': [],
            'discourse_forum_connections': [],
            'email_connections': [],
        })

        for row in rows:
            project_uuid = row.project_uuid
            proj = projects[project_uuid]
            proj['org_id'] = row.org_id if row.org_id is not None else -1

            conn_type = row.connection_type
            conn_id = row.connection_id
            if conn_type and conn_id:
                if conn_type == 'github_issue':
                    proj['github_issue_connections'].append(conn_id)
                elif conn_type == 'discourse_forum':
                    proj['discourse_forum_connections'].append(conn_id)
                elif conn_type == 'email':
                    proj['email_connections'].append(conn_id)

        updated = 0
        for project_uuid, proj in projects.items():
            try:
                # tickets: project-wide table, same as seaqa-indexer TicketSeaDBAPI pattern
                tickets_count = self._get_table_count(seadb_api, project_uuid, 'tickets')

                # github issues: same as seaqa-indexer github_seadb_api.get_github_issues_count()
                github_issues_count = 0
                for conn_id in proj['github_issue_connections']:
                    table_name = 'github_issue_' + str(conn_id)
                    github_issues_count += self._get_table_count(seadb_api, project_uuid, table_name)

                # discourse forum topics: same as seaqa-indexer discourse_seadb_api.get_topic_count()
                forum_threads_count = 0
                for conn_id in proj['discourse_forum_connections']:
                    table_name = 'discourse_forum_' + str(conn_id)
                    forum_threads_count += self._get_table_count(seadb_api, project_uuid, table_name)

                # email threads: count from thread table (user requirement: 按照 threads 来统计)
                email_threads_count = 0
                for conn_id in proj['email_connections']:
                    table_name = 'email_thread_' + str(conn_id)
                    email_threads_count += self._get_table_count(seadb_api, project_uuid, table_name)

                seaqa_db.upsert_project_issues_statistics(
                    project_uuid=project_uuid,
                    org_id=proj['org_id'],
                    tickets_count=tickets_count,
                    github_issues_count=github_issues_count,
                    forum_threads_count=forum_threads_count,
                    email_threads_count=email_threads_count,
                )
                updated += 1
            except Exception as e:
                logger.error('Failed to count issues for project %s: %s', project_uuid, e)

        logger.info('Issues statistics updated for %d/%d projects', updated, len(projects))
