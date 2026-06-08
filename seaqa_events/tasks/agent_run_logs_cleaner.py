# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta, UTC
from threading import Thread, Event

from sqlalchemy import text

from seaqa_events.db import init_db_session_class
from seaqa_events.utils.seadb_api import SeaDBAPI
from collections.abc import Iterator

__all__ = [
    'AgentRunLogsCleaner',
]

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL = 24 * 60 * 60
DEFAULT_DAYS = 30
DEFAULT_BATCH_SIZE = 100
DEFAULT_PROJECT_PAGE_SIZE = 50


class AgentRunLogsCleaner(object):
    def __init__(self, configs):
        self._db_session_class = init_db_session_class(configs)
        self._enabled = True
        self._interval = DEFAULT_INTERVAL
        self._days = DEFAULT_DAYS
        self._batch_size = DEFAULT_BATCH_SIZE
        self._parse_config(configs)

    def _parse_config(self, configs):
        cleaner_config = configs.get('AGENT_RUN_LOGS_CLEANER', {})
        if not cleaner_config:
            return

        self._enabled = cleaner_config.get('enabled', True)
        try:
            self._interval = int(cleaner_config.get('interval', self._interval))
        except Exception:
            self._interval = DEFAULT_INTERVAL
        try:
            self._days = int(cleaner_config.get('days', self._days))
        except Exception:
            self._days = DEFAULT_DAYS

    def start(self):
        if not self.is_enabled():
            logger.warning('Can not start agent run logs cleaner: it is not enabled!')
            return
        logger.info(
            'Start agent run logs cleaner, interval=%s sec, days=%s, batch_size=%s',
            self._interval,
            self._days,
            self._batch_size,
        )
        CleanAgentRunLogsTimer(
            self._interval,
            self._days,
            self._batch_size,
            self._db_session_class,
        ).start()

    def is_enabled(self):
        return self._enabled


class CleanAgentRunLogsTimer(Thread):
    def __init__(self, interval, days, batch_size, db_session_class):
        Thread.__init__(self)
        self._interval = interval
        self._days = days
        self._batch_size = batch_size
        self._project_page_size = DEFAULT_PROJECT_PAGE_SIZE
        self._db_session_class = db_session_class
        self._seadb_api = SeaDBAPI()
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                self._clean()

    def cancel(self):
        self.finished.set()

    def _clean(self):
        cutoff_time = (datetime.now(UTC) - timedelta(days=self._days)).isoformat()
        total_projects = 0
        total_deleted_runs = 0

        for project_uuid in self._iter_project_uuids():
            total_projects += 1
            try:
                project_deleted_runs = self._clean_project_logs(project_uuid, cutoff_time)
                total_deleted_runs += project_deleted_runs
            except Exception as e:
                logger.exception('Failed to clean agent run logs for project %s: %s', project_uuid, e)

        logger.info(
            'Clean agent run logs finished, retention days=%s, projects=%s, total_deleted_runs=%s',
            self._days,
            total_projects,
            total_deleted_runs,
        )

    def _iter_project_uuids(self) -> Iterator[str]:
        page = 1
        while True:
            offset = (page - 1) * self._project_page_size
            with self._db_session_class() as session:
                rows = session.execute(
                    text('SELECT uuid FROM projects ORDER BY id LIMIT :limit OFFSET :offset'),
                    {
                        'limit': self._project_page_size,
                        'offset': offset,
                    },
                ).fetchall()

            if not rows:
                break

            for row in rows:
                yield str(row.uuid)

            if len(rows) < self._project_page_size:
                break
            page += 1

    def _clean_project_logs(self, project_uuid, cutoff_time):
        deleted_runs = 0
        while True:
            run_ids = self._list_expired_run_ids(project_uuid, cutoff_time)
            if not run_ids:
                break

            self._delete_actions_by_run_ids(project_uuid, run_ids)
            deleted_runs += self._delete_runs_by_ids(project_uuid, run_ids)
            if len(run_ids) < self._batch_size:
                break

        return deleted_runs

    def _list_expired_run_ids(self, project_uuid, cutoff_time):
        sql = (
            "SELECT `_pk` FROM `agent_runs` "
            f"WHERE `started_at` < '{cutoff_time}' "
            f"LIMIT {self._batch_size}"
        )
        try:
            result = self._seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            return [int(row.get('_pk')) for row in rows if row.get('_pk') is not None]
        except Exception as e:
            logger.exception('Failed to list expired agent run ids for project %s: %s', project_uuid, e)
            return []

    def _delete_actions_by_run_ids(self, project_uuid, run_ids):
        run_ids_str = ','.join(str(run_id) for run_id in run_ids)
        delete_sql = f"DELETE FROM `agent_actions` WHERE `run_id` IN ({run_ids_str})"
        try:
            self._seadb_api.query_rows(project_uuid, delete_sql)
        except Exception as e:
            logger.exception('Failed to delete agent actions for project %s: %s', project_uuid, e)

    def _delete_runs_by_ids(self, project_uuid, run_ids):
        try:
            self._seadb_api.delete_rows(project_uuid, 'agent_runs', run_ids)
            return len(run_ids)
        except Exception as e:
            logger.exception('Failed to delete agent runs for project %s: %s', project_uuid, e)
            return 0
