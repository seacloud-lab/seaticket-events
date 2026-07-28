# -*- coding: utf-8 -*-
import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone

from sqlalchemy import bindparam, text

from seaqa_events.app.event_redis import RedisClient
from seaqa_events.db import init_db_session_class
from seaqa_events.utils import uuid_str_to_32_chars
from seaqa_events.utils.indexer_api import get_project_storage_usage

logger = logging.getLogger(__name__)

MAX_UINT64 = 2 ** 64 - 1
PROJECT_STORAGE_UPDATE_CHANNEL = 'project_storage_update'
PROJECT_STORAGE_BATCH_SIZE = 50
REDIS_RECONNECT_INTERVAL = 1
NO_MSG_TIMEOUT = 24 * 60 * 60


class NoMessageException(Exception):
    pass


def _validate_size(value, field_name):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError('%s must be a non-negative integer' % field_name)
    if value < 0 or value > MAX_UINT64:
        raise ValueError('%s is out of range' % field_name)
    return value


class ProjectStorageUsageCollector:
    def __init__(self, configs):
        self._configs = configs
        self._db_session_class = init_db_session_class(configs)
        self._enabled = True
        self._interval = 24 * 60 * 60
        self._parse_config(configs)

    def _parse_config(self, configs):
        collector_config = configs.get('PROJECT_STORAGE_USAGE_COLLECTOR', {})
        self._enabled = collector_config.get('enabled', True)
        self._interval = collector_config.get('interval', self._interval)
        if isinstance(self._interval, bool) or not isinstance(self._interval, int) or self._interval <= 0:
            raise ValueError('PROJECT_STORAGE_USAGE_COLLECTOR.interval must be a positive integer')

    def start(self):
        if not self._enabled:
            logger.warning('Project storage usage collector is disabled')
            return
        logger.info('Start project storage usage collector, interval=%s sec', self._interval)
        StorageUsageTimer(self._interval, self._db_session_class, self._configs).start()


class StorageUsageTimer(threading.Thread):
    def __init__(self, interval, db_session_class, configs):
        super().__init__(daemon=True)
        self._interval = interval
        self._db_session_class = db_session_class
        self._redis_client = RedisClient(configs, socket_timeout=10)
        self._pending_projects = {}
        self._pending_lock = threading.Lock()
        self.finished = threading.Event()

    def run(self):
        listener = threading.Thread(target=self._listen_project_updates, daemon=True)
        listener.start()
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    self._update_pending_projects()
                except Exception:
                    logger.exception('Project storage usage collection failed')

    def _update_pending_projects(self):
        started_at = time.monotonic()
        with self._pending_lock:
            project_uuids = list(self._pending_projects)
            self._pending_projects = {}
        if not project_uuids:
            logger.info('No projects need storage usage update')
            return

        retry_projects = []
        storage_statistics = []
        projects_to_collect = deque(project_uuids)
        try:
            while projects_to_collect:
                project_uuid = projects_to_collect[0]
                result = self._collect_project(project_uuid)
                if result is None:
                    retry_projects.append(project_uuid)
                else:
                    storage_statistics.append({'project_uuid': project_uuid, **result})
                projects_to_collect.popleft()

            while storage_statistics:
                batch = storage_statistics[:PROJECT_STORAGE_BATCH_SIZE]
                batch_project_uuids = [stat['project_uuid'] for stat in batch]
                try:
                    with self._db_session_class() as session:
                        active_projects = session.execute(
                            text(
                                'SELECT p.uuid AS project_uuid, w.org_id '
                                'FROM projects p '
                                'JOIN workspaces w ON w.id=p.workspace_id '
                                'WHERE p.uuid IN :project_uuids'
                            ).bindparams(bindparam('project_uuids', expanding=True)),
                            {'project_uuids': batch_project_uuids},
                        ).all()
                        project_org_map = {
                            row.project_uuid: row.org_id
                            for row in active_projects
                        }
                        calculated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                        rows = [
                            {
                                **stat,
                                'org_id': project_org_map[stat['project_uuid']],
                                'calculated_at': calculated_at,
                            }
                            for stat in batch
                            if stat['project_uuid'] in project_org_map
                        ]
                        if rows:
                            session.execute(text(
                                """INSERT INTO project_storage_statistics
                                   (project_uuid, org_id, file_size, crawl_data_size,
                                    total_size, calculated_at)
                                   VALUES (:project_uuid, :org_id, :file_size,
                                           :crawl_data_size, :total_size, :calculated_at)
                                   ON DUPLICATE KEY UPDATE
                                    org_id=VALUES(org_id),
                                    file_size=VALUES(file_size),
                                    crawl_data_size=VALUES(crawl_data_size),
                                    total_size=VALUES(total_size),
                                    calculated_at=VALUES(calculated_at)"""
                            ), rows)
                        session.commit()
                except Exception:
                    logger.exception('Failed to persist storage usage for projects %s', batch_project_uuids)
                    retry_projects.extend(batch_project_uuids)
                del storage_statistics[:len(batch)]
        finally:
            pending_projects = retry_projects + [stat['project_uuid'] for stat in storage_statistics] + list(projects_to_collect)
            if pending_projects:
                with self._pending_lock:
                    for project_uuid in pending_projects:
                        self._pending_projects.setdefault(project_uuid, None)

        logger.info(
            'Project storage usage collection finished: attempted=%s failed=%s elapsed=%.3fs',
            len(project_uuids), len(retry_projects), time.monotonic() - started_at,
        )

    def _collect_project(self, project_uuid):
        try:
            response = get_project_storage_usage(project_uuid)
            if not response.get('success'):
                raise ValueError('Invalid Indexer storage usage response')
        except Exception:
            logger.exception('Failed to collect Indexer storage usage for project %s', project_uuid)
            return None

        try:
            file_size = _validate_size(response['usage']['file_size'], 'file_size')
            crawl_size = _validate_size(response['usage']['crawl_data_size'], 'crawl_data_size')
            total_size = file_size + crawl_size
            if total_size > MAX_UINT64:
                raise ValueError('total_size is out of range')
            return {
                'file_size': file_size,
                'crawl_data_size': crawl_size,
                'total_size': total_size,
            }
        except Exception:
            logger.exception('Failed to collect storage usage for project %s', project_uuid)
            return None

    def _listen_project_updates(self):
        subscriber = None
        last_message_at = 0
        while not self.finished.is_set():
            try:
                if subscriber is None:
                    subscriber = self._redis_client.get_subscriber(PROJECT_STORAGE_UPDATE_CHANNEL)
                    last_message_at = time.monotonic()
                message = subscriber.get_message()
                if not message:
                    if time.monotonic() - last_message_at > NO_MSG_TIMEOUT:
                        raise NoMessageException('No message received for %s seconds' % NO_MSG_TIMEOUT)
                    self.finished.wait(REDIS_RECONNECT_INTERVAL)
                    continue
                last_message_at = time.monotonic()
                try:
                    payload = json.loads(message.get('data'))
                    project_uuid = payload.get('project_uuid')
                except Exception as e:
                    logger.warning('Ignore invalid project storage update message: %s', e)
                    continue
                if not project_uuid:
                    continue
                try:
                    normalized_uuid = uuid_str_to_32_chars(project_uuid)
                    with self._pending_lock:
                        self._pending_projects.setdefault(normalized_uuid, None)
                except Exception as e:
                    logger.warning('Ignore project storage update with invalid project_uuid: %s', e)
            except Exception as e:
                logger.warning('Project storage update subscriber failed: %s', e)
                if subscriber is not None:
                    try:
                        subscriber.close()
                    except Exception:
                        pass
                    subscriber = None
                self.finished.wait(REDIS_RECONNECT_INTERVAL)

    def cancel(self):
        self.finished.set()
