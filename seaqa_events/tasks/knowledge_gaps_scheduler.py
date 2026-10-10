# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timezone
from threading import Thread, Event

from sqlalchemy import text

from seaqa_events.db import init_db_session_class
from seaqa_events.utils.ai_api import analyze_knowledge_gaps

__all__ = [
    'KnowledgeGapsScheduler',
]

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL = 24 * 60 * 60
QUEUE_FULL_RETRY_DELAY = 60


class KnowledgeGapsScheduler(object):
    def __init__(self, configs):
        self._db_session_class = init_db_session_class(configs)
        self._enabled = True
        self._interval = DEFAULT_INTERVAL
        self._parse_config(configs)

    def _parse_config(self, configs):
        scheduler_config = configs.get('KNOWLEDGE_GAPS', {})
        if not scheduler_config:
            return
        self._enabled = scheduler_config.get('enabled', True)
        try:
            self._interval = int(scheduler_config.get('interval', self._interval))
        except Exception:
            self._interval = DEFAULT_INTERVAL

    def start(self):
        if not self.is_enabled():
            logger.warning('Can not start knowledge gaps scheduler: it is not enabled!')
            return
        logger.info('Start knowledge gaps scheduler, interval=%s sec', self._interval)
        KnowledgeGapsTimer(self._interval, self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


class KnowledgeGapsTimer(Thread):
    def __init__(self, interval, db_session_class):
        Thread.__init__(self)
        self._interval = interval
        self._db_session_class = db_session_class
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    self._run_once()
                except Exception:
                    logger.exception('Knowledge gaps scheduler run failed')

    def cancel(self):
        self.finished.set()

    def _run_once(self):
        project_uuids = self._list_project_uuids()
        logger.info('Knowledge gaps scheduler found %s projects', len(project_uuids))
        for project_uuid in project_uuids:
            if not self._submit_project(project_uuid):
                break

    def _submit_project(self, project_uuid):
        while not self.finished.is_set():
            try:
                result = analyze_knowledge_gaps(project_uuid)
                logger.info('Knowledge gaps analysis submitted for %s: %s', project_uuid, result)
                return True
            except ConnectionError as e:
                status = e.args[0] if e.args else None
                if status != 429:
                    logger.exception('Failed to submit knowledge gaps analysis for %s: %s', project_uuid, e)
                    return True
                logger.warning(
                    'Knowledge gaps queue is full, retry project %s in %s sec',
                    project_uuid,
                    QUEUE_FULL_RETRY_DELAY,
                )
                if self.finished.wait(QUEUE_FULL_RETRY_DELAY):
                    return False
            except Exception as e:
                logger.exception('Failed to submit knowledge gaps analysis for %s: %s', project_uuid, e)
                return True
        return False

    def _list_project_uuids(self):
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        sql = (
            'SELECT DISTINCT s.project_uuid FROM portal_chat_messages m '
            'INNER JOIN portal_chat_sessions s ON s.session_uuid = m.session_uuid '
            "WHERE m.role = 'user' AND m.evaluated_at IS NULL AND m.created_at < :cutoff"
        )
        with self._db_session_class() as session:
            rows = session.execute(text(sql), {'cutoff': cutoff}).fetchall()
        return [str(row.project_uuid) for row in rows if row.project_uuid]
