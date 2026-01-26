# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta
from threading import Thread, Event

from sqlalchemy import text

from seaqa_events.db import init_db_session_class

__all__ = [
    'NotificationCleaner',
]

logger = logging.getLogger(__name__)


class NotificationCleaner(object):
    def __init__(self, configs):
        self._db_session_class = init_db_session_class(configs)
        self._enabled = True
        self._interval = 60 * 60 * 24  # default 24h
        self._days = 30
        self._parse_config(configs)

    def _parse_config(self, configs):
        cleaner_config = configs.get('NOTIFICATION_CLEANER', {})
        if not cleaner_config:
            return
        self._enabled = cleaner_config.get('enabled', True)
        try:
            self._interval = int(cleaner_config.get('interval', self._interval))
        except Exception:
            self._interval = 60 * 60 * 24
        try:
            self._days = int(cleaner_config.get('days', self._days))
        except Exception:
            self._days = 30

    def start(self):
        if not self.is_enabled():
            logger.warning('Can not start notification cleaner: it is not enabled!')
            return
        logger.info('Start notification cleaner, interval=%s sec, days=%s', self._interval, self._days)
        CleanNotificationsTimer(self._interval, self._days, self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


class CleanNotificationsTimer(Thread):
    def __init__(self, interval, days, db_session_class):
        Thread.__init__(self)
        self._interval = interval
        self._days = days
        self._db_session_class = db_session_class
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                self._clean()

    def cancel(self):
        self.finished.set()

    def _clean(self):
        cutoff_date = datetime.utcnow() - timedelta(days=self._days)
        session = self._db_session_class()
        try:
            sql_user = "DELETE FROM `user_notifications` WHERE `timestamp` < :cutoff_date"
            sql_project = "DELETE FROM `project_notification` WHERE `timestamp` < :cutoff_date"
            user_result = session.execute(text(sql_user), {'cutoff_date': cutoff_date})
            project_result = session.execute(text(sql_project), {'cutoff_date': cutoff_date})
            session.commit()
            logger.info(
                'Clean notifications finished, days=%s, user_deleted=%s, project_deleted=%s',
                self._days,
                getattr(user_result, 'rowcount', None),
                getattr(project_result, 'rowcount', None),
            )
        except Exception as e:
            logger.exception('Failed to clean notifications: %s', e)
            session.rollback()
        finally:
            session.close()
