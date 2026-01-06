# -*- coding: utf-8 -*-
import os
import logging
from threading import Thread, Event

from seaqa_events.utils import get_opt_from_conf_or_env, parse_bool, get_python_executable, run, parse_interval
from seaqa_events.app.config import SEAQA_WEB_DIR

__all__ = [
    'EmailNoticesSender',
]


class EmailNoticesSender(object):
    def __init__(self, configs):
        self._enabled = True
        self._logfile = None
        self._interval = 60 * 60  # 60min
        self._prepare_logfile()
        self._parse_config(configs)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'email_notices_sender.log')

    def _parse_config(self, configs):
        """parse send email related options from config file
        """
        section_name = 'EMAIL_SENDER'
        key_enabled = 'enabled'
        key_interval = 'interval'
        email_sender_config = configs.get(section_name, {})
        if not email_sender_config:
            return

        # enabled
        enabled = get_opt_from_conf_or_env(email_sender_config, key_enabled, default='true')
        enabled = parse_bool(enabled)
        self._enabled = enabled
        # interval
        interval = get_opt_from_conf_or_env(email_sender_config, key_interval, default='60 * 60')
        interval = parse_interval(interval, 60 * 60)
        self._interval = interval

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start email notices sender: it is not enabled!')
            return

        logging.info('Start email notices sender, interval = %s sec', self._interval)

        SendSeaqaEmailTimer(self._interval, self._logfile).start()

    def is_enabled(self):
        return self._enabled


class SendSeaqaEmailTimer(Thread):

    def __init__(self, interval, logfile):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = logfile
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Starts to send email...')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(SEAQA_WEB_DIR, 'manage.py')

                    cmd = [
                        python_exec,
                        manage_py,
                        'send_email_notices',
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=SEAQA_WEB_DIR, output=fp)
                except Exception as e:
                    logging.exception('error when send email: %s', e)

    def cancel(self):
        self.finished.set()
