# -*- coding: utf-8 -*-
import os
import logging
import json
import time
import jwt
import requests
from threading import Thread, Event

from seaqa_events.utils import get_opt_from_conf_or_env, parse_bool, parse_interval
from seaqa_events.app.config import JWT_PRIVATE_KEY, SEAQA_WEB_INNER_SERVER_URL

__all__ = [
    'EmailNoticesSender',
]


class EmailNoticesSender(object):
    def __init__(self, configs):
        self._enabled = True
        self._logfile = None
        self._interval = 60 * 60  # 60min
        self._timeout = 30
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

        SendSeaqaEmailTimer(self._interval, self._logfile, self._timeout).start()

    def is_enabled(self):
        return self._enabled


class SendSeaqaEmailTimer(Thread):

    def __init__(self, interval, logfile, timeout):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = logfile
        self._timeout = timeout
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                if not SEAQA_WEB_INNER_SERVER_URL:
                    logging.warning('SEAQA_WEB_INNER_SERVER_URL is not set, skip sending email')
                    continue
                logging.info('Starts to send email...')
                try:
                    url = f'{SEAQA_WEB_INNER_SERVER_URL}/api2/internal/send-email-notices/'
                    payload = {'exp': int(time.time()) + 300}
                    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
                    if isinstance(token, bytes):
                        token = token.decode('utf-8')
                    headers = {"Authorization": "Token %s" % token}
                    with open(self._logfile, 'a') as fp:
                        resp = requests.post(url, headers=headers, timeout=self._timeout)
                        fp.write(f'[{resp.status_code}] {resp.text}\n')
                        try:
                            data = json.loads(resp.text) if resp.text else {}
                        except Exception as e:
                            data = {}
                    if resp.status_code >= 400:
                        logging.error('Failed to send email: %s', resp.text)
                    if isinstance(data, dict) and data.get('error'):
                        logging.error('Failed to send email: %s', data.get('error'))
                except Exception as e:
                    logging.exception('error when send email: %s', e)

    def cancel(self):
        self.finished.set()
