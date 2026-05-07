# -*- coding: utf-8 -*-
import json
import logging
import time
from datetime import datetime
from threading import Thread

from seaqa_events.additional_credits import AdditionalCreditsService
from seaqa_events.app.event_redis import RedisClient
from seaqa_events.db import init_db_session_class

logger = logging.getLogger(__name__)


class AdditionalCreditsWorker:
    def __init__(self, configs):
        self._db_session_class = init_db_session_class(configs)
        self._redis_client = RedisClient(configs, socket_timeout=10)
        self._additional_credits_service = AdditionalCreditsService(self._db_session_class, configs)
        self.channel = configs.get('ADDITIONAL_CREDITS_REDIS_CHANNEL', 'billing_ai_credits')
        self.log_none_message_timeout = 60 * 10
        self._enabled = True

    def receive(self):
        logger.info('Starts to receive additional credits messages...')
        subscriber = self._redis_client.get_subscriber(self.channel)
        last_message_time = datetime.now()
        while True:
            try:
                message = subscriber.get_message()
                if message is not None:
                    last_message_time = datetime.now()
                    try:
                        data = json.loads(message['data'])
                    except Exception:
                        logger.warning('additional credits redis message invalid, channel=%s', self.channel)
                        continue

                    try:
                        self._handle_message(data)
                    except Exception as e:
                        logger.exception('handle additional credits message %s error %s', data, e)
                else:
                    if (datetime.now() - last_message_time).seconds >= self.log_none_message_timeout:
                        logger.warning(
                            'No redis message received for channel %s in %s seconds',
                            self.channel,
                            self.log_none_message_timeout,
                        )
                        last_message_time = datetime.now()
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis for channel %s: %s', self.channel, e)
                subscriber = self._redis_client.get_subscriber(self.channel)
                last_message_time = datetime.now()

    def _handle_message(self, data):
        if not isinstance(data, dict):
            raise ValueError('additional credits message must be a dict')

        if data.get('operation') != 'add_additional_credits':
            logger.warning('Unsupported additional credits operation: %s', data.get('operation'))
            return

        org_id = data.get('org_id')
        credits = data.get('credits')
        try:
            org_id = int(org_id)
            credits = float(credits)
        except (TypeError, ValueError):
            raise ValueError('org_id or credits invalid')

        self._additional_credits_service.add_credits(org_id, credits)
        logger.info('Added additional credits from redis message for org_id=%s credits=%s', org_id, credits)

    def start(self):
        if not self._enabled:
            logger.warning('Can not start additional credits worker: it is not enabled!')
            return
        Thread(target=self.receive, daemon=True).start()
