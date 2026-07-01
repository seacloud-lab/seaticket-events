# -*- coding: utf-8 -*-
import json
import logging
import os
from threading import Event, Thread

from seaqa_events.app.config import SEAQA_WEB_DIR
from seaqa_events.app.event_redis import RedisClient
from seaqa_events.utils import get_python_executable, run

__all__ = [
    'AgentAutoActionProcessor',
]

logger = logging.getLogger(__name__)

AGENT_AUTO_ACTION_QUEUE_NAME = 'agent_auto_action'


class AgentAutoActionProcessor(object):
    def __init__(self, configs):
        self._configs = configs
        self._enabled = True
        self._logfile = None
        self._prepare_logfile()

    def _prepare_logfile(self):
        logdir = os.environ.get('LOG_DIR', '')
        self._logfile = os.path.join(logdir, 'agent_auto_action_processor.log')

    def start(self):
        if not self._enabled:
            logger.warning('Can not start agent auto action processor: it is not enabled!')
            return

        logger.info('Start agent auto action processor.')
        worker = AgentAutoActionWorker(self._configs, self._logfile)
        worker.daemon = True
        worker.start()


class AgentAutoActionWorker(Thread):
    def __init__(self, configs, logfile):
        super().__init__(name='agent_auto_action_processor_worker')
        self._redis_client = RedisClient(configs, socket_timeout=10)
        self._logfile = logfile
        self.finished = Event()

    def cancel(self):
        self.finished.set()

    def run(self):
        while not self.finished.is_set():
            try:
                item = self._redis_client.connection.brpop(AGENT_AUTO_ACTION_QUEUE_NAME, timeout=5)
                if not item:
                    continue
                _, payload = item
                self._process_payload(payload)
            except Exception as e:
                logger.exception('Agent auto action worker failed: %s', e)

    def _process_payload(self, payload):
        try:
            data = json.loads(payload)
        except Exception:
            logger.warning('Invalid auto action payload: %s', payload)
            return

        project_uuid = str(data.get('project_uuid') or '').strip()
        action_id = data.get('action_id')
        if not project_uuid or action_id is None:
            logger.warning('Ignore auto action payload without project_uuid/action_id: %s', payload)
            return

        python_exec = get_python_executable()
        manage_py = os.path.join(SEAQA_WEB_DIR, 'manage.py')
        cmd = [
            python_exec,
            manage_py,
            'process_agent_auto_action',
            '--project-uuid',
            project_uuid,
            '--action-id',
            str(action_id),
        ]
        with open(self._logfile, 'a') as fp:
            run(cmd, cwd=SEAQA_WEB_DIR, output=fp)
