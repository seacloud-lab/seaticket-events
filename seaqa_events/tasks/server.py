from threading import Thread
from waitress import serve

from seaqa_events.tasks.request_handler import app as application


class TasksService(Thread):

    def __init__(self, configs, app):
        Thread.__init__(self)
        self._parse_configs(configs)

        app.analysis_task_manager.init(self._workers, self.task_timeout, configs)
        app.analysis_task_manager.run()

        application.config['app'] = app

    def _parse_configs(self, configs):
        tasks_config = configs.get('TASKS', {})
        self._host = tasks_config.get('host', '127.0.0.1')
        self._port = tasks_config.get('port', 6001)
        self._workers = tasks_config.get('workers', 3)
        self.task_timeout = tasks_config.get('task_timeout', 3600)

    def run(self):
        serve(application, host=self._host, port=int(self._port))
