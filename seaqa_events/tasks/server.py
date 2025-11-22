from threading import Thread
from waitress import serve

from seaqa_events.tasks.request_handler import app as application
from seaqa_events.tasks.task_manager import task_manager


class TasksService(Thread):

    def __init__(self, config):
        Thread.__init__(self)
        self._parse_config(config)
        
        task_manager.init(self._workers, self.task_timeout, config)
        task_manager.run()

    def _parse_config(self, config):
        self._host = '127.0.0.1'
        self._port = '6001'
        self._workers = 3
        self.task_timeout = 3600

        section_name = 'TASKS'
        if not config.has_section(section_name):
            section_name = 'TASKS'

        if config.has_option(section_name, 'host'):
            self._host = config.get(section_name, 'host')
        if config.has_option(section_name, 'port'):
            self._port = config.getint(section_name, 'port')  

        if config.has_option(section_name, 'workers'):
            self._workers = config.getint(section_name, 'workers')

        if config.has_option(section_name, 'task_timeout'):
            self._io_task_timeout = config.getint(section_name, 'task_timeout')
            

    def run(self):
        serve(application, host=self._host, port=int(self._port))
