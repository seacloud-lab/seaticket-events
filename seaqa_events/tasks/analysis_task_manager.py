import queue
import threading
import time
import uuid

from seaqa_events.tasks import task_logger
from seaqa_events.tasks.io_task_manager import log_function_call


class AnalysisTaskManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.task_results_map = {}
        self.tasks_queue = queue.Queue(10)
        self.current_task_info = {}
        self.threads = []
        self.conf = {}
        self.project_tasks_map = {}

    def init(self, workers, task_timeout, config):
        self.conf['task_timeout'] = task_timeout
        self.conf['workers'] = workers

    def is_valid_task_id(self, task_id):
        return task_id in self.tasks_map or task_id in self.task_results_map

    def has_running_task(self, project_uuid):
        """Check if the project has a running task"""
        return project_uuid in self.project_tasks_map

    @log_function_call
    def add_embedding_analysis_task(self, project_uuid, connection_ids, username, start_year=None, end_year=None):
        from seaqa_events.tasks import perform_embedding_analysis

        task_id = str(uuid.uuid4())
        task = (perform_embedding_analysis,
                (project_uuid, connection_ids, username, start_year, end_year))
        self.tasks_map[task_id] = task
        self.tasks_queue.put(task_id)
        self.project_tasks_map[project_uuid] = task_id

        return task_id

    def query_status(self, task_id):
        task_result = self.task_results_map.pop(task_id, None)
        if not task_result:
            if task_id in self.tasks_map:
                return False, None
            else:
                return None, None
        return True, task_result

    def threads_is_alive(self):
        info = {}
        for t in self.threads:
            info[t.name] = t.is_alive()
        return info

    def handle_task(self):
        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                task_logger.error(e)
                continue

            task = self.tasks_map.get(task_id)
            if type(task) != tuple or len(task) < 1:
                continue
            if type(task[0]).__name__ != 'function':
                continue
            task_info = task_id + ' ' + str(task[0])
            try:
                self.current_task_info[task_id] = task_info
                task_logger.info('Run task: %s' % task_info)
                start_time = time.time()

                task_result = task[0](*task[1])
                if isinstance(task_result, dict):
                    task_result['success'] = True
                else:
                    task_result = {'success': True}

                self.task_results_map[task_id] = task_result

                finish_time = time.time()
                task_logger.info('Run task success: %s cost %ds \n' % (task_info, int(finish_time - start_time)))
                self.current_task_info.pop(task_id, None)
            except Exception as e:
                error_msg = str(e.args[0]) if e.args else str(e)
                self.task_results_map[task_id] = {
                    'success': False,
                    'error_msg': error_msg
                }
                task_logger.exception(e)
                task_logger.error('Failed to handle task %s, args: %s, error: %s \n' % (task_info, task[1], e))
                self.current_task_info.pop(task_id, None)
            finally:
                self.tasks_map.pop(task_id, None)
                project_uuid = task[1][0] if task and len(task) > 1 and task[1] else None
                if project_uuid:
                    self.project_tasks_map.pop(project_uuid, None)

    def run(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'Tasks AnalysisTaskManager Thread-' + str(i)
            t = threading.Thread(target=self.handle_task, name=t_name)
            self.threads.append(t)
            t.daemon = True
            t.start()
