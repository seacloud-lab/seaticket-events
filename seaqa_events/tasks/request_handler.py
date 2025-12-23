import json
import jwt
import logging

from flask import Flask, request, make_response

from seaqa_events.app.config import JWT_PRIVATE_KEY
from seaqa_events.tasks import task_logger
from seaqa_events.tasks.task_manager import task_manager

app = Flask(__name__)
logger = logging.getLogger(__name__)


def check_auth_token(req):
    auth = req.headers.get('Authorization', '').split()
    if not auth or auth[0].lower() != 'token' or len(auth) != 2:
        return False, 'Token invalid.'

    token = auth[1]
    if not token:
        return False, 'Token invalid.'

    private_key = JWT_PRIVATE_KEY
    try:
        jwt.decode(token, private_key, algorithms=['HS256'])
    except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
        return False, str(e)

    return True, None


@app.route('/add-embedding-analysis-task', methods=['POST'])
def add_embedding_analysis_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        task_logger.warning('tasks server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('tasks server busy.', 400))

    try:
        context = json.loads(request.data)
    except:
        return make_response(('add embedding analysis task context invalid.', 400))

    project_uuid = context.get('project_uuid')
    connection_ids = context.get('connection_ids')
    username = context.get('username')

    if not project_uuid:
        return make_response(('project_uuid is required.', 400))
    if not connection_ids:
        return make_response(('connection_ids is required.', 400))
    if not username:
        return make_response(('username is required.', 400))

    try:
        task_id = task_manager.add_embedding_analysis_task(
            project_uuid, connection_ids, username)
    except Exception as e:
        task_logger.error(e)
        return make_response((str(e), 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/convert-kb-view-to-excel', methods=['POST'])
def convert_kb_view_to_excel():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        task_logger.warning('tasks server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('tasks server busy.', 400))

    try:
        context = json.loads(request.data)
    except:
        return make_response(('convert kb view to excel context invalid.', 400))

    project_uuid = context.get('project_uuid')
    view_id = context.get('view_id')
    username = context.get('username')

    if not project_uuid:
        return make_response(('project_uuid is required.', 400))
    if not view_id:
        return make_response(('view_id is required.', 400))
    if not username:
        return make_response(('username is required.', 400))

    try:
        task_id = task_manager.add_convert_kb_view_to_excel_task(project_uuid, view_id, username)
    except Exception as e:
        task_logger.error(e)
        return make_response((str(e), 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/import-kb-from-excel', methods=['POST'])
def import_kb_from_excel():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        task_logger.warning('tasks server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('tasks server busy.', 400))

    try:
        context = json.loads(request.data)
    except:
        return make_response(('import kb from excel context invalid.', 400))

    project_uuid = context.get('project_uuid')
    username = context.get('username')
    file_name = context.get('file_name')
    preview_only = context.get('preview_only', False)

    if not project_uuid:
        return make_response(('project_uuid is required.', 400))
    if not username:
        return make_response(('username is required.', 400))
    if not file_name:
        return make_response(('file name is required.', 400))

    try:
        if preview_only:
            task_id = task_manager.add_preview_import_kb_excel_task(project_uuid, file_name)
        else:
            task_id = task_manager.add_import_kb_from_excel_task(project_uuid, username, file_name)
    except Exception as e:
        task_logger.error(e)
        return make_response((str(e), 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/embedding-analysis-task-status', methods=['GET'])
def query_embedding_analysis_task_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response(({'error_msg': error}, 403))

    task_id = request.args.get('task_id')
    if not task_manager.is_valid_task_id(task_id):
        return make_response(({'error_msg': 'task_id not found.'}, 404))

    try:
        is_finished, task_result = task_manager.query_status(task_id)

    except Exception as e:
        task_logger.exception(e)
        return make_response(({'error_msg': 'Internal Server Error'}, 500))

    if is_finished is None:
        return make_response(({'error_msg': 'task_id not found.'}, 404))

    if task_result and not task_result.get('success'):
        return make_response((task_result, 500))

    return_result = {'is_finished': is_finished}
    if isinstance(task_result, dict):
        return_result.update(task_result)

    return make_response((return_result, 200))


@app.route('/kb-task-status', methods=['GET'])
def query_kb_task_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response(({'error_msg': error}, 403))

    task_id = request.args.get('task_id')
    if not task_manager.is_valid_task_id(task_id):
        return make_response(({'error_msg': 'task_id not found.'}, 404))

    try:
        is_finished, task_result = task_manager.query_status(task_id)
    except Exception as e:
        task_logger.exception(e)
        return make_response(({'error_msg': 'Internal Server Error'}, 500))

    if is_finished is None:
        return make_response(({'error_msg': 'task_id not found.'}, 404))

    if task_result and not task_result.get('success'):
        return make_response((task_result, 500))

    return_result = {'is_finished': is_finished}
    if isinstance(task_result, dict):
        return_result.update(task_result)

    return make_response((return_result, 200))
