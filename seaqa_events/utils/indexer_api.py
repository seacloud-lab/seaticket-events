import time
import jwt
import requests
from urllib.parse import urljoin

from seaqa_events.app.config import JWT_PRIVATE_KEY, SEAQA_INDEXER_INNER_SERVER_URL


DEFAULT_TIMEOUT = 30


def _build_headers():
    payload = {'exp': int(time.time()) + 300}
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    return {'Authorization': f'Token {token}'}


def _parse_response(resp):
    if resp.status_code < 200 or resp.status_code >= 300:
        raise ConnectionError(resp.status_code, resp.text)
    return resp.json()


def cleanup_deleted_project(project_uuid):
    headers = _build_headers()
    url = urljoin(SEAQA_INDEXER_INNER_SERVER_URL, '/cleanup-deleted-project')
    resp = requests.post(url, json={'project_uuid': project_uuid}, headers=headers, timeout=DEFAULT_TIMEOUT)
    return _parse_response(resp)


def cleanup_deleted_connection(project_uuid, connection_id, connection_type):
    headers = _build_headers()
    url = urljoin(SEAQA_INDEXER_INNER_SERVER_URL, '/cleanup-deleted-connection')
    payload = {
        'project_uuid': project_uuid,
        'connection_id': connection_id,
        'connection_type': connection_type,
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=DEFAULT_TIMEOUT)
    return _parse_response(resp)


def get_project_storage_usage(project_uuid, timeout=60):
    headers = _build_headers()
    url = urljoin(SEAQA_INDEXER_INNER_SERVER_URL, '/storage-usage/project')
    resp = requests.post(url, json={'project_uuid': project_uuid}, headers=headers, timeout=timeout)
    return _parse_response(resp)
