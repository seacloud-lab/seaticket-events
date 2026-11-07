import time
import jwt
import requests
from urllib.parse import urljoin

from seaqa_events.app.config import JWT_PRIVATE_KEY, SEAQA_AI_INNER_SERVER_URL


DEFAULT_TIMEOUT = 30


def _build_headers():
    payload = {'exp': int(time.time()) + 300}
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    return {'Authorization': f'Token {token}'}


def _parse_response(resp):
    if resp.status_code < 200 or resp.status_code >= 300:
        raise ConnectionError(resp.status_code, resp.text)
    return resp.json()


def analyze_knowledge_gaps(project_uuid):
    headers = _build_headers()
    url = urljoin(SEAQA_AI_INNER_SERVER_URL, '/internal/analyze/knowledge-gaps')
    resp = requests.post(
        url,
        json={'project_uuid': project_uuid},
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )
    return _parse_response(resp)
