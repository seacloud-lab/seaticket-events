import logging
import os
import threading

import yaml

logger = logging.getLogger(__name__)

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_CACHE = None


def _schema_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'table_schemas.yaml')


def preload_seadb_table_schemas():
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE

    with _SCHEMA_LOCK:
        if _SCHEMA_CACHE is not None:
            return _SCHEMA_CACHE
        yaml_path = _schema_file_path()
        if not os.path.isfile(yaml_path):
            raise FileNotFoundError(f'Seadb table schema file not found: {yaml_path}')
        logger.info(f'Loading seadb table schemas from: {yaml_path}')
        with open(yaml_path, 'r', encoding='utf-8') as fp:
            _SCHEMA_CACHE = yaml.safe_load(fp) or {}
    return _SCHEMA_CACHE


def get_seadb_table_schemas():
    return preload_seadb_table_schemas()
