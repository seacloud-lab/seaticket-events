# -*- coding: utf-8 -*-
import uuid
import os
import logging
import configparser


logger = logging.getLogger(__name__)

from seaqa_events.utils.constant import ConnectionType

def get_connection_table_name(connection_type, connection_id):
    table_name = ''
    if connection_type == ConnectionType.GITHUB_ISSUE.value:
        table_name = ConnectionType.GITHUB_ISSUE.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
        table_name = ConnectionType.DISCOURSE_FORUM.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.SEAFILE.value:
        table_name = ConnectionType.SEAFILE.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.SITE.value:
        table_name = ConnectionType.SITE.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.EMAIL.value:
        table_name = ConnectionType.EMAIL.value + '_' + 'thread_' + str(connection_id)
    return table_name


def uuid_str_to_36_chars(project_uuid):
    if isinstance(project_uuid, uuid.UUID):
        return str(project_uuid)
    if len(project_uuid) == 32:
        return str(uuid.UUID(project_uuid))
    else:
        return project_uuid


def uuid_str_to_32_chars(project_uuid):
    if len(project_uuid) == 36:
        return uuid.UUID(project_uuid).hex
    else:
        return project_uuid


def parse_bool(v):
    if isinstance(v, bool):
        return v

    v = str(v).lower()

    if v == '1' or v == 'true':
        return True
    else:
        return False


def get_opt_from_conf_or_env(config, key, env_key=None, default=None):
    # Get option value from events.conf.
    # If not specified in events.conf, check the environment variable.
    try:
        return config.get(key)
    except (configparser.NoSectionError, configparser.NoOptionError):
        if env_key is None:
            return default
        else:
            return os.environ.get(env_key.upper(), default)

def parse_interval(interval, default):
    if isinstance(interval, (int, int)):
        return interval

    interval = interval.lower()

    unit = 1
    if interval.endswith('s'):
        pass
    elif interval.endswith('m'):
        unit *= 60
    elif interval.endswith('h'):
        unit *= 60 * 60
    elif interval.endswith('d'):
        unit *= 60 * 60 * 24
    else:
        pass

    val = int(interval.rstrip('smhd')) * unit
    if val < 10:
        logger.warning('insane interval %s', val)
        return default
    else:
        return val
