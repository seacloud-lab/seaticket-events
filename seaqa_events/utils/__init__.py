# -*- coding: utf-8 -*-
import uuid

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
