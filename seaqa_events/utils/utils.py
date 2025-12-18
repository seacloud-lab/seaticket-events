import uuid


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
