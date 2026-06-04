from seaqa_events.utils.constant import ConnectionType
from seaqa_events.seadb_models.schema_loader import get_seadb_table_schemas
from seaqa_events.seadb_models.models import SchemaTableNames


class TableNameInvalidError(Exception):
    pass


class ColumnNameInvalidError(Exception):
    pass


SCHEMA_TABLE_NAME_CONNECTION = {
    ConnectionType.GITHUB_ISSUE.value: SchemaTableNames.GITHUB_ISSUES,
    ConnectionType.DISCOURSE_FORUM.value: SchemaTableNames.DISCOURSE_TOPICS,
    ConnectionType.SITE.value: SchemaTableNames.WEB_CRAWL,
    ConnectionType.SEAFILE.value: SchemaTableNames.SEAFILE,
    ConnectionType.EMAIL.value: SchemaTableNames.THREAD,
    ConnectionType.NOTION.value: SchemaTableNames.NOTION,
}

# Mapping from connection_type to the comments/replies/secondary table
SCHEMA_TABLE_NAME_CONNECTION_COMMENTS = {
    ConnectionType.GITHUB_ISSUE.value: SchemaTableNames.GITHUB_ISSUE_COMMENTS,
    ConnectionType.DISCOURSE_FORUM.value: SchemaTableNames.DISCOURSE_REPLIES,
    ConnectionType.EMAIL.value: SchemaTableNames.EMAIL,
}


def _resolve_table_name_by_schema_table_key(schema_table_name, connection_id=None):
    if '{connection_id}' in schema_table_name:
        if connection_id is None:
            raise ValueError(f'connection_id is required for table name: {schema_table_name}')
        return schema_table_name.format(connection_id=connection_id)
    return schema_table_name


def get_table_name_from_schema(schema_table_name, connection_id=None):
    """Resolve the actual seadb table name from the YAML schema for a given schema table name."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    table_name_schema = table_schema.get('table_name')
    table_name = _resolve_table_name_by_schema_table_key(table_name_schema, connection_id)
    if not table_name:
        raise TableNameInvalidError()
    return table_name


def get_column_name_from_schema(schema_table_name, column_name):
    """Return the column_name defined in YAML schema. Raises ColumnNameInvalidError if column not found."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    columns = table_schema.get('columns') or {}
    if column_name not in columns:
        raise ColumnNameInvalidError()
    return column_name


