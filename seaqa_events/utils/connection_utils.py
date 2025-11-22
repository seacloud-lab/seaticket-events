import logging
from seaqa_events.utils.constant import ConnectionType

logger = logging.getLogger(__name__)

def list_connection_records_with_columns(seadb_api, project_uuid, connection_id, connection_type, column_names, limit):
    table_name = get_connection_table_name(connection_type, connection_id)
    column_join = ', '.join(['`%s`' % column_name for column_name in column_names])
    sql = f"SELECT {column_join} FROM `{table_name}` LIMIT {limit}"
    try:
        res = seadb_api.query_rows(project_uuid, sql)
        records = res.get('results', [])
    except Exception as e:
        logger.error(f"Failed to list connection records with columns: {e}")
        return []
    return records
    

def get_connection_table_name(connection_type, connection_id):
    table_name = ''
    if connection_type == ConnectionType.GITHUB_ISSUE.value:
        table_name = ConnectionType.GITHUB_ISSUE.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
        table_name = ConnectionType.DISCOURSE_FORUM.value + '_' + str(connection_id)
    return table_name
