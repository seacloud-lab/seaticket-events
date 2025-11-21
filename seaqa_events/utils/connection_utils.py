import logging
from seaqa_events.utils.constant import ConnectionType

logger = logging.getLogger(__name__)

def list_connection_records_with_columns(seadb_api, project_uuid, connection_id, column_names, limit):
    table_name = ConnectionType.GITHUB_ISSUE.value + '_' + str(connection_id)
    column_join = ', '.join(['`%s`' % column_name for column_name in column_names])
    sql = f"SELECT {column_join} FROM `{table_name}` LIMIT {limit}"
    try:
        res = seadb_api.query_rows(project_uuid, sql)
        records = res.get('results', [])
    except Exception as e:
        logger.error(f"Failed to list connection records with columns: {e}")
        return []
    return records
    
