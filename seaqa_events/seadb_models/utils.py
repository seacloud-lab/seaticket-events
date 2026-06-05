from seaqa_events.utils.constant import ConnectionType
from seaqa_events.seadb_models.models import SchemaTables


SCHEMA_TABLE_NAME_CONNECTION = {
    ConnectionType.GITHUB_ISSUE.value: SchemaTables.GITHUB_ISSUES,
    ConnectionType.DISCOURSE_FORUM.value: SchemaTables.DISCOURSE_TOPICS,
    ConnectionType.SITE.value: SchemaTables.WEB_CRAWL,
    ConnectionType.SEAFILE.value: SchemaTables.SEAFILE,
    ConnectionType.EMAIL.value: SchemaTables.THREAD,
    ConnectionType.NOTION.value: SchemaTables.NOTION,
}

# Mapping from connection_type to the comments/replies/secondary table
SCHEMA_TABLE_NAME_CONNECTION_COMMENTS = {
    ConnectionType.GITHUB_ISSUE.value: SchemaTables.GITHUB_ISSUE_COMMENTS,
    ConnectionType.DISCOURSE_FORUM.value: SchemaTables.DISCOURSE_REPLIES,
    ConnectionType.EMAIL.value: SchemaTables.EMAIL,
}
