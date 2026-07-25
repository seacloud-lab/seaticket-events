from seaqa_events.utils.constant import ConnectionType
from seaqa_events.seadb_models.models import SchemaTables


SCHEMA_TABLE_NAME_CONNECTION = {
    ConnectionType.GITHUB_ISSUE.value: SchemaTables.GITHUB_ISSUES,
    ConnectionType.DISCOURSE_FORUM.value: SchemaTables.DISCOURSE_TOPICS,
    ConnectionType.SITE.value: SchemaTables.WEB_CRAWL,
    ConnectionType.SEAFILE.value: SchemaTables.SEAFILE,
    ConnectionType.EMAIL.value: SchemaTables.THREAD,
    ConnectionType.NOTION.value: SchemaTables.NOTION,
    ConnectionType.GENERAL_TASK.value: SchemaTables.GENERAL_TASK,
    ConnectionType.LINEAR.value: SchemaTables.LINEAR_ISSUES,
    ConnectionType.CONFLUENCE.value: SchemaTables.CONFLUENCE,
    ConnectionType.DISCORD.value: SchemaTables.DISCORD_THREADS,
}

# Mapping from connection_type to the comments/replies/secondary table
SCHEMA_TABLE_NAME_CONNECTION_COMMENTS = {
    ConnectionType.GITHUB_ISSUE.value: SchemaTables.GITHUB_ISSUE_COMMENTS,
    ConnectionType.DISCOURSE_FORUM.value: SchemaTables.DISCOURSE_REPLIES,
    ConnectionType.EMAIL.value: SchemaTables.EMAIL,
    ConnectionType.LINEAR.value: SchemaTables.LINEAR_ISSUE_COMMENTS,
    ConnectionType.DISCORD.value: SchemaTables.DISCORD_THREAD_MESSAGES,
}
