from enum import Enum


class ConnectionType(Enum):
    EMAIL = 'email'
    GITHUB_ISSUE = 'github_issue'
    DISCOURSE_FORUM = 'discourse_forum'
    GENERAL_TASK = 'general_task'
    SITE = 'site'
    SEAFILE = 'seafile'
    NOTION = 'notion'
    LINEAR = 'linear'
    CONFLUENCE = 'confluence'
    DISCORD = 'discord'

class AIScenario(Enum):
    SUMMARY = 'summary'
    AGENT = 'agent'
    CHAT = 'chat'
    PORTAL_CHAT = 'portal-chat'
    SEARCH = 'search'
    RECORD_GENERATION = 'record-generation'
    ANALYSIS = 'analysis'
    UNKNOWN = 'unknown'

    @classmethod
    def is_valid(cls, value):
        return value in {item.value for item in cls}

MAX_EMBEDDING_ANALYSIS_RECORDS = 100000

KB_DISPLAY_ALL_COLUMNS = ['title', 'content', 'creator', 'created_time', 'last_modifier', 'modified_time']

ISSUE_CONNECTION_TYPES = [
    ConnectionType.GITHUB_ISSUE.value,
    ConnectionType.DISCOURSE_FORUM.value,
    ConnectionType.DISCORD.value,
    ConnectionType.EMAIL.value,
]

# Connection types that store user data in the shared connection_user table.
# When a connection of these types is deleted, its id must be removed
# from all user records in connection_user.
CONNECTION_USER_CLEANUP_TYPES = [
    ConnectionType.GENERAL_TASK.value,
    ConnectionType.CONFLUENCE.value,
]
