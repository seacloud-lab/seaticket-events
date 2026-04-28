from enum import Enum


class ConnectionType(Enum):
    EMAIL = 'email'
    GITHUB_ISSUE = 'github_issue'
    DISCOURSE_FORUM = 'discourse_forum'
    SITE = 'site'
    SEAFILE = 'seafile'
    NOTION = 'notion'

class AIScenario(Enum):
    SUMMARY = 'summary'
    AGENT = 'agent'
    CHAT = 'chat'
    SEARCH = 'search'
    RECORD_GENERATION = 'record_generation'
    UNKNOWN = 'unknown'

    @classmethod
    def is_valid(cls, value):
        return value in {item.value for item in cls}

MAX_EMBEDDING_ANALYSIS_RECORDS = 100000

KB_DISPLAY_ALL_COLUMNS = ['title', 'content', 'creator', 'created_time', 'last_modifier', 'modified_time']

ISSUE_CONNECTION_TYPES = [
    ConnectionType.GITHUB_ISSUE.value,
    ConnectionType.DISCOURSE_FORUM.value,
    ConnectionType.EMAIL.value,
]
