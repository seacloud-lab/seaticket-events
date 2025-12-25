from enum import Enum


class ConnectionType(Enum):
    EMAIL = 'email'
    GITHUB_ISSUE = 'github_issue'
    DISCOURSE_FORUM = 'discourse_forum'
    SITE = 'site'
    SEAFILE = 'seafile'


MAX_EMBEDDING_ANALYSIS_RECORDS = 100000

KB_DISPLAY_ALL_COLUMNS = ['title', 'content', 'creator', 'created_time', 'last_modifier', 'modified_time']
