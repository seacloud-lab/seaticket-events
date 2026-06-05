from seaqa_events.seadb_models.schema_loader import SCHEMA

class SchemaTables:
    WEB_CRAWL = SCHEMA.WebCrawlTable
    GITHUB_ISSUES = SCHEMA.GithubIssuesTable
    GITHUB_ISSUE_COMMENTS = SCHEMA.GithubIssueCommentsTable
    DISCOURSE_TOPICS = SCHEMA.DiscourseTopicsTable
    DISCOURSE_REPLIES = SCHEMA.DiscourseRepliesTable
    SEAFILE = SCHEMA.SeafileTable
    TICKETS = SCHEMA.TicketsTable
    TICKET_COMMENTS = SCHEMA.TicketCommentsTable
    TICKET_ACTIVITIES = SCHEMA.TicketActivitiesTable
    EMAIL = SCHEMA.EmailTable
    THREAD = SCHEMA.ThreadTable
    KNOWLEDGE_BASE = SCHEMA.KnowledgeBaseTable
    TAG = SCHEMA.TagTable
    PORTAL_ISSUES = SCHEMA.PortalIssuesTable
    PORTAL_ISSUE_COMMENTS = SCHEMA.PortalIssueCommentsTable
    AGENT_RUNS = SCHEMA.AgentRunsTable
    AGENT_ACTIONS = SCHEMA.AgentActionsTable
    NOTION = SCHEMA.NotionTable
    GENERAL_TASK = SCHEMA.GeneralTaskTable
    GENERAL_TASK_USER = SCHEMA.GeneralTaskUserTable
