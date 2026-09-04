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
    CONNECTION_USER = SCHEMA.ConnectionUserTable
    LINEAR_ISSUES = SCHEMA.LinearIssuesTable
    LINEAR_ISSUE_COMMENTS = SCHEMA.LinearIssueCommentsTable
    DISCORD_THREADS = SCHEMA.DiscordThreadsTable
    DISCORD_THREAD_MESSAGES = SCHEMA.DiscordThreadMessagesTable
    CONFLUENCE = SCHEMA.ConfluenceTable
    SLACK_MESSAGES = SCHEMA.SlackMessagesTable
    SLACK_MESSAGE_REPLIES = SCHEMA.SlackMessageRepliesTable
