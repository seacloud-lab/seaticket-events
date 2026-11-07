# -*- coding: utf-8 -*-
import time

from seaqa_events.services.additional_credits import AdditionalCreditsService
from seaqa_events.tasks.ai_stats_worker import AIStatsWorker
from seaqa_events.tasks.agent_run_logs_cleaner import AgentRunLogsCleaner
from seaqa_events.tasks.email_notice_sender import EmailNoticesSender
from seaqa_events.tasks.notification_cleaner import NotificationCleaner
from seaqa_events.tasks.project_connection_cleaner import ProjectConnectionCleaner
from seaqa_events.tasks.project_storage_usage import ProjectStorageUsageCollector
from seaqa_events.tasks.issues_counter import IssuesCounter
from seaqa_events.tasks.linking_processor import LinkingProcessor
from seaqa_events.tasks.knowledge_gaps_scheduler import KnowledgeGapsScheduler


class App(object):
    def __init__(self, configs):
        # additional credits service
        self.additional_credits_service = AdditionalCreditsService(configs)

        # ai stats, listen redis and cron
        self.ai_stats_worker = AIStatsWorker(configs, self.additional_credits_service)

        # email notices sender
        self.email_notices_sender = EmailNoticesSender(configs)

        # notification cleaner
        self.notification_cleaner = NotificationCleaner(configs)

        # project/connection cleaner
        self.project_connection_cleaner = ProjectConnectionCleaner(configs)

        self.project_storage_usage_collector = ProjectStorageUsageCollector(configs)

        # agent run logs cleaner
        self.agent_run_logs_cleaner = AgentRunLogsCleaner(configs)

        # project issues counter
        self.issues_counter = IssuesCounter(configs)

        # linking processor: records activity logs on tickets linked to data events
        self.linking_processor = LinkingProcessor(configs)

        self.knowledge_gaps_scheduler = KnowledgeGapsScheduler(configs)

    def serve_forever(self):
        # ai stats worker
        self.ai_stats_worker.start()
        
        self.email_notices_sender.start()

        self.notification_cleaner.start()

        self.project_connection_cleaner.start()

        self.project_storage_usage_collector.start()

        self.agent_run_logs_cleaner.start()

        self.issues_counter.start()

        self.linking_processor.start()

        self.knowledge_gaps_scheduler.start()

        # Keep main thread alive
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass
