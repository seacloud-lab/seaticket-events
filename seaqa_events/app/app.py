# -*- coding: utf-8 -*-
import time

from seaqa_events.tasks.ai_stats_worker import AIStatsWorker
from seaqa_events.tasks.email_notice_sender import EmailNoticesSender
from seaqa_events.tasks.notification_cleaner import NotificationCleaner
from seaqa_events.tasks.issues_counter import IssuesCounter
from seaqa_events.tasks.linking_processor import LinkingProcessor


class App(object):
    def __init__(self, configs):
        # ai stats, listen redis and cron
        self.ai_stats_worker = AIStatsWorker(configs)

        # email notices sender
        self.email_notices_sender = EmailNoticesSender(configs)

        # notification cleaner
        self.notification_cleaner = NotificationCleaner(configs)

        # project issues counter
        self.issues_counter = IssuesCounter(configs)

        # linking processor: records activity logs on tickets linked to data events
        self.linking_processor = LinkingProcessor(configs)

    def serve_forever(self):
        # ai stats worker
        self.ai_stats_worker.start()
        
        self.email_notices_sender.start()

        self.notification_cleaner.start()

        self.issues_counter.start()

        self.linking_processor.start()

        # Keep main thread alive
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass

