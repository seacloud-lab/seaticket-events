# -*- coding: utf-8 -*-
import time

from seaqa_events.tasks.ai_stats_worker import AIStatsWorker
from seaqa_events.tasks.email_notice_sender import EmailNoticesSender


class App(object):
    def __init__(self, configs):
        # ai stats, listen redis and cron
        self.ai_stats_worker = AIStatsWorker(configs)

        # email notices sender
        self.email_notices_sender = EmailNoticesSender(configs)

    def serve_forever(self):
        # ai stats worker
        self.ai_stats_worker.start()
        
        self.email_notices_sender.start()

        # Keep main thread alive
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass

