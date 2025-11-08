# -*- coding: utf-8 -*-
import time

from seaqa_events.tasks.ai_stats_worker import AIStatsWorker


class App(object):
    def __init__(self, config):
        # ai stats, listen redis and cron
        self.ai_stats_worker = AIStatsWorker(config)

    def serve_forever(self):
        # ai stats worker
        self.ai_stats_worker.start()

        # Keep main thread alive
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass

