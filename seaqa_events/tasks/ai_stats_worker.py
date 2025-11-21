# -*- coding: utf-8 -*-
import json
import logging
import time
from datetime import datetime
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from threading import Thread, Lock

from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil import relativedelta
from sqlalchemy import text

from seaqa_events.app.config import AI_PRICES
from seaqa_events.app.event_redis import RedisClient
from seaqa_events.db import init_db_session_class
from seaqa_events.utils import uuid_str_to_32_chars

logger = logging.getLogger(__name__)


class AIStatsWorker:
    def __init__(self, configs):
        self._db_session_class = init_db_session_class(configs)
        self._redis_client = RedisClient(configs, socket_timeout=10)
        self.stats_lock = Lock()
        self.channel = 'log_ai_model_usage'
        self.keep_months = 3
        self.log_none_message_timeout = 60 * 10
        self._parse_config(configs)
        self.reset_stats()

    def _parse_config(self, configs):
        self._enabled = configs.get('AI_STATS', {}).get('enabled', True)

    def reset_stats(self):
        self.org_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
        self.owner_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
        self.project_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))

    def save_to_memory(self, usage_info):
        if not usage_info.get('model'):
            return

        model = usage_info['model']
        usage = usage_info.get('usage') or {}

        if model not in AI_PRICES:
            logger.warning('model %s price not defined', model)
            return

        # Normalize token field names
        if 'prompt_tokens' in usage:
            usage['input_tokens'] = usage['prompt_tokens']
        if 'completion_tokens' in usage:
            usage['output_tokens'] = usage['completion_tokens']

        if not isinstance(usage.get('input_tokens'), int):
            usage['input_tokens'] = 0
        if not isinstance(usage.get('output_tokens'), int):
            usage['output_tokens'] = 0

        # Track organization or user level usage
        if usage_info['org_id'] != -1:
            self.org_stats[usage_info['org_id']][model]['input_tokens'] += usage.get('input_tokens') or 0
            self.org_stats[usage_info['org_id']][model]['output_tokens'] += usage.get('output_tokens') or 0
        else:
            self.owner_stats[usage_info['username']][model]['input_tokens'] += usage.get('input_tokens') or 0
            self.owner_stats[usage_info['username']][model]['output_tokens'] += usage.get('output_tokens') or 0

        # Track project-level usage
        project_uuid = usage_info.get('project_uuid')
        if project_uuid:
            project_uuid = uuid_str_to_32_chars(usage_info['project_uuid'])
            self.project_stats[project_uuid][model]['input_tokens'] += usage.get('input_tokens') or 0
            self.project_stats[project_uuid][model]['output_tokens'] += usage.get('output_tokens') or 0

    def receive(self):
        logger.info('Starts to receive ai calls...')
        subscriber = self._redis_client.get_subscriber(self.channel)
        last_message_time = datetime.now()
        while True:
            try:
                message = subscriber.get_message()
                if message is not None:
                    last_message_time = datetime.now()
                    try:
                        usage_info = json.loads(message['data'])
                    except:
                        logging.warning('log_ai_model_usage message invalid')
                        continue
                    logger.debug('usage_info %s', usage_info)
                    try:
                        with self.stats_lock:
                            self.save_to_memory(usage_info)
                    except Exception as e:
                        logger.exception('save usage_info %s to memory error %s', usage_info, e)
                else:
                    if (datetime.now() - last_message_time).seconds >= self.log_none_message_timeout:
                        logger.warning('No log_ai_model_usage message received for %s seconds', self.log_none_message_timeout)
                        last_message_time = datetime.now()
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber(self.channel)
                last_message_time = datetime.now()

    def query_project_owners(self, project_uuids):
        project_owners_dict = {}
        if not project_uuids:
            return project_owners_dict

        sql = """
            SELECT p.uuid AS project_uuid, w.owner AS owner, w.org_id as org_id
            FROM projects p
            JOIN workspaces w ON p.workspace_id=w.id
            WHERE p.uuid IN :project_uuids
        """ 
        session = self._db_session_class()
        try:
            results = session.execute(text(sql), {'project_uuids': project_uuids})
            for row in results:
                project_owners_dict[row.project_uuid] = {
                    'owner': row.owner,
                    'org_id': row.org_id or -1
                }
        except Exception as e:
            logger.exception(e)
        finally:
            session.close()
        return project_owners_dict

    def stats_worker(self):
        if not self.org_stats and not self.owner_stats and not self.project_stats:
            logger.info('There are no stats')
            return

        # Create atomic snapshot and reset
        with self.stats_lock:
            org_stats = deepcopy(self.org_stats)
            owner_stats = deepcopy(self.owner_stats)
            project_stats = deepcopy(self.project_stats)
            self.reset_stats()

        logger.info('There are %s org stats', len(org_stats))
        logger.info('There are %s owner stats (including groups with -1 org_id)', len(owner_stats))
        logger.info('There are %s project stats', len(project_stats))

        month = datetime.today().replace(day=1).date()

        # Prepare team/org statistics
        team_data = []
        team_sql = '''
        INSERT INTO `stats_ai_by_team`(`org_id`, `month`, `model`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`)
        VALUES (:org_id, :month, :model, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
        ON DUPLICATE KEY UPDATE `input_tokens`=`input_tokens`+VALUES(`input_tokens`),
                                `output_tokens`=`output_tokens`+VALUES(`output_tokens`),
                                `cost`=`cost`+VALUES(`cost`),
                                `updated_at`=VALUES(`updated_at`)
        '''
        for org_id, models_dict in org_stats.items():
            for model, usage in models_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0

                input_tokens_price = AI_PRICES[model].get('input_tokens_1k') or 0
                output_tokens_price = AI_PRICES[model].get('output_tokens_1k') or 0
                input_cost = input_tokens_price * (input_tokens / 1000)
                output_cost = output_tokens_price * (output_tokens / 1000)
                logger.info('org %s model %s, input_tokens %s cost %s, output_tokens %s cost %s',
                           org_id, model, input_tokens, input_cost, output_tokens, output_cost)

                params = {
                    'org_id': org_id,
                    'month': month,
                    'model': model,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': input_cost + output_cost,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                team_data.append(params)

        # Prepare owner/user statistics
        owner_data = []
        owner_sql = '''
        INSERT INTO `stats_ai_by_owner`(`owner_id`, `month`, `model`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`)
        VALUES (:owner_id, :month, :model, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
        ON DUPLICATE KEY UPDATE `input_tokens`=`input_tokens`+VALUES(`input_tokens`),
                                `output_tokens`=`output_tokens`+VALUES(`output_tokens`),
                                `cost`=`cost`+VALUES(`cost`),
                                `updated_at`=VALUES(`updated_at`)
        '''
        for owner_id, models_dict in owner_stats.items():
            for model, usage in models_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0

                input_tokens_price = AI_PRICES[model].get('input_tokens_1k') or 0
                output_tokens_price = AI_PRICES[model].get('output_tokens_1k') or 0
                input_cost = input_tokens_price * (input_tokens / 1000)
                output_cost = output_tokens_price * (output_tokens / 1000)
                logger.info('owner %s model %s, input_tokens %s cost %s, output_tokens %s cost %s',
                           owner_id, model, input_tokens, input_cost, output_tokens, output_cost)

                params = {
                    'owner_id': owner_id,
                    'month': month,
                    'model': model,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': input_cost + output_cost,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                owner_data.append(params)

        # Prepare project statistics (replaces dtable concept)
        project_data = []
        project_sql = '''
        INSERT INTO `stats_ai_by_project`(`project_uuid`, `date`, `model`, `owner`, `org_id`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`)
        VALUES (:project_uuid, :date, :model, :owner, :org_id, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
        ON DUPLICATE KEY UPDATE `input_tokens`=`input_tokens`+VALUES(`input_tokens`),
                                `output_tokens`=`output_tokens`+VALUES(`output_tokens`),
                                `cost`=`cost`+VALUES(`cost`),
                                `updated_at`=VALUES(`updated_at`)
        '''
        project_owners_dict = self.query_project_owners(list(project_stats.keys()))
        for project_uuid, models_dict in project_stats.items():
            for model, usage in models_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0
                owner_info = project_owners_dict.get(project_uuid)
                if owner_info:
                    owner = owner_info['owner']
                    org_id = owner_info['org_id']
                else:
                    owner = None
                    org_id = None

                input_tokens_price = AI_PRICES[model].get('input_tokens_1k') or 0
                output_tokens_price = AI_PRICES[model].get('output_tokens_1k') or 0
                input_cost = input_tokens_price * (input_tokens / 1000)
                output_cost = output_tokens_price * (output_tokens / 1000)
                logger.info('project %s model %s, input_tokens %s cost %s, output_tokens %s cost %s',
                           project_uuid, model, input_tokens, input_cost, output_tokens, output_cost)

                params = {
                    'project_uuid': project_uuid,
                    'date': datetime.today().date(),
                    'model': model,
                    'owner': owner,
                    'org_id': org_id,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': input_cost + output_cost,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                project_data.append(params)

        # Execute database inserts
        session = self._db_session_class()
        try:
            if team_data:
                session.execute(text(team_sql), team_data)
            if owner_data:
                session.execute(text(owner_sql), owner_data)
            if project_data:
                session.execute(text(project_sql), project_data)
            session.commit()
        except Exception as e:
            logger.exception(e)
        finally:
            session.close()

    def stats(self):
        """Scheduler for periodic statistics aggregation."""
        sched = BlockingScheduler()
        # Fire every 1 minute
        @sched.scheduled_job('cron', day_of_week='*', hour='*', minute='*/1', misfire_grace_time=30, max_instances=1)
        def timed_job():
            logger.info('Starts to stats ai calls in memory...')
            self.stats_worker()

        sched.start()

    def clean(self):
        """Scheduler for cleaning old statistics data."""
        sched = BlockingScheduler()
        # Fire at 0 o'clock every day
        @sched.scheduled_job('cron', day_of_week='*', hour='0', misfire_grace_time=600)
        def timed_job():
            logger.info('Starts to clean old stats ai...')
            session = self._db_session_class()
            sql1 = "DELETE FROM `stats_ai_by_team` WHERE `month` < :clean_month"
            sql2 = "DELETE FROM `stats_ai_by_owner` WHERE `month` < :clean_month"
            sql3 = "DELETE FROM `stats_ai_by_project` WHERE `date` < :clean_month"
            clean_month = (datetime.now() - relativedelta.relativedelta(months=self.keep_months)).strftime('%Y-%m-01')
            try:
                session.execute(text(sql1), {'clean_month': clean_month})
                session.execute(text(sql2), {'clean_month': clean_month})
                session.execute(text(sql3), {'clean_month': clean_month})
                session.commit()
            except Exception as e:
                logger.exception(e)
            finally:
                session.close()

        sched.start()

    def start(self):
        if not self._enabled:
            logger.warning('Can not stats AI: it is not enabled!')
            return
        Thread(target=self.receive, daemon=True).start()
        Thread(target=self.stats, daemon=True).start()
        Thread(target=self.clean, daemon=True).start()
