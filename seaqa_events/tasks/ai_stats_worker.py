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
from seaqa_events.utils.constant import AIScenario

logger = logging.getLogger(__name__)


class AIStatsWorker:
    def __init__(self, configs, additional_credits_service=None):
        self._db_session_class = init_db_session_class(configs)
        self._redis_client = RedisClient(configs, socket_timeout=10)
        self._additional_credits_service = additional_credits_service
        self.stats_lock = Lock()
        self.channel = 'log_ai_model_usage'
        self.keep_months = 3
        self.log_none_message_timeout = 60 * 10
        self._parse_config(configs)
        self.reset_stats()

    def _parse_config(self, configs):
        self._enabled = configs.get('AI_STATS', {}).get('enabled', True)

    def reset_stats(self):
        self.ai_usage_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
        # { project_uuid: org_id } - cache to avoid repeated database queries
        self.project_org_cache = {}
        self.project_workspace_owner_cache = {}

    def save_to_memory(self, usage_info):
        """
        usage_info:
        {
            'model': 'gpt-4o-mini-2024-07-18',
            'usage': {'prompt_tokens': 1678, 'completion_tokens': 35, 'total_tokens': 1713},
            'project_uuid': '5680b4426dc0418b9e2ae176627f4d94',
            'scenario': 'chat',
            'org_id': 1

        }
        """
        if not usage_info.get('model'):
            return

        model = usage_info['model']
        usage = usage_info.get('usage') or {}
        project_uuid = usage_info.get('project_uuid')

        if model not in AI_PRICES:
            logger.warning('model %s price not defined', model)
            return

        # Normalize token field names (prompt_tokens -> input_tokens, completion_tokens -> output_tokens)
        input_tokens = usage.get('prompt_tokens') or usage.get('input_tokens') or 0
        output_tokens = usage.get('completion_tokens') or usage.get('output_tokens') or 0

        if not isinstance(input_tokens, int):
            input_tokens = 0
        if not isinstance(output_tokens, int):
            output_tokens = 0

        org_id = usage_info.get('org_id')
        scenario = usage_info.get('scenario')
        if not isinstance(scenario, str):
            scenario = AIScenario.UNKNOWN.value
        else:
            scenario = scenario.strip().lower() or AIScenario.UNKNOWN.value
            if not AIScenario.is_valid(scenario):
                scenario = AIScenario.UNKNOWN.value
        project_uuid = uuid_str_to_32_chars(project_uuid)
        if org_id is None:
            if project_uuid in self.project_org_cache:
                org_id = self.project_org_cache[project_uuid]
            else:
                org_id = self._get_org_id_by_project_uuid(project_uuid)
                if org_id is None:
                    logger.warning('project %s org_id not found', project_uuid)
                    return
        self.project_org_cache[project_uuid] = org_id
        
        if project_uuid in self.project_workspace_owner_cache:
            workspace_owner = self.project_workspace_owner_cache[project_uuid]
        else:
            workspace_owner = self._get_workspace_owner_by_project_uuid(project_uuid)
            if workspace_owner is None:
                logger.warning('Workspace of project %s not found', project_uuid)
                return
            self.project_workspace_owner_cache[project_uuid] = workspace_owner

        # project_stats
        key = (model, scenario)
        stats = self.ai_usage_stats[project_uuid][key]
        stats['input_tokens'] += input_tokens
        stats['output_tokens'] += output_tokens

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

    def stats_worker(self):
        if not self.ai_usage_stats:
            logger.debug('There are no stats')
            return

        # Create atomic snapshot and reset
        with self.stats_lock:
            project_stats = deepcopy(self.ai_usage_stats)
            project_org_cache = deepcopy(self.project_org_cache)
            project_workspace_owner_cache = deepcopy(self.project_workspace_owner_cache)
            self.reset_stats()

        logger.debug('There are %s project stats', len(project_stats))

        today = datetime.today().date()
        now = datetime.now()

        # Prepare statistics (ai_usage_statistics)
        # due to only has key `id` is unique, so the update will be performed using multiple SQL statements.
        select_sql = '''
        SELECT `id` FROM `ai_usage_statistics`
        WHERE `project_uuid`=:project_uuid AND `date`=:date AND `model`=:model AND `owner`=:owner AND `group_id`=:group_id AND `scenario`=:scenario
        LIMIT 1
        '''
        update_sql = '''
        UPDATE `ai_usage_statistics`
        SET `input_tokens`=`input_tokens`+:input_tokens,
            `output_tokens`=`output_tokens`+:output_tokens,
            `cost`=`cost`+:cost,
            `updated_at`=:updated_at
        WHERE `id`=:id
        '''
        insert_sql = '''
        INSERT INTO `ai_usage_statistics`(`project_uuid`, `date`, `model`, `owner`, `group_id`, `org_id`, `scenario`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`)
        VALUES (:project_uuid, :date, :model, :owner, :group_id, :org_id, :scenario, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
        '''
        project_data = []
        org_cost_delta = defaultdict(float)
        for project_uuid, stats_dict in project_stats.items():
            org_id = project_org_cache.get(project_uuid)
            workspace_owner = project_workspace_owner_cache.get(project_uuid)
            for (model, scenario), usage in stats_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0
                input_tokens_price = AI_PRICES[model].get('input_tokens', 0)
                output_tokens_price = AI_PRICES[model].get('output_tokens', 0)
                cost = input_tokens_price * (input_tokens / 1e6) + output_tokens_price * (output_tokens / 1e6)

                owner = None
                group_id = None

                if workspace_owner.endswith('@seafile_group'):
                    group_id = int(workspace_owner.rsplit('@seafile_group', 1)[0])
                else:
                    owner = workspace_owner

                logger.debug('project %s model %s owner %s group_id %s scenario %s org_id %s, input_tokens %s, output_tokens %s, cost %s',
                           project_uuid, model, owner, group_id, scenario, org_id, input_tokens, output_tokens, cost)

                project_data.append({
                    'project_uuid': project_uuid,
                    'date': today,
                    'model': model,
                    'owner': owner,
                    'group_id': group_id,
                    'org_id': org_id,
                    'scenario': scenario,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': cost,
                    'created_at': now,
                    'updated_at': now
                })
                if org_id is not None:
                    org_cost_delta[org_id] += cost

        # Execute database operations
        session = self._db_session_class()
        try:

            # For project stats: check existence then update or insert
            for data in project_data:
                result = session.execute(text(select_sql), {
                    'project_uuid': data['project_uuid'],
                    'date': data['date'],
                    'model': data['model'],
                    'owner': data['owner'],
                    'group_id': data['group_id'],
                    'scenario': data['scenario'],
                }).fetchone()

                if result:
                    session.execute(text(update_sql), {
                        'id': result[0],
                        'input_tokens': data['input_tokens'],
                        'output_tokens': data['output_tokens'],
                        'cost': data['cost'],
                        'updated_at': data['updated_at']
                    })
                else:
                    session.execute(text(insert_sql), data)

            session.commit()
        except Exception as e:
            logger.exception(e)
        finally:
            session.close()
        for org_id, cost_delta in org_cost_delta.items():
            try:
                result = self._additional_credits_service.deduct_overflow_credits(org_id, cost_delta)
                if result and result.get('overflow_credits_to_deduct'):
                    logger.info(
                        'Deduct additional credits for org_id=%s monthly_credit=%s overflow=%s deducted=%s not_covered=%s remaining=%s',
                        org_id,
                        result.get('monthly_credit'),
                        result.get('overflow_credits_to_deduct'),
                        result.get('deducted_from_additional'),
                        result.get('not_covered'),
                        result.get('additional_remaining'),
                    )
            except Exception as e:
                logger.exception('Failed to deduct additional credits for org_id=%s: %s', org_id, e)

    def _get_org_id_by_project_uuid(self, project_uuid):
        session = self._db_session_class()
        org_id = None
        try:
            sql = """SELECT workspace_id FROM projects WHERE uuid=:project_uuid AND deleted=false"""
            result = session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()
            if result:
                workspace_id = result[0]
                sql = """SELECT org_id FROM workspaces WHERE id=:workspace_id AND deleted=false"""
                workspace = session.execute(text(sql), {'workspace_id': workspace_id}).fetchone()
                if workspace:
                    org_id = workspace[0]
            return org_id
        except Exception as e:
            logger.exception(e)
        finally:
            session.close()
    
    def _get_workspace_owner_by_project_uuid(self, project_uuid):
        session = self._db_session_class()
        try:
            sql = """SELECT workspace_id FROM projects WHERE uuid=:project_uuid AND deleted=false"""
            result = session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()
            if result:
                workspace_id = result[0]
                sql = """SELECT owner FROM workspaces WHERE id=:workspace_id AND deleted=false"""
                workspace = session.execute(text(sql), {'workspace_id': workspace_id}).fetchone()
                if workspace:
                    return workspace[0]

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
            logger.debug('Starts to stats ai calls in memory...')
            self.stats_worker()

        sched.start()

    def clean(self):
        """Scheduler for cleaning old statistics data."""
        sched = BlockingScheduler()
        # Fire at 0 o'clock every day
        @sched.scheduled_job('cron', day_of_week='*', hour='0', misfire_grace_time=600)
        def timed_job():
            logger.debug('Starts to clean old stats ai...')
            session = self._db_session_class()
            sql = "DELETE FROM `ai_usage_statistics` WHERE `date` < :clean_month"
            clean_month = (datetime.now() - relativedelta.relativedelta(months=self.keep_months)).strftime('%Y-%m-01')
            try:
                session.execute(text(sql), {'clean_month': clean_month})
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
