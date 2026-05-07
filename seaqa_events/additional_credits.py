# -*- coding: utf-8 -*-
import logging
from datetime import date

from sqlalchemy import text

logger = logging.getLogger(__name__)

TEAM_FREE = 'free'
TEAM_STARTER = 'start'
TEAM_PRO = 'pro'
TEAM_BUSINESS = 'business'
TEAM_ENTERPRISE = 'enterprise'


DEFAULT_ENABLED_ROLE_PERMISSIONS = {
    TEAM_FREE: {
        'can_add_project': True,
        'can_add_group': True,
        'can_use_saml': False,
        'ai_credit': -1,
        'can_use_advanced_customization': True,
    },
    TEAM_STARTER: {
        'can_add_project': True,
        'can_add_group': True,
        'can_use_saml': False,
        'ai_credit': -1,
        'can_use_advanced_customization': True,
    },
    TEAM_PRO: {
        'can_add_project': True,
        'can_add_group': True,
        'can_use_saml': False,
        'ai_credit': -1,
        'can_use_advanced_customization': True,
    },
    TEAM_BUSINESS: {
        'can_add_project': True,
        'can_add_group': True,
        'can_use_saml': True,
        'ai_credit': -1,
        'can_use_advanced_customization': True,
    },
    TEAM_ENTERPRISE: {
        'can_add_project': True,
        'can_add_group': True,
        'can_use_saml': True,
        'ai_credit': -1,
        'can_use_advanced_customization': True,
    },
}


def get_enabled_role_permissions_by_role(role):
    if not role:
        role = TEAM_FREE

    if role not in DEFAULT_ENABLED_ROLE_PERMISSIONS:
        logger.warning('%s is not a valid role, use default role.', role)
        role = TEAM_FREE

    return DEFAULT_ENABLED_ROLE_PERMISSIONS[role]


class AdditionalCreditsService:
    def __init__(self, db_session_class, configs):
        self._db_session_class = db_session_class
        self._configs = configs

    def _get_monthly_credit_by_role(self, role):
        try:
            return float(get_enabled_role_permissions_by_role(role).get('ai_credit', -1))
        except (TypeError, ValueError):
            logger.warning('Invalid default ai_credit config for role %s', role)
            return -1

    def get_monthly_credit_by_org_id(self, org_id):
        session = self._db_session_class()
        try:
            result = session.execute(
                text('SELECT role FROM org_settings WHERE org_id=:org_id LIMIT 1'),
                {'org_id': org_id},
            ).fetchone()
            role = (result[0] if result and result[0] else 'free').strip().lower()
            return self._get_monthly_credit_by_role(role)
        except Exception as e:
            logger.exception('Failed to get monthly credit by org_id %s: %s', org_id, e)
            return -1
        finally:
            session.close()

    def add_credits(self, org_id, credits):
        credits = float(credits)
        if credits <= 0:
            raise ValueError('credits must be greater than 0')

        session = self._db_session_class()
        try:
            session.execute(text('''
                INSERT INTO additional_credits (org_id, credits, updated_at)
                VALUES (:org_id, :credits, NOW())
                ON DUPLICATE KEY UPDATE
                    credits = credits + VALUES(credits),
                    updated_at = VALUES(updated_at)
            '''), {
                'org_id': org_id,
                'credits': credits,
            })
            session.commit()
        except Exception as e:
            session.rollback()
            logger.exception('Failed to add additional credits for org_id %s: %s', org_id, e)
            raise
        finally:
            session.close()

    def get_month_used_credits(self, org_id, today=None):
        month_today = today or date.today()
        month_start = month_today.replace(day=1)
        session = self._db_session_class()
        try:
            result = session.execute(text('''
                SELECT COALESCE(SUM(cost), 0)
                FROM ai_usage_statistics
                WHERE org_id=:org_id AND date>=:month_start
            '''), {
                'org_id': org_id,
                'month_start': month_start,
            }).fetchone()
            total_cost = float(result[0] or 0)
            return total_cost * 100
        except Exception as e:
            logger.exception('Failed to get month used credits for org_id %s: %s', org_id, e)
            return 0
        finally:
            session.close()

    def deduct_overflow_credits(self, org_id, cost_delta):
        cost_delta = float(cost_delta)
        if cost_delta <= 0:
            return None

        monthly_credit = self.get_monthly_credit_by_org_id(org_id)
        if monthly_credit < 0:
            return {
                'org_id': org_id,
                'monthly_credit': monthly_credit,
                'overflow_credits_to_deduct': 0,
                'additional_remaining': None,
            }

        current_used_credits = self.get_month_used_credits(org_id)
        current_overflow = max(0.0, current_used_credits - monthly_credit)
        previous_used_credits = max(0.0, current_used_credits - cost_delta * 100)
        previous_overflow = max(0.0, previous_used_credits - monthly_credit)
        overflow_credits_to_deduct = current_overflow - previous_overflow

        if overflow_credits_to_deduct <= 0:
            return {
                'org_id': org_id,
                'monthly_credit': monthly_credit,
                'overflow_credits_to_deduct': 0,
                'additional_remaining': None,
            }

        session = self._db_session_class()
        try:
            row = session.execute(
                text('SELECT credits FROM additional_credits WHERE org_id=:org_id FOR UPDATE'),
                {'org_id': org_id},
            ).fetchone()
            current_additional = float(row[0]) if row else 0.0
            deducted = min(current_additional, overflow_credits_to_deduct)
            remaining = current_additional - deducted

            if row:
                session.execute(
                    text('UPDATE additional_credits SET credits=:credits, updated_at=NOW() WHERE org_id=:org_id'),
                    {'credits': remaining, 'org_id': org_id},
                )
            elif overflow_credits_to_deduct > 0:
                session.execute(text('''
                    INSERT INTO additional_credits (org_id, credits, updated_at)
                    VALUES (:org_id, 0, NOW())
                    ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at)
                '''), {'org_id': org_id})

            session.commit()
            return {
                'org_id': org_id,
                'monthly_credit': monthly_credit,
                'overflow_credits_to_deduct': overflow_credits_to_deduct,
                'deducted_from_additional': deducted,
                'not_covered': overflow_credits_to_deduct - deducted,
                'additional_remaining': remaining,
            }
        except Exception as e:
            session.rollback()
            logger.exception('Failed to deduct overflow additional credits for org_id %s: %s', org_id, e)
            raise
        finally:
            session.close()
