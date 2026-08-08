# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta
from threading import Thread, Event

from sqlalchemy import text

from seaqa_events.db import init_db_session_class
from seaqa_events.utils import uuid_str_to_36_chars
from seaqa_events.utils.indexer_api import cleanup_deleted_project, cleanup_deleted_connection
from seaqa_events.utils.seadb_api import SeaDBAPI
from seaqa_events.utils import get_connection_all_table_names
from seaqa_events.utils.constant import CONNECTION_USER_CLEANUP_TYPES
from seaqa_events.seadb_models.models import SchemaTables

logger = logging.getLogger(__name__)

TRASH_CLEANUP_DAYS = 30


class ProjectConnectionCleaner(object):
    def __init__(self, configs):
        self._db_session_class = init_db_session_class(configs)
        self._enabled = True
        self._interval = 24 * 60 * 60
        self._parse_config(configs)

    def _parse_config(self, configs):
        cleaner_config = configs.get('PROJECT_CONNECTION_CLEANER', {})
        if not cleaner_config:
            return

        self._enabled = cleaner_config.get('enabled', True)
        try:
            self._interval = int(cleaner_config.get('interval', self._interval))
        except Exception:
            self._interval = 24 * 60 * 60

    def start(self):
        if not self.is_enabled():
            logger.warning('Can not start project connection cleaner: it is not enabled!')
            return
        logger.info('Start project connection cleaner, interval=%s sec', self._interval)
        CleanProjectConnectionsTimer(self._interval, self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


class CleanProjectConnectionsTimer(Thread):
    def __init__(self, interval, db_session_class):
        Thread.__init__(self)
        self._interval = interval
        self._db_session_class = db_session_class
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                self._clean()

    def cancel(self):
        self.finished.set()

    def _clean(self):
        self._clean_expired_trash_projects()
        self._clean_deleted_projects()
        self._clean_deleted_connections()

    def _clean_expired_trash_projects(self):
        expired_projects = self._get_expired_trash_projects()
        for project in expired_projects:
            project_uuid = str(project.uuid)
            logger.info('Start moving expired trash project %s to deleted', project_uuid)
            try:
                self._move_trash_project_to_deleted(project_uuid)
                logger.info('Finish moving expired trash project %s to deleted', project_uuid)
            except Exception as e:
                logger.exception('Fail to move expired trash project %s, error: %s', project_uuid, e)

    def _get_expired_trash_projects(self):
        expiry_time = datetime.utcnow() - timedelta(days=TRASH_CLEANUP_DAYS)
        with self._db_session_class() as session:
            sql = """SELECT uuid FROM projects WHERE deleted=true AND delete_time < :expiry_time"""
            return session.execute(text(sql), {'expiry_time': expiry_time}).fetchall()

    def _move_trash_project_to_deleted(self, project_uuid):
        with self._db_session_class() as session:
            session.execute(text("""INSERT IGNORE INTO deleted_projects (project_uuid) VALUES (:project_uuid)"""), {'project_uuid': project_uuid})
            session.execute(text("""DELETE FROM projects WHERE uuid=:project_uuid"""), {'project_uuid': project_uuid})
            session.commit()

    def _clean_deleted_projects(self):
        deleted_projects = self._get_all_deleted_projects()
        for project in deleted_projects:
            project_uuid = project.project_uuid
            logger.info('Start clean deleted project %s', project_uuid)
            try:
                self._clear_deleted_project(project_uuid)
                logger.info('Finish clean deleted project %s', project_uuid)
            except Exception as e:
                logger.exception('Fail to clean deleted project %s, error: %s', project_uuid, e)

    def _clean_deleted_connections(self):
        deleted_connections = self._get_all_deleted_connections()
        for connection in deleted_connections:
            project_uuid = connection.project_uuid
            connection_id = connection.id
            connection_type = connection.type

            logger.info('Start clean deleted connection %s (type=%s)', connection_id, connection_type)
            try:
                self._clear_deleted_connection(project_uuid, connection_id, connection_type)
                logger.info('Finish clean deleted connection %s', connection_id)
            except Exception as e:
                logger.exception('Fail to clean deleted connection, project_uuid=%s, connection_id=%s, error=%s', project_uuid, connection_id, e)

    def _clear_deleted_project(self, project_uuid):
        resp = cleanup_deleted_project(project_uuid)
        if resp.get('success'):
            seadb_api = SeaDBAPI()
            seadb_api.delete_base(project_uuid)
            self._delete_deleted_project(project_uuid)
        else:
            logger.error('Fail to cleanup deleted project %s, error: %s', project_uuid, resp.get('error_msg'))

    def _clear_deleted_connection(self, project_uuid, connection_id, connection_type):
        resp = cleanup_deleted_connection(project_uuid, connection_id, connection_type)
        if resp.get('success'):
            self.cleanup_deleted_connection_tables(project_uuid, connection_id, connection_type)
            if connection_type in CONNECTION_USER_CLEANUP_TYPES:
                self._cleanup_connection_users(project_uuid, connection_id, connection_type)
            self._delete_deleted_connection(connection_id)
        else:
            logger.error('Fail to cleanup deleted connection %s, error: %s', connection_id, resp.get('error_msg'))

    def _get_all_deleted_connections(self):
        with self._db_session_class() as session:
            sql = """SELECT id, project_uuid, type FROM project_connection WHERE deleted=true"""
            return session.execute(text(sql)).fetchall()

    def _get_all_deleted_projects(self):
        with self._db_session_class() as session:
            sql = """SELECT project_uuid FROM deleted_projects"""
            return session.execute(text(sql)).fetchall()

    def _delete_deleted_project(self, project_uuid):
        project_uuid_36 = uuid_str_to_36_chars(project_uuid)
        with self._db_session_class() as session:
            # delete project views
            session.execute(text("""DELETE FROM connection_views WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid})
            session.execute(text("""DELETE FROM ticket_views WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid})
            session.execute(text("""DELETE FROM knowledge_base_views WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid})
            session.execute(text("""DELETE FROM project_notification WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid_36})
            # delete chat sessions and messages
            session.execute(text("""DELETE FROM chat_messages WHERE session_uuid IN (SELECT session_uuid FROM chat_sessions WHERE project_uuid=:project_uuid)"""), {'project_uuid': project_uuid_36})
            session.execute(text("""DELETE FROM chat_message_thought_process WHERE session_uuid IN (SELECT session_uuid FROM chat_sessions WHERE project_uuid=:project_uuid)"""), {'project_uuid': project_uuid_36})
            session.execute(text("""DELETE FROM chat_sessions WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid_36})
            # delete portal chat sessions and messages
            session.execute(text("""DELETE FROM portal_chat_messages WHERE session_uuid IN (SELECT session_uuid FROM portal_chat_sessions WHERE project_uuid=:project_uuid)"""), {'project_uuid': project_uuid_36})
            session.execute(text("""DELETE FROM portal_chat_sessions WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid_36})
            session.execute(text("""DELETE FROM portal_external_invitations WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid_36})
            # delete project issues statistics
            session.execute(text("""DELETE FROM project_issues_statistics WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid_36})
            # delete project connection
            session.execute(text("""DELETE FROM project_connection WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid})
            # delete deleted_projects record
            session.execute(text("""DELETE FROM deleted_projects WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid})
            session.execute(text("""DELETE FROM project_linear_oauth WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid})
            session.execute(text("""DELETE FROM project_github_app_installation WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid_36})
            session.execute(text("""DELETE FROM project_confluence_oauth WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid})
            session.execute(text("""DELETE FROM project_jira_oauth WHERE project_uuid=:project_uuid"""), {'project_uuid': project_uuid})
            session.commit()

    def _delete_deleted_connection(self, connection_id):
        with self._db_session_class() as session:
            sql = """DELETE FROM project_connection WHERE id=:connection_id"""
            session.execute(text(sql), {'connection_id': connection_id})
            session.commit()

    def cleanup_deleted_connection_tables(self, project_uuid, connection_id, connection_type):
        table_names = set(get_connection_all_table_names(connection_id, connection_type))
        if not table_names:
            logger.warning('No SeaDB cleanup table mapping found for connection %s (type=%s)', connection_id, connection_type)
            return

        seadb_api = SeaDBAPI()
        base_metadata = seadb_api.get_base_metadata(project_uuid)
        tables = base_metadata.get('tables', []) or []

        for table in tables:
            table_name = table.get('name')
            if table_name not in table_names:
                continue
            seadb_api.delete_table(project_uuid, table.get('id'))

    def _cleanup_connection_users(self, project_uuid, connection_id, connection_type):
        """Remove a deleted connection from all user records in connection_user.

        Queries all rows whose connection_id list contains the given connection_id,
        removes it from the list, and deletes the row entirely if the list becomes empty.
        """
        table_name = SchemaTables.CONNECTION_USER.table_name()
        seadb_api = SeaDBAPI()
        page_size = 1000
        total_cleaned = 0

        while True:
            sql = f"SELECT _pk, connection_ids FROM `{table_name}` WHERE {connection_id} IN connection_ids AND connection_type='{connection_type}' LIMIT {page_size}"
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            delete_users = []
            update_users = []
            for row in rows:
                existing_ids = row.get('connection_ids') or []
                existing_ids.remove(connection_id)
                if not existing_ids:
                    delete_users.append(row['_pk'])
                else:
                    update_users.append({
                        'pk': row['_pk'],
                        'row': {'connection_ids': existing_ids}
                    })

            if delete_users:
                seadb_api.delete_rows(project_uuid, table_name, delete_users)
            if update_users:
                seadb_api.update_rows(project_uuid, table_name, update_users)

            total_cleaned += len(rows)
            if len(rows) < page_size:
                break

        logger.info('Cleaned %d connection_user rows for deleted connection %s', total_cleaned, connection_id)
