# -*- coding: utf-8 -*-
import logging
from threading import Thread, Event

from sqlalchemy import text

from seaqa_events.db import init_db_session_class
from seaqa_events.utils.indexer_api import cleanup_deleted_project, cleanup_deleted_connection


logger = logging.getLogger(__name__)


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
        self._clean_deleted_projects()
        self._clean_deleted_connections()

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
            self._delete_deleted_project(project_uuid)
        else:
            logger.error('Fail to cleanup deleted project %s, error: %s', project_uuid, resp.get('error_msg'))

    def _clear_deleted_connection(self, project_uuid, connection_id, connection_type):
        resp = cleanup_deleted_connection(project_uuid, connection_id, connection_type)
        if resp.get('success'):
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
        with self._db_session_class() as session:
            sql = """DELETE FROM deleted_projects WHERE project_uuid=:project_uuid"""
            session.execute(text(sql), {'project_uuid': project_uuid})
            session.commit()

    def _delete_deleted_connection(self, connection_id):
        with self._db_session_class() as session:
            sql = """DELETE FROM project_connection WHERE id=:connection_id"""
            session.execute(text(sql), {'connection_id': connection_id})
            session.commit()
