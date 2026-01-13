# -*- coding: utf-8 -*-
import uuid
import sys
import os
import logging
import subprocess
import configparser


logger = logging.getLogger(__name__)
pyexec = None

from seaqa_events.utils.constant import ConnectionType

def get_connection_table_name(connection_type, connection_id):
    table_name = ''
    if connection_type == ConnectionType.GITHUB_ISSUE.value:
        table_name = ConnectionType.GITHUB_ISSUE.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
        table_name = ConnectionType.DISCOURSE_FORUM.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.SEAFILE.value:
        table_name = ConnectionType.SEAFILE.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.SITE.value:
        table_name = ConnectionType.SITE.value + '_' + str(connection_id)
    elif connection_type == ConnectionType.EMAIL.value:
        table_name = ConnectionType.EMAIL.value + '_' + 'thread_' + str(connection_id)
    return table_name


def uuid_str_to_36_chars(project_uuid):
    if isinstance(project_uuid, uuid.UUID):
        return str(project_uuid)
    if len(project_uuid) == 32:
        return str(uuid.UUID(project_uuid))
    else:
        return project_uuid


def uuid_str_to_32_chars(project_uuid):
    if len(project_uuid) == 36:
        return uuid.UUID(project_uuid).hex
    else:
        return project_uuid

def find_in_path(prog):
    if 'win32' in sys.platform:
        sep = ';'
    else:
        sep = ':'

    dirs = os.environ['PATH'].split(sep)
    for d in dirs:
        d = d.strip()
        if d == '':
            continue
        path = os.path.join(d, prog)
        if os.path.exists(path):
            return path

    return None

def _get_python_executable():
    if sys.executable and os.path.isabs(sys.executable) and os.path.exists(sys.executable):
        return sys.executable

    try_list = [
        'python3.8',
        'python38',
        'python3.7',
        'python37',
        'python3.6',
        'python36',
    ]

    for prog in try_list:
        path = find_in_path(prog)
        if path is not None:
            return path

    path = os.environ.get('PYTHON', 'python')

    return path

def get_python_executable():
    # Find a suitable python executable
    global pyexec
    if pyexec is not None:
        return pyexec

    pyexec = _get_python_executable()
    return pyexec


def run(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False, output=None):
    def quote(args):
        return ' '.join(['"%s"' % arg for arg in args])

    cmdline = quote(argv)

    with open(os.devnull, 'w') as devnull:
        kwargs = dict(cwd=cwd, env=env, shell=True)

        if suppress_stdout:
            kwargs['stdout'] = devnull
        if suppress_stderr:
            kwargs['stderr'] = devnull

        if output:
            kwargs['stdout'] = output
            kwargs['stderr'] = output

        return subprocess.Popen(cmdline, **kwargs)
