# -*- coding: utf-8 -*-
import configparser
import os
import uuid

def get_opt_from_conf_or_env(config, section, key, env_key=None, default=None):
    # Get option value from events.conf.
    # If not specified in events.conf, check the environment variable.
    try:
        return config.get(section, key)
    except (configparser.NoSectionError, configparser.NoOptionError):
        if env_key is None:
            return default
        else:
            return os.environ.get(env_key.upper(), default)


def parse_bool(v):
    if isinstance(v, bool):
        return v

    v = str(v).lower()

    if v == '1' or v == 'true':
        return True
    else:
        return False


def uuid_str_to_32_chars(dtable_uuid):
    if len(dtable_uuid) == 36:
        return uuid.UUID(dtable_uuid).hex
    else:
        return dtable_uuid
