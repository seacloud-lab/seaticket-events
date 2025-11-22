# -*- coding: utf-8 -*-
import logging
import os
import sys
import yaml

logger = logging.getLogger(__name__)


# SEAQA_WEB_DIR - required environment variable
seaqa_web_dir = os.environ.get('SEAQA_WEB_DIR', '')
if not seaqa_web_dir:
    logging.critical('SEAQA_WEB_DIR is not set')
    raise RuntimeError('SEAQA_WEB_DIR is not set')
if not os.path.exists(seaqa_web_dir):
    logging.critical('SEAQA_WEB_DIR %s does not exist' % seaqa_web_dir)
    raise RuntimeError('SEAQA_WEB_DIR does not exist.')

sys.path.insert(0, seaqa_web_dir)

try:
    import seahub.settings as seaqa_settings

    # AI-related settings
    AI_PRICES = getattr(seaqa_settings, 'AI_PRICES', {})
    JWT_PRIVATE_KEY = getattr(seaqa_settings, 'JWT_PRIVATE_KEY', '')
    SEADB_INNER_SERVER_URL = getattr(seaqa_settings, 'SEADB_INNER_SERVER_URL', '')

except Exception as e:
    logger.critical("Can not import seaqa_web settings: %s." % e)
    raise RuntimeError("Can not import seaqa_web settings: %s" % e)


def _read_yaml(yaml_file_path=None, component_name=None):
    if yaml_file_path:
        # first read, get vars from global
        configs = {}
        try:
            with open(yaml_file_path, 'r', encoding='utf-8') as yaml_file:
                current_yaml_config = yaml.safe_load(yaml_file)
                configs = current_yaml_config.get('global', {})
                if component_name:
                    component_config = current_yaml_config.get(component_name, {})
                    configs.update(component_config)
                    if 'from_yaml' in component_config:
                        del configs['from_yaml']
                        configs.update(_read_yaml(component_config['from_yaml']))
        except Exception as e:
            logger.error(f'Failure to read YAML config file: {e}')
        return configs
    return {}

def _check_type(func):
    def wrapper(self, key, default=None, check_type=True):
        result = func(self, key, default)
        if check_type:
            if (need_type := type(default)) in (int, float):
                try:
                    result = need_type(result)
                except:
                    raise ValueError(f'Type of {key} must be a number')
            elif need_type == bool:
                if isinstance(result, str):
                    result = result.lower() in ('true', '1')
                else:
                    result = bool(result)
        return result
    return wrapper

class _ConfigParser(object):
    def __init__(self, yaml_file_path, component_name):
        assert yaml_file_path and component_name, "yaml_file_path and component_name must be specified in initilizing ConfigParser"
        self.refresh_yaml_configs(yaml_file_path, component_name)
    
    def refresh_yaml_configs(self, yaml_file_path=None, component_name=None):
        self.yaml_file_path = yaml_file_path or self.yaml_file_path
        self.component_name = component_name or self.component_name
        self.yaml_configs = _read_yaml(self.yaml_file_path, self.component_name)
    
    @_check_type
    def get(self, key, default=None):
        return os.getenv(key) if key in os.environ else self.yaml_configs.get(key, default)

CONF_DIR = os.getenv('CONF_PATH', '/opt/seaqa/conf/')
yaml_file_path = os.path.join(CONF_DIR, os.environ.get('SEAQA_CONFIG_NAME', 'seaqa_config.yaml'))
configs = _ConfigParser(yaml_file_path, 'seaqa-events')
