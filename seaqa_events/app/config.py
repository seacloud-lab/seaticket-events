# -*- coding: utf-8 -*-
import logging
import os
import json
import yaml

logger = logging.getLogger(__name__)

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
        if key in os.environ:
            value = os.getenv(key)
            try:
                value = json.loads(value)
            except:
                pass
            return value
        return self.yaml_configs.get(key, default)
    
def get_llm_price(models):
    if not models or not isinstance(models, list):
        return {}

    result = {}
    for model in models:
        if not isinstance(model, dict) or model.get('disable', False):
            continue
        if model.get('type') in ('proxy', 'other', 'hosted_vllm'):
            required_fields = ('model', 'url', 'price')
        else:
            required_fields = ('model', 'key', 'price')
        if not all(field in model for field in required_fields):
            continue
        if not isinstance(model['price'], dict):
            if not isinstance(model['price'], (int, float)):
                continue
            model['price'] = {
                'input': model['price'],
                'output': 0
            }
        elif 'input' not in model['price']:
            continue
        elif 'output' not in model['price']:
            model['price']['output'] = 0 # compatible for embedding models
        result[model['model']] = model['price']
    return result

CONF_DIR = os.getenv('CONF_PATH', '/opt/seaqa/conf/')
yaml_file_path = os.path.join(CONF_DIR, os.environ.get('SEAQA_CONFIG_NAME', 'seaqa_config.yaml'))
configs = _ConfigParser(yaml_file_path, 'seaqa-events')
AI_PRICES = get_llm_price(configs.get('LLM_MODELS', []) + [configs.get('EMBEDDING_MODEL', {})])
JWT_PRIVATE_KEY = configs.get('JWT_PRIVATE_KEY')
SEADB_SERVER_URL = configs.get('SEADB_SERVER_URL', 'http://seadb:8888')

SEAQA_WEB_DIR = os.getenv('SEAQA_WEB_DIR', '/data/dev/seaqa-web')
