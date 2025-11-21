# -*- coding: utf-8 -*-
import configparser
import logging
import os
import sys

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
    SEADB_SERVER_URL = getattr(seaqa_settings, 'SEADB_SERVER_URL', '')

except Exception as e:
    logger.critical("Can not import seaqa_web settings: %s." % e)
    raise RuntimeError("Can not import seaqa_web settings: %s" % e)


def get_config(config_file):
    config = configparser.ConfigParser()

    if config_file:
        try:
            config.read(config_file)
        except Exception as e:
            logger.critical("Failed to read config file %s: %s" % (config_file, e))
            raise RuntimeError("Failed to read config file %s: %s" % (config_file, e))
    else:
        logger.info("No config file specified. ")

    return config
