# -*- coding: utf-8 -*-
import argparse
import logging

from seaqa_events.app.app import App
from seaqa_events.app.log import LogConfigurator
from seaqa_events.app.config import get_config
from seaqa_events.app.event_redis import redis_cache
from seaqa_events.db import create_db_tables


def main():
    args = parser.parse_args()
    LogConfigurator(args.loglevel, args.logfile)

    config = get_config(args.config_file)

    redis_cache.init_redis(config)

    try:
        create_db_tables(config)
    except Exception as e:
        logging.error('Failed create or prepare tables, error: %s' % e)
        raise RuntimeError('Failed create or prepare tables, error: %s' % e)

    app = App(config)
    app.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', help='config file')
    parser.add_argument('--logfile', help='log file')
    parser.add_argument('--loglevel', default='info', help='log level')

    main()
