# -*- coding: utf-8 -*-
import copy
import json
import logging
import time
import redis

logger = logging.getLogger(__name__)

REDIS_METRIC_KEY = 'metric'


class RedisClient(object):
    """Redis client for pub/sub messaging and caching."""

    def __init__(self, configs, socket_connect_timeout=30, socket_timeout=None):
        self._host = configs.get('REDIS_HOST', '127.0.0.1')
        self._port = configs.get('REDIS_PORT', 6379)
        self._password = configs.get('REDIS_PASSWORD')

        """
        By default, each Redis instance created will in turn create its own connection pool.
        Every caller using redis client will has it's own pool with configs caller passed.
        """
        self.connection = redis.Redis(
            host=self._host, port=self._port, password=self._password,
            socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout,
            decode_responses=True
        )

    def get_subscriber(self, channel_name):
        """Get a pub/sub subscriber for a channel."""
        while True:
            try:
                subscriber = self.connection.pubsub(ignore_subscribe_messages=True)
                subscriber.subscribe(channel_name)
            except redis.AuthenticationError as e:
                logger.critical('connect to redis auth error: %s', e)
                raise e
            except Exception as e:
                logger.error('redis pubsub failed. {} retry after 10s'.format(e))
                time.sleep(10)
            else:
                return subscriber

    def get(self, key):
        """Get value from Redis."""
        return self.connection.get(key)

    def set(self, key, value, timeout=None):
        """Set value in Redis with optional timeout."""
        if not timeout:
            return self.connection.set(key, value)
        else:
            return self.connection.setex(key, timeout, value)

    def delete(self, key):
        """Delete key from Redis."""
        return self.connection.delete(key)

    def publish(self, channel_name, message):
        """Publish message to a channel."""
        return self.connection.publish(channel_name, message)


class RedisCache(object):
    """Singleton Redis cache wrapper."""

    def __init__(self):
        self._redis_client = None

    def init_redis(self, configs):
        """Initialize Redis client."""
        self._redis_client = RedisClient(configs)

    def get(self, key):
        """Get value from cache."""
        return self._redis_client.get(key)

    def set(self, key, value, timeout=None):
        """Set value in cache with optional timeout."""
        return self._redis_client.set(key, value, timeout=timeout)

    def delete(self, key):
        """Delete key from cache."""
        return self._redis_client.delete(key)

    def create_or_update(self, key, value):
        """Create or update a key with merged values."""
        try:
            current_value = self._redis_client.get(key)
            if current_value:
                current_value_dict_copy = copy.deepcopy(json.loads(current_value))
                current_value_dict_copy.update(value)
                self._redis_client.set(key, json.dumps(current_value_dict_copy))
            else:
                self._redis_client.set(key, json.dumps(value))
        except Exception as e:
            logging.error(e)

    def publish(self, channel_name, message):
        """Publish message to a channel."""
        return self._redis_client.publish(channel_name, message)


# Global redis cache instance
redis_cache = RedisCache()
