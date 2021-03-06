import redis
from loguru import logger

from config import cons as ct


def redisHGet(key: str, field):
    connConfig = ct.conf('REDIS')
    conn = redis.Redis(host=connConfig['host'],
                       port=connConfig['port'],
                       password=connConfig['password'])
    val = conn.hget(connConfig['keyPrefix'] + key, field)
    return val


def redisHSet(key: str, field, value) -> bool:
    connConfig = ct.conf('REDIS')
    conn = redis.Redis(host=connConfig['host'],
                       port=connConfig['port'],
                       password=connConfig['password'])
    val = conn.hset(connConfig['keyPrefix'] + key, field, value)
    return val


def redisSet(key: str, value) -> bool:
    connConfig = ct.conf('REDIS')
    conn = redis.Redis(host=connConfig['host'],
                       port=connConfig['port'],
                       password=connConfig['password'])
    val = conn.set(connConfig['keyPrefix'] + key, value)
    return val


def redisGet(key: str):
    connConfig = ct.conf('REDIS')
    conn = redis.Redis(host=connConfig['host'],
                       port=connConfig['port'],
                       password=connConfig['password'])
    val = conn.get(connConfig['keyPrefix'] + key)
    return val
