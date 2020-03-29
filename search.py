import sys

from loguru import logger

from config import cons as ct
from elasticsearch import init_es
from stock import task

if __name__ == "__main__":
    logger.add(sys.stderr, format='{time} {level} {message}',
               filter="es", level="ERROR")
    logger.add(sys.stdout, colorize=True,
               format="<green>{time}</green> <level>{message}</level>",
               level="INFO")
    logger.add("es_{time}.log",  rotation="1 day", level="INFO")
    env = ct.conf('ENV')
    logger.info(f'env is {env}')

    init_es.init_cbond_es()
    init_es.init_stock_es()
    task.run_his_cb_basic_ino_task()
