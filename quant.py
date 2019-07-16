import sys

from loguru import logger

from config import cons as ct
from stock import task

if __name__ == "__main__":
    logger.add(sys.stderr, format='{time} {level} {message}',
               filter="idc", level="ERROR")
    logger.add(sys.stdout, colorize=True,
               format="<green>{time}</green> <level>{message}</level>",
               level="INFO")
    logger.add("idc_{time}.log",  rotation="1 day", level="INFO")
    env = ct.conf('ENV')
    logger.info(f'env is {env}')

    # task two
    task.run_his_dividend_stock_price_task(2)
    # task three
    task.run_stock_52week_lowprice_task()
