import sys

from loguru import logger

from mongo.dal import updateOne
from stock.task import run_hs_stock_adj_price_task

if __name__ == "__main__":
    logger.add(sys.stderr, format='{time} {level} {message}',
               filter="idc", level="ERROR")
    logger.add(sys.stdout, colorize=True,
               format="<green>{time}</green> <level>{message}</level>",
               level="INFO")
    logger.add("idc_{time}.log",  rotation="1 day", level="INFO")
    run_hs_stock_adj_price_task(600)
    # updateOne()
