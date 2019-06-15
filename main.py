import sys

from loguru import logger

from config import cons as ct
from mongo.dal import updateOne
from stock.task import run_hs_stock_adj_price_task

# 更新股票历史前复权数据任务


def his_fq_stock_price_task():
    # 默认回溯天数
    logger.info(f'*******his_fq_stock_price_task start*******')
    back_track_datys = ct.conf('BACK_TRACK_DAYS')
    if len(sys.argv) > 2 and sys.argv[2].isdigit():
        back_track_datys = int(sys.argv[2])
    logger.info(f'fetch stock price data {back_track_datys} days ago')
    run_hs_stock_adj_price_task(back_track_datys)
    logger.info(f'*******his_fq_stock_price_task end*******')


if __name__ == "__main__":
    logger.add(sys.stderr, format='{time} {level} {message}',
               filter="idc", level="ERROR")
    logger.add(sys.stdout, colorize=True,
               format="<green>{time}</green> <level>{message}</level>",
               level="INFO")
    logger.add("idc_{time}.log",  rotation="1 day", level="INFO")
    env = ct.conf('ENV')
    logger.info(f'env is {env}')
    # task one
    his_fq_stock_price_task()
