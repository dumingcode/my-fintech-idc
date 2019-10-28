import sys
import time

from loguru import logger

from config import cons as ct
from stock import task
from stock import basic
from stock import price
from stock import quant
# 更新股票历史前复权数据任务


def his_fq_stock_price_task():
    logger.info(f'*******his_fq_stock_price_task start*******')
    back_track_datys = ct.conf('BACK_TRACK_DAYS')
    exchange = ''
    if len(sys.argv) > 2 and sys.argv[2].isdigit():
        back_track_datys = int(sys.argv[2])
    # 获取交易市场
    if len(sys.argv) > 3 and (sys.argv[3] == 'SSE' or sys.argv[3] == 'SZSE'):
        exchange = sys.argv[3]
    # 获取个股代码
    if len(sys.argv) > 4:
        given_stock = sys.argv[4]
        task.run_his_given_stock_adj_price_task(given_stock, back_track_datys)
        logger.info(
            f'*******given stock****{given_stock}**{back_track_datys}*')
        return
    logger.info(
        f'fetch stock price data {back_track_datys} days ago market is \
            {exchange} ')
    task.run_his_stock_adj_price_task(back_track_datys, exchange)
    logger.info(f'*******his_fq_stock_price_task end*******')


if __name__ == "__main__":
    logger.add(sys.stderr, format='{time} {level} {message}',
               filter="idc", level="ERROR")
    logger.add(sys.stdout, colorize=True,
               format="<green>{time}</green> <level>{message}</level>",
               level="INFO")
    env = ct.conf('ENV')
    logger.info(f'env is {env}')
    if env == 'prod':
        logger.add("idc_{time}.log",  rotation="1 day", level="INFO")

    # task one
    his_fq_stock_price_task()
    if len(sys.argv) > 4:
        exit()
    # task two
    task.run_his_dividend_stock_price_task(2)
    # task three
    task.run_stock_52week_lowprice_task()
    # task four
    task.run_his_cb_price_task(30)
    # task five
    task.run_his_cb_quant_task(20)
    # taks six 统计自选股中占各个指数的数量
    task.run_opt_stock_index_sample_task()
    # task seven 统计个股国证二级三级分类 每周6跑一次
    curWeek = int(time.strftime("%w"))
    if curWeek == 6:
        task.run_stock_ind_sample_task()
