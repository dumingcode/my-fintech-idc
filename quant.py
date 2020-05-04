import sys
from loguru import logger
from config import cons as ct
from stock import task
from house import house_task
from stock import quant
from fund import scrawl
from fund import fundTarget

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
    # task.run_his_dividend_stock_price_task(2)
    # task three
    # task.run_stock_52week_lowprice_task()
    # task four
    # house_task.run_house_extract_localhis_data_task()
    # task.run_opt_stock_index_sample_task()

    # fundTarget.createFundTarget()
    scrawl.initEsIndexMapping()
    msScrawl = scrawl.MstarScrawl()
    msScrawl.scrawlFundTask()
