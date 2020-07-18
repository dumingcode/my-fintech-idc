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
               filter="fund", level="ERROR")
    logger.add(sys.stdout, colorize=True,
               format="<green>{time}</green> <level>{message}</level>",
               level="INFO")
    logger.add("fund_{time}.log",  rotation="1 day", level="INFO")
    env = ct.conf('ENV')
    logger.info(f'env is {env}')

    # 每月跑一次公募基金抓取任务
    msScrawl = scrawl.MstarScrawl()
    msScrawl.scrawlFundTask()
    # msScrawl.restoreRedisDataFromMongo()
