import sys
from config import cons_prod as ct_prod
from config import cons_dev as ct_dev
from loguru import logger
import random


def conf(key):
    env = 'dev'
    try:
        if len(sys.argv) > 1 and sys.argv[1] == 'prod':
            env = 'prod'
        if env == 'prod':
            return ct_prod.CONFIG[key]
        elif env == 'dev':
            return ct_dev.CONFIG[key]
        else:
            logger.error(f'config env error:{env} ')
    except KeyError:
        logger.critical(f'config key:{key} is not found')
        sys.exit()

# 获取腾讯前复权url（内含分红送股数据）


def tecentUrl(symbol: str, diff_days: int) -> str:
    code = ''
    if symbol.startswith('6'):
        code = f'sh{symbol}'
    else:
        code = f'sz{symbol}'
    ran = random.randrange(1000000, 100000000, 1)
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,,,{diff_days},qfq&r={ran}'
    return url
