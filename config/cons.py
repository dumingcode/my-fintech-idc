import sys
from config import cons_prod as ct_prod
from config import cons_dev as ct_dev
from loguru import logger


def conf(key):
    env = 'prod'
    try:
        if len(sys.argv) > 1 and sys.argv[1] == 'dev':
            env = 'dev'
        if env == 'prod':
            return ct_prod.CONFIG[key]
        elif env == 'dev':
            return ct_dev.CONFIG[key]
        else:
            logger.error(f'config env error:{env} ')
    except KeyError:
        logger.critical(f'config key:{key} is not found')
        sys.exit()
