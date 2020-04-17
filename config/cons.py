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
# 110 113 sh
# 123 127 128 sz


def tecentUrl(symbol: str, diff_days: int) -> str:
    code = ''
    if symbol.startswith('6') or symbol.startswith('110') \
       or symbol.startswith('113'):
        code = f'sh{symbol}'
    else:
        code = f'sz{symbol}'
    ran = random.randrange(1000000, 100000000, 1)
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code}' + \
        f',day,,,{diff_days},qfq&r={ran}'
    return url


# 获取东方财富转债列表
def cbListUrl() -> str:
    ran = random.randrange(52093791, 58093791, 1)
    url = "http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?" + \
        "type=KZZ_LB2.0&token=70f12f2f4f091e459a279469fe49eca5&cmd=&st=" +\
        "STARTDATE&sr=-1&p=1&ps=2000&js=var%20YNohbZAK=" + \
        "{pages:(tp),data:(x),font:(font)}"+f'&rt={ran}'
    return url

# http://cn.morningstar.com/handler/quicktake.ashx?command=portfolio&fcid=F0000004AI&randomid=0.9657097972824025
# 晨星持仓url 包含10大股票和10大债券


def mstarPortfolio(fcid: str) -> str:
    ran = random.random()
    url = f'http://cn.morningstar.com/handler/quicktake.ashx?command=' +\
        f'portfolio&fcid={fcid}&randomid={ran}'
    return url

# http://cn.morningstar.com/handler/quicktake.ashx?command=rating&fcid=F0000004AI&randomid=0.9657097972824025
# 晨星评级


def mstarRating(fcid: str) -> str:
    ran = random.random()
    url = f'http://cn.morningstar.com/handler/quicktake.ashx?command=' +\
        f'rating&fcid={fcid}&randomid={ran}'
    return url

# http://cn.morningstar.com/handler/quicktake.ashx?command=performance&fcid=F0000004AI&randomid=0.9657097972824025
# 晨星业绩表现计算


def mstarPerformance(fcid: str) -> str:
    ran = random.random()
    url = f'http://cn.morningstar.com/handler/quicktake.ashx?command=' +\
        f'rating&fcid={fcid}&randomid={ran}'
    return url

# http://cn.morningstar.com/handler/quicktake.ashx?command=return&fcid=F0000004AI&randomid=0.9657097972824025
# 晨星业绩回报


def mstarReturn(fcid: str) -> str:
    ran = random.random()
    url = f'http://cn.morningstar.com/handler/quicktake.ashx?command=' +\
        f'return&fcid={fcid}&randomid={ran}'
    return url

# http://cn.morningstar.com/handler/quicktake.ashx?command=manage&fcid=F0000004AI&randomid=0.9657097972824025
# 晨星基金投资目标成立日基金经理


def mstarManage(fcid: str) -> str:
    ran = random.random()
    url = f'http://cn.morningstar.com/handler/quicktake.ashx?command=' +\
        f'manage&fcid={fcid}&randomid={ran}'
    return url

# http://cn.morningstar.com/handler/quicktake.ashx?command=fee&fcid=F0000004AI&randomid=0.9657097972824025
# 基金费用


def mstarFee(fcid: str) -> str:
    ran = random.random()
    url = f'http://cn.morningstar.com/handler/quicktake.ashx?command=' +\
        f'fee&fcid={fcid}&randomid={ran}'
    return url

# http://cn.morningstar.com/handler/quicktake.ashx?command=banchmark&fcid=F0000004AI&randomid=0.9657097972824025
# 基金种类和benchmark


def mstarBanchmark(fcid: str) -> str:
    ran = random.random()
    url = f'http://cn.morningstar.com/handler/quicktake.ashx?command=' +\
        f'banchmark&fcid={fcid}&randomid={ran}'
    return url


def mstarHeaders() -> object:
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/' +
        '537.36 (KHTML, like Gecko) Chrome/60.0.3100.0 Safari/537.36',
        "Accept": "text/html;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8",
        "Connection": "keep-alive"
    }
