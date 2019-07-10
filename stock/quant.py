import datetime
import json
from loguru import logger

from db import redisDal
from mongo import dal


def manage52WeekLowestPrice(param):
    """
        获取并保存个股52周最低价
    Parameters
    ------
        Dict
        code: str      股票代码 600030
    Return
    -------
        True 成功
        False 失败
    """
    try:
        code = param['code']
        delta = datetime.timedelta(weeks=52)
        past = int((datetime.date.today() - delta).strftime('%Y%m%d'))
        lowestPrice = dal.queryMinLowPrice('hisprice', code, past)
        if lowestPrice is None:
            logger.info(f'52 week low {code} --not found')
            return False

        redisData = redisDal.redisHGet('xueQiuStockSet', code)
        if redisData is None:
            redisObj = {'code': code, 'low': lowestPrice['min_value']}
        else:
            redisObj = json.loads(redisData)
            redisObj['low'] = lowestPrice['min_value']
        redisObj['lowGenDate'] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")
        if lowestPrice['min_value'] > 0:
            redisDal.redisHSet('xueQiuStockSet', code, json.dumps(redisObj))
            logger.info(f'52 week low {code} --{json.dumps(redisObj)}')
    except Exception as err:
        logger.critical(err)
        return False
    return True
