import datetime
import json
from loguru import logger

from db import redisDal
from mongo import dal
import numpy as np
import talib


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


def test_talib():
    close = numpy.random.random(100)
    output = talib.SMA(close)
    return output


def calc_ma(code: str, diff_days: int):
    """
    更新转债diff_days天内MA
    Parameters
    ------
    diff_days: 计算x天前的MA数据
    Return
    -------
    result 是否正常结束
    """
    try:
        _array = dal.queryMany({'code': code}, {'close': 1, '_id': 0},
                               diff_days, [('date', -1)], 'hisprice')
        array = list(_array)
        if len(array) == 0:
            return None
        narray = np.array([])
        for price in array:
            narray = np.append(narray, price['close'])
        ma = talib.MA(narray, timeperiod=diff_days, matype=0)
        return ma[-1]
    except Exception as exp:
        logger.error(exp)
        return None
    return None


def calc_atr(code: str, diff_days: int):
    """
    更新转债diff_days天内MA
    Parameters
    ------
    diff_days: 计算x天前的MA数据
    Return
    -------
    result 是否正常结束
    """
    try:
        _array = dal.queryMany({'code': code}, {'close': 1, 'high': 1, 'low':
                                                1, '_id': 0},
                               diff_days+1, [('date', -1)], 'hisprice')
        array = list(_array)
        if len(array) == 0:
            return None
        narray_high = np.array([])
        narray_low = np.array([])
        narray_close = np.array([])
        for price in array:
            narray_high = np.append(narray_high, price['high'])
            narray_low = np.append(narray_low, price['low'])
            narray_close = np.append(narray_close, price['close'])
        atr = talib.ATR(narray_high, narray_low, narray_close,
                        timeperiod=diff_days)
        return atr[-1]
    except Exception as exp:
        logger.error(exp)
        return None
    return None
