import datetime
import json
from loguru import logger

from db import redisDal
from mongo import dal
import numpy as np
import talib

from stock import basic
from sklearn import linear_model
import pandas as pd


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
        if np.isnan(ma[-1]):
            return None
        else:
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
        if np.isnan(atr[-1]):
            return None
        else:
            return atr[-1]
    except Exception as exp:
        logger.error(exp)
        return None
    return None


def calc_user_stock_cover_index():
    """
    统计更新用户的持仓，计算持仓跟各大指数的重合度
    Parameters
    ------
    Return
    -------
    result 是否正常结束
    """
    try:
        # 用户持仓个股增加股票名称
        param = {
            'list_status': 'L',
            'exchange': '',
            'fields': 'symbol,name'
        }
        hs_df = basic.get_hs_stock_list(param)
        globalStock = {}
        for index, row in hs_df.iterrows():
            globalStock[row['symbol']] = row['name']
        _array = dal.queryMany(None, {'_id': 1, 'samples': 1},
                               0, None, 'indexSample')
        array = list(_array)
        if len(array) == 0:
            return None
        _opt_stock_array = dal.queryMany(None, {'_id': 1, 'stock': 1},
                                         0, None, 'optStock')
        opt_stock_array = list(_opt_stock_array)
        if len(opt_stock_array) == 0:
            return None

        for opt_stock in opt_stock_array:
            user_id = opt_stock['_id']
            user_stock = opt_stock['stock']
            user_quant = {}
            user_quant['user'] = user_id
            for index_sample in array:
                index_name = index_sample['_id']
                user_quant['index'] = index_name
                user_quant['_id'] = f'{user_id}_{index_name}'
                calc_opt_stock_in_index_sample(
                    user_stock, index_sample['samples'], user_quant, globalStock)
                dal.updateOne(
                    {'_id': user_quant['_id']}, 'optQuant', user_quant, True)
                logger.info(user_quant)
    except Exception as exp:
        logger.error(exp)
        return None
    return None


def calc_opt_stock_in_index_sample(opt_stock_str: str, index_sample: list,
                                   user_quant: dict, globalStock: dict):
    """
    具体计算用户的自选股和指数的重合数量和具体重合标的
    """
    opt_stocks = opt_stock_str.split(',')
    hit_count = 0
    opt_stock_index_hit = ''
    for opt_stock in opt_stocks:
        try:
            if index_sample.index(opt_stock) >= 0:
                name = globalStock[opt_stock]
                opt_stock_index_hit += f'{opt_stock}{name}'

                opt_stock_index_hit += ','
                hit_count += 1
        except ValueError as exp:
            pass
    if hit_count >= 1:
        opt_stock_index_hit = opt_stock_index_hit[0: len(
            opt_stock_index_hit)-1]
    user_quant['num'] = hit_count
    user_quant['hit_stocks'] = opt_stock_index_hit
    user_quant['date'] = datetime.datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S")


def calcIndustrySample():
    """
        更新个股的国证二级、三级行业分类
    Parameters
    ------
        stock_code 股票代码

    Return
    -------
        True 成功
        False 失败
    """
    try:
        indSamples_ = dal.queryMany(None, {'_id': 1, 'samples': 1},
                                    0, None, 'industrySample')
        indSamples = list(indSamples_)
        for indSample in indSamples:
            for stock_code in indSample['samples']:
                redisData = redisDal.redisHGet('xueQiuStockSet', stock_code)
                if redisData is None:
                    redisObj = {'code': stock_code}
                else:
                    redisObj = json.loads(redisData)
                ind = indSample['_id']
                if len(ind) == 5:
                    redisObj['gz2'] = ind
                elif len(ind) == 7:
                    redisObj['gz3'] = ind
                redisDal.redisHSet(
                    'xueQiuStockSet', stock_code, json.dumps(redisObj))
                logger.info(f'calcIndustrySample {stock_code} --{ind}')
    except Exception as err:
        logger.critical(err)
        return False
    return True


def query_stock_days_price_chg(code: str, diff_days: int):
    """
    查询diff_days天内个股的价格波动率
    Parameters
    ------
    diff_days: 计算x天前
    code: 个股代码 600030
    Return
    ------- Data Frame
    ------- index date
    ------- pct
    result dataFrame
    """
    try:
        _array = dal.queryMany({'code': code}, {
            'pct_chg': 1, 'date': 1, '_id': 0}, diff_days+1,
            [('date', -1)], 'hisprice')
        array = list(_array)
        if len(array) == 0:
            return None
        narray_pct = np.array([])
        narray_date = np.array([])
        narray_date.dtype = 'int32'
        for price in array:
            narray_pct = np.append(narray_pct, price['pct_chg'])
            narray_date = np.append(narray_date, price['date'])
        df = pd.DataFrame(narray_pct, index=narray_date, columns=['pct'])
        return df
    except Exception as exp:
        logger.error(exp)
        return None


def calc_alpha_beta(code: str, diff_days: int):
    """
    计算diff_days天内alpha、beta
    Parameters
    ------
    diff_days: 计算x天前的alpha beta数据
    Return
    -------
    result 是否正常结束
    """
    try:
        benchmark_pd = query_stock_days_price_chg('399300', diff_days)
        stock_pd = query_stock_days_price_chg(code, diff_days)
        if stock_pd is None:
            return None
        innerPd = stock_pd.join(benchmark_pd, how='inner',
                                lsuffix='stock', rsuffix='benchmark')
        innerPd = innerPd.dropna()
        reg = linear_model.LinearRegression()
        reg.fit(innerPd[['pctbenchmark']].values,
                innerPd['pctstock'].values)
        # 计算2年数据
        beta2 = reg.coef_[0]
        alpha2 = reg.intercept_ / 100
        RR2 = reg.score(innerPd[['pctbenchmark']].values,
                        innerPd['pctstock'].values)

        past = int((datetime.date.today() -
                    datetime.timedelta(diff_days / 2)).strftime('%Y%m%d'))
        innerPd = innerPd[innerPd.index > past]
        reg.fit(innerPd[['pctbenchmark']].values,
                innerPd['pctstock'].values)
        # 计算1年数据
        beta1 = reg.coef_[0]
        alpha1 = reg.intercept_ / 100
        RR1 = reg.score(innerPd[['pctbenchmark']].values,
                        innerPd['pctstock'].values)
        redisData = redisDal.redisHGet('xueQiuStockSet', code)
        logger.info(
            f'{code} alpha1 {alpha1} beta1 {beta1} r1 {RR1} alpha2 {alpha2} \
            beta2 {beta2} r1 {RR2}')
        if redisData is not None:
            redisObj = json.loads(redisData)
            redisObj['alphaBeta2Year'] = json.dumps({
                'alpha': alpha2, 'beta': beta2, 'r2': RR2})
            redisObj['alphaBeta1Year'] = json.dumps({
                'alpha': alpha1, 'beta': beta1, 'r2': RR1})
            redisObj['alphaBetaGenDate'] = datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')
            redisDal.redisHSet('xueQiuStockSet', code, json.dumps(redisObj))
    except Exception as exp:
        logger.error(exp)
        return None
    return None
