import datetime
import random
import time

import pandas as pd
from loguru import logger

from mongo import dal
from stock import basic
from stock import price
from stock import dividend_share
from stock import quant
from config import cons as ct
from elasticsearch import es

import numpy as np
import talib
from db import redisDal
import json

import tushare as ts

import time

# 自动任务：更新沪深股市全部上市股票past_diff_days天前的前复权数据


def run_his_stock_adj_price_task(past_diff_days, exchange=''):
    """
        更新沪深股市上市公司前复权股票历史价格
    Parameters
    ------
        past_diff_days: 查询x天前的股票复权数据
        exchange: 交易所 SSE上交所 SZSE深交所
    Return
    -------
        result 是否正常结束
    """
    param = {
        'list_status': 'L',
        'exchange': exchange,
        'fields': 'ts_code,symbol,list_date'
    }
    hs_df = basic.get_hs_stock_list(param)
    delta = datetime.timedelta(days=past_diff_days)
    past_str = (datetime.date.today() - delta).strftime('%Y%m%d')
    today_str = datetime.date.today().strftime('%Y%m%d')
    for index, row in hs_df.iterrows():
        ts_code = row['ts_code'].split('.')[0]
        logger.info(f'{ts_code} start fqprice job {today_str}')
        adj_price_df = price.get_adj_price({
            'ts_code': row['ts_code'],
            'start_date': past_str,
            'end_date': today_str,
            'adj': 'qfq',
            'freq': 'D',
            'asset': 'I' if str(row['ts_code']).startswith('399') else 'E'
        })
        if adj_price_df is None:
            continue
        for index, row in adj_price_df.iterrows():
            trade_date = row['trade_date']
            _id = f'{ts_code}-{trade_date}'
            data = {
                '_id': _id,
                'code': ts_code,
                'date': int(row['trade_date']),
                'open': float('%.2f' % row['open']),
                'close': float('%.2f' % row['close']),
                'high': float('%.2f' % row['high']),
                'low': float('%.2f' % row['low']),
                'pre_close': float('%.2f' % row['pre_close']),
                'change': float('%.2f' % row['change']),
                'pct_chg': float('%.2f' % row['pct_chg']),
                'vol': float('%.2f' % row['vol']),
                'amout': float('%.3f' % row['amount'])
            }
            res = dal.updateOne({'_id': _id}, 'hisprice', data, True)
        time.sleep(1)
    return True

# 自动任务：更新沪深股市指定上市股票past_diff_days天前的前复权数据


def run_his_given_stock_adj_price_task(ts_code, past_diff_days=600):
    """
        更新沪深股市指定上市股票past_diff_days天前的前复权数据
    Parameters
    ------
        past_diff_days: 查询x天前的股票复权数据
        ts_code: 000001.SZ
    Return
    -------
        result 是否正常结束
    """
    logger.info(f'{ts_code} run_his_given_stock_adj_price_task start')
    if ts_code.endswith('.SZ') or ts_code.endswith('.SH'):
        pass
    else:
        logger.critical(f'ts_code {ts_code} is invalid')
        return False

    delta = datetime.timedelta(days=past_diff_days)
    past_str = (datetime.date.today() - delta).strftime('%Y%m%d')
    today_str = datetime.date.today().strftime('%Y%m%d')
    adj_price_df = price.get_adj_price({
        'ts_code': ts_code,
        'start_date': past_str,
        'end_date': today_str,
        'adj': 'qfq',
        'freq': 'D',
        'asset': 'I' if ts_code.startswith('399') else 'E'
    })

    if adj_price_df is None:
        logger.warning(f'ts_code {ts_code} price data is None')
        return False
    ts_code = ts_code.split('.')[0]
    for index, row in adj_price_df.iterrows():
        trade_date = row['trade_date']
        _id = f'{ts_code}-{trade_date}'
        data = {
            '_id': _id,
            'code': ts_code,
            'date': int(row['trade_date']),
            'open': float('%.2f' % row['open']),
            'close': float('%.2f' % row['close']),
            'low': float('%.2f' % row['low']),
            'high': float('%.2f' % row['high']),
            'pre_close': float('%.2f' % row['pre_close']),
            'change': float('%.2f' % row['change']),
            'pct_chg': float('%.2f' % row['pct_chg']),
            'vol': float('%.2f' % row['vol']),
            'amout': float('%.3f' % row['amount'])
        }
        res = dal.updateOne({'_id': _id}, 'hisprice', data, True)
    logger.info(f'{ts_code} run_his_given_stock_adj_price_task end')
    return True


def run_his_dividend_stock_price_task(diff_days: int):
    """
    更新沪深股市diff_days天内存在分红除权的股票历史前复权价格
    Parameters
    ------
    diff_days: 查询x天前的股票复权数据
    Return
    -------
    result 是否正常结束
    """
    logger.info('********run_his_dividend_stock_price_task start******')
    try:
        divid_stocks = dividend_share.get_recent_dividend_share_stocks(
            diff_days)
        for stock in divid_stocks:
            run_his_given_stock_adj_price_task(stock)
            time.sleep(1)
    except Exception as exp:
        logger.critical(exp)
        return False
    logger.info('*********run_his_dividend_stock_price_task end********')
    return True
# 自动任务：更新沪深股市全部上市股票past_diff_days天前的前复权数据


def run_stock_52week_lowprice_task():
    """
        更新沪深股市上市公司52周最低价
    Parameters
    ------
    Return
    -------
        result 是否正常结束
    """
    param = {
        'list_status': 'L',
        'exchange': '',
        'fields': 'symbol'
    }
    hs_df = basic.get_hs_stock_list(param)
    for index, row in hs_df.iterrows():
        code = row['symbol']
        quant.manage52WeekLowestPrice({'code': code})
    return True


def run_his_cb_price_task(diff_days: int):
    """
    更新沪深股市diff_days天内转债价格数据
    Parameters
    ------
    diff_days: 查询x天前的转债复权数据
    Return
    -------
    result 是否正常结束
    """
    logger.info('********run_his_cb_price_task start******')
    try:
        cbs = basic.get_hs_cb_list()
        for cb in cbs:
            code = cb['BONDCODE']
            array = price.get_tecent_price(code, diff_days)
            if len(array) == 0:
                continue
            for obj in array:
                res = dal.updateOne(
                    {'_id': obj['_id']}, 'hisprice', obj, True)
            time.sleep(1)
            logger.info(f'fetch cbond {code} end')
    except Exception as exp:
        logger.error(exp)
        return False
    logger.info('*********run_his_cb_price_task end********')
    return True


def run_his_cb_quant_task(diff_days: int):
    """
    更新转债diff_days天内MA和ATR数据
    Parameters
    ------
    diff_days: 计算x天前数据
    Return
    -------
    result 是否正常结束
    """
    logger.info('********run_his_cb_quant_task start******')
    try:
        cbs = basic.get_hs_cb_list()
        for cb in cbs:
            code = cb['BONDCODE']
            ma = quant.calc_ma(code, diff_days)
            ma10 = quant.calc_ma(code, 10)
            ma5 = quant.calc_ma(code, 5)
            atr = quant.calc_atr(code, diff_days)
            redisData = redisDal.redisHGet('xueQiuStockSet', code)
            if redisData is None:
                redisObj = {'code': code}
            else:
                redisObj = json.loads(redisData)
            redisObj['ma20'] = round(ma, 2) if ma else ''
            redisObj['ma10'] = round(ma10, 2) if ma10 else ''
            redisObj['ma5'] = round(ma5, 2) if ma5 else ''
            redisObj['atr'] = round(atr, 2) if atr else ''
            redisObj['ma20GenDate'] = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S")
            redisDal.redisHSet('xueQiuStockSet', code,
                               json.dumps(redisObj))
            logger.info(f'atr ma {code} --{json.dumps(redisObj)}')
    except Exception as exp:
        logger.error(exp)
        return False
    logger.info('*********run_his_cb_quant_task end********')
    return True


def run_opt_stock_index_sample_task():
    """
    统计账户中自选股与各个指数的重合度
    Parameters
    ------
    Return
    -------
    result 是否正常结束
    """
    logger.info('********run_opt_stock_index_sample_task start******')
    quant.calc_user_stock_cover_index()
    logger.info('********run_opt_stock_index_sample_task end******')


def run_stock_ind_sample_task():
    """
        更新沪深股市上市公司国证行业指数分类数据
    Parameters
    ------
    Return
    -------
        result 是否正常结束
    """
    try:
        quant.calcIndustrySample()
    except Exception as exp:
        logger.error(exp)
    return True


def run_stock_alpha_beta_task():
    """
        更新沪深股市上市公司相对沪深300的alpha beta
    Parameters
    ------
    Return
    -------
        result 是否正常结束
    """
    param = {
        'list_status': 'L',
        'exchange': '',
        'fields': 'symbol'
    }
    hs_df = basic.get_hs_stock_list(param)
    for index, row in hs_df.iterrows():
        code = row['symbol']
        quant.calc_alpha_beta(code, 460)
    return True


def run_his_cb_basic_ino_task():
    """
    更新沪深股市转债基础信息代码表
    Parameters
    ------
    Return
    -------
    result 是否正常结束
    """
    logger.info('********run_his_cb_basic_ino_task start******')
    try:
        cbs = basic.get_hs_cb_list()
        for cb in cbs:
            code = cb['BONDCODE']
            res = dal.updateOne(
                {'_id': code}, 'cbBasicInfo', cb, True)
            es_res = es.insert_document('cbond', json.dumps({
                'stockname': cb['SECURITYSHORTNAME'],
                'stockcode': cb['SWAPSCODE'],
                'bondname': cb['SNAME'],
                'bondcode': cb['BONDCODE'],
            }), code)
            logger.info(f'fetch cbond es result {es_res} end')
            time.sleep(2)
            logger.info(f'fetch cbond basic info {code} end')
    except Exception as exp:
        logger.error(exp)
        return False
    logger.info('*********run_his_cb_basic_ino_task end********')
    return True


def run_stock_insert_es_task():
    """
        构建沪深上市公司代码和名称的es存储
    Parameters
    ------
    Return
    -------
        result 是否正常结束
    """
    param = {
        'list_status': 'L',
        'exchange': '',
        'fields': 'symbol'
    }
    hs_df = basic.get_hs_stock_list(param)
    for index, row in hs_df.iterrows():
        code = row['symbol']
        name = row['name']
        es_res = es.insert_document('stock', json.dumps({
            'stockcode': row['symbol'],
            'stockname': row['name']
        }), code)
        logger.info(f'fetch stock es result {es_res} end')
    return True


def run_maintain_tushare_token_life_task():
    """
        调用tushare token以保证小号一直在调用
    Parameters
    ------
    Return
    -------
        result 是否正常结束
    """
    try:
        pro = ts.pro_api(ct.tushareBackUpToken())
        df = pro.tmt_twincome(item='8')
        logger.info(f'tmt_twincomedetail {len(df)} end')
        time.sleep(2)
        df = pro.tmt_twincome(item='1')
        logger.info(f'tmt_pc {len(df)} end')
        time.sleep(2)
        df = pro.tmt_twincome(item='18')
        logger.info(f'tmt_industry_pc {len(df)} end')
        time.sleep(2)
        df = pro.tmt_twincome(item='28')
        logger.info(f'tmt_lcd {len(df)} end')
    except Exception as exp:
        logger.error(exp)
    return True


def run_stock_day_week_month_ma_task():
    """
        构建沪深上市公司代码和名称的es存储
    Parameters
    ------
    Return
    -------
        result 是否正常结束
    """
    param = {
        'list_status': 'L',
        'exchange': '',
        'fields': 'symbol'
    }
    hs_df = basic.get_hs_stock_list(param)
    for index, row in hs_df.iterrows():
        try:
            code = row['symbol']
            name = row['name']
            today = datetime.datetime.now().strftime("%Y%m%d")
            two_year = int(time.strftime(
                '%Y', time.localtime(time.time()))) - 2
            five_year = int(time.strftime(
                '%Y', time.localtime(time.time()))) - 5
            eight_year = int(time.strftime(
                '%Y', time.localtime(time.time()))) - 11
            month_day = time.strftime('%m%d', time.localtime(time.time()))
            two_year_ago = '{}{}'.format(two_year, month_day)
            five_year_ago = '{}{}'.format(five_year, month_day)
            eight_year_ago = '{}{}'.format(eight_year, month_day)

            param_day = {
                'ts_code': row['ts_code'],
                'start_date': two_year_ago,
                'end_date': today,
                'adj': 'qfq',
                'freq': 'D',
                'asset': 'E',
                'ma': [20, 30, 60, 90, 120, 250]
            }
            day_df = price.get_adj_ma_price(param_day)
            ma_data = day_df.fillna('').iloc[0]
            ma_day = {
                'trade_date': ma_data['trade_date'],
                'ma20': ma_data['ma20'],
                'ma30': ma_data['ma30'],
                'ma60': ma_data['ma60'],
                'ma90': ma_data['ma90'],
                'ma120': ma_data['ma120'],
                'ma250': ma_data['ma250']
            }

            param_week = {
                'ts_code': row['ts_code'],
                'start_date': five_year_ago,
                'end_date': today,
                'adj': 'qfq',
                'freq': 'W',
                'asset': 'E',
                'ma': [20, 30, 60, 90, 120, 250]
            }
            week_df = price.get_adj_ma_price(param_week)
            ma_week_data = week_df.fillna('').iloc[0]
            ma_week = {
                'trade_date': ma_week_data['trade_date'],
                'ma20': ma_week_data['ma20'],
                'ma30': ma_week_data['ma30'],
                'ma60': ma_week_data['ma60'],
                'ma90': ma_week_data['ma90'],
                'ma120': ma_week_data['ma120'],
                'ma250': ma_week_data['ma250']
            }

            param_month = {
                'ts_code': row['ts_code'],
                'start_date': eight_year_ago,
                'end_date': today,
                'adj': 'qfq',
                'freq': 'M',
                'asset': 'E',
                'ma': [20, 30, 60, 90, 120]
            }
            month_df = price.get_adj_ma_price(param_month)
            ma_month_data = month_df.fillna('').iloc[0]
            ma_month = {
                'trade_date': ma_month_data['trade_date'],
                'ma20': ma_month_data['ma20'],
                'ma30': ma_month_data['ma30'],
                'ma60': ma_month_data['ma60'],
                'ma90': ma_month_data['ma90'],
                'ma120': ma_month_data['ma120']
            }
            redisObj = {'code': code, 'ma_day': ma_day,
                        'ma_week': ma_week, 'ma_month': ma_month}
            # save
            redisRes = redisDal.redisSet(f'ma:{code}', json.dumps(redisObj))
            logger.info(f'{code} ma result:' + str(redisRes))
        except Exception as exp:
            logger.error(exp)
    return True
