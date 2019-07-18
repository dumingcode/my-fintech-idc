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
