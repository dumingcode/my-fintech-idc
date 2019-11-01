import datetime
import json
import random
import time

import requests
from loguru import logger

from config import cons as ct
from stock.basic import get_hs_stock_list


def get_recent_dividend_share_stocks(diff_days: int = 5) -> []:
    """
        获取最近进行分红除权的股票代码列表
    Parameters
    ------
        diff_days: int      x天内是否个股进行过分红除权
    Return
    -------
        array
            ts_code       ts股票代码
    """
    param = {
        'list_status': 'L',
        'exchange': '',
        'fields': 'ts_code,sumbol'
    }
    hs_df = get_hs_stock_list(param)
    ret_jsons = None
    stock_list = []
    for index, row in hs_df.iterrows():
        code = row['symbol']
        try:
            is_dividend = is_stock_recent_dividend(code, diff_days)
            if is_dividend:
                stock_list.append(row['ts_code'])
            time.sleep(1)
        except Exception as err:
            logger.critical(err)
            continue
    logger.info('dividend stock list:')
    logger.info(stock_list)
    return stock_list


def is_stock_recent_dividend(code: str, diff_days: int) -> bool:
    dividend_url = ct.tecentUrl(code, diff_days)
    html = requests.get(dividend_url)
    if html.status_code != 200:
        logger.critical(
            f'{dividend_url} is error code :{html.status_code}')
        return False
    ret_jsons = json.loads(html.text)
    query_code = f'sh{code}' if code.startswith('6') else f'sz{code}'
    try:
        if code.startswith('68'):
            qfqdays = ret_jsons['data'][query_code]['day']
        else:
            qfqdays = ret_jsons['data'][query_code]['qfqday']
    except KeyError:
        logger.critical(f'{query_code} data invalid')
        return
    for qfqday in qfqdays:
        if len(qfqday) > 6:
            return True
    return False
