import datetime
import logging
import pandas as pd
import logging
from stock.basic import get_hs_stock_list
from stock.price import get_adj_price
from mongo.dal import updateOne


# 自动任务：更新沪深股市全部上市股票past_diff_days天前的前复权数据
def run_hs_stock_adj_price_task(past_diff_days):
    """
        沪深股市股票列表
    Parameters
    ------
        past_diff_days: 查询x天前的股票复权数据
    Return
    -------
        result 是否正常结束
    """
    param = {
        'list_status': 'L',
        'exchange': '',
        'fields': 'ts_code,sumbol,list_date'
    }
    hs_df = get_hs_stock_list(param)
    hs_df_ = hs_df.head(10)
    delta = datetime.timedelta(days=past_diff_days)
    past_str = (datetime.date.today() - delta).strftime('%Y%m%d')
    today_str = datetime.date.today().strftime('%Y%m%d')
    for index, row in hs_df_.iterrows():
        logging.debug(index)
        adj_price_df = get_adj_price({
            'ts_code': row['ts_code'],
            'start_date': past_str,
            'end_date': today_str,
            'adj': 'qfq'
        })
        for index, row in adj_price_df.iterrows():
            ts_code = row['ts_code'].split('.')[0]
            trade_date = row['trade_date']
            _id = f'{ts_code}-{trade_date}'
            data = {
                '_id': _id,
                'code': ts_code,
                'date': int(row['trade_date']),
                'open': float('%.2f' % row['open']),
                'close': float('%.2f' % row['close']),
                'low': float('%.2f' % row['low']),
                'pre_close': float('%.2f' % row['pre_close']),
                'change': float('%.2f' % row['change']),
                'pct_chg': float('%.2f' % row['pct_chg']),
                'vol': float('%.2f' % row['vol']),
                'amout': float('%.3f' % row['amount'])
            }
            updateOne({'_id': _id}, data, True)
        # print(adj_price_df.head(10))
