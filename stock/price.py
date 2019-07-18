import tushare as ts
from config import cons as ct
import requests
import json
from loguru import logger


def get_adj_price(param):
    """
        获取复权行情
    Parameters
    ------
        Dict
        ts_code: str      ts股票代码
        start_date: str  开始日期 (格式：YYYYMMDD)
        end_date: str 结束日期 (格式：YYYYMMDD)
        adj: 复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None
        freq: 数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D
        ma: list 均线，支持任意周期的均价和均量，输入任意合理int数值
    Return
    -------
        DataFrame
            股票列表(DataFrame):
                ts_code       ts股票代码
                symbol        市场代码
                name          名称
                area          上市地区
                industry      行业
                list_date     上市日期
    """
    ts.set_token(ct.conf('TOKEN'))
    ts.pro_api()
    df = ts.pro_bar(ts_code=param['ts_code'],
                    adj=param['adj'],
                    start_date=param['start_date'],
                    asset=param['asset'],
                    end_date=param['end_date'])
    return df


def get_tecent_price(code: str, diff_days: int) -> []:
    """
        根据tecent获取价格数据
    Parameters
    ------
        Dict
        code: str      股票代码
        diff_days: int  n天前的历史数据
    Return
    -------
        array
            股票列表():
                _id 主键
                code 代码
                date 日期
                open 开盘价
                close 收盘价
                high  最高价
                low  最低价
                amount 成交量
    """
    url = ct.tecentUrl(code, diff_days)
    html = requests.get(url)
    if html.status_code != 200:
        logger.critical(
            f'{url} is error code :{html.status_code}')
        return []
    ret_jsons = json.loads(html.text)
    # 转债代码代码规则
    # 110 113 sh
    # 123 127 128 sz
    query_code = f'sh{code}' if code.startswith('6') or code.startswith(
        '110') or code.startswith('113') else f'sz{code}'
    stockArr = []
    try:
        temp = ret_jsons['data'][query_code]
        if 'qfqday' in temp:
            qfqdays = ret_jsons['data'][query_code]['qfqday']
        elif 'day' in temp:
            qfqdays = ret_jsons['data'][query_code]['day']
        else:
            return []
        for element in qfqdays:
            stockInfo = {}
            date = element[0]
            date = date.replace('-', '')
            stockInfo['_id'] = f'{code}-{date}'
            stockInfo['code'] = code
            stockInfo['date'] = int(date)
            stockInfo['open'] = float(element[1])
            stockInfo['close'] = float(element[2])
            stockInfo['high'] = float(element[3])
            stockInfo['low'] = float(element[4])
            stockInfo['amount'] = float(element[5])
            stockArr.append(stockInfo)
    except KeyError:
        logger.warning(f'{query_code} data invalid')
        return []
    return stockArr
