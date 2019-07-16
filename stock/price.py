import tushare as ts
from config import cons as ct


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


def get_tecent_price(code: str, diff_days: int) -> bool:
    dividend_url = ct.tecentUrl(code, diff_days)
    html = requests.get(dividend_url)
    if html.status_code != 200:
        logger.critical(
            f'{dividend_url} is error code :{html.status_code}')
        return False
    ret_jsons = json.loads(html.text)
    # 110 113 sh
    # 123 127 128 sz
    query_code = f'sh{code}' if code.startswith('6') or code.startswith(
        '110') or code.startswith('113') else f'sz{code}'
    try:
        qfqdays = ret_jsons['data'][query_code]['qfqday']
    except KeyError:
        logger.critical(f'{query_code} data invalid')
        return
    for qfqday in qfqdays:
        if len(qfqday) > 6:
            return True
    return False
