import tushare as ts

from config import cons as ct


def get_hs_stock_list(param):
    """
        沪深股市股票列表
    Parameters
    ------
        Dict
        is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
        list_status: 上市状态： L上市 D退市 P暂停上市
        exchange: 交易所 SSE上交所 SZSE深交所 HKEX港交所(未上线)
        fields: ts_code,symbol,name,area,industry,list_date
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
    pro = ts.pro_api()
    df = pro.stock_basic(
        list_status=param['list_status'], exchange=param['exchange'],
        filelds=param['fields'])
    return df
