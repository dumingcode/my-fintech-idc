import tushare as ts
from config import cons as ct
def get_adj_price(param):
    ts.set_token(ct.TOKEN)
    pro = ts.pro_api()
    df = ts.pro_bar(ts_code=param['ts_code'], adj=param['adj'], start_date=param['start_date'], end_date=param['end_date'])
    return df