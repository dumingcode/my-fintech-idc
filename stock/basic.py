import tushare as ts
from config import cons as ct

def get_hs_stock_list():
    ts.set_token(ct.TOKEN)
    pro = ts.pro_api()
    df = pro.stock_basic(list_status='L',exchange='',fields='ts_code,symbol,name,area,industry,list_date')
    return df