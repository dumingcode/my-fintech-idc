from stock import task
from stock import basic
from stock import quant
from stock import price

# 校验针对个股获取历史数据


def test_given_stock_hisprice_task():
    assert task.run_his_given_stock_adj_price_task('399300.SZ', 1) is True

# history_data[history_data.ph_star==raw]['ph'].iloc[0]

# 测试返回的股票代码含有沪深300


def test_stock_list_hs300():
    df = basic.get_hs_stock_list({
        'list_status': 'L',
        'exchange': '',
        'fields': 'ts_code,symbol,list_date'
    })
    assert df[df.ts_code == '399300.SZ']['symbol'].iloc[0] == '399300'

# 校验52周最低价生成


def test_52week_low():
    assert quant.manage52WeekLowestPrice({'code': '000001'}) is False


# 测试转债列表返回应该大于0
def test_cb_list():
    arr = basic.get_hs_cb_list()
    assert len(arr) > 0

# 测试转债的价格数据获取


def test_cb_price():
    arr = price.get_tecent_price('113020', 5)
    assert len(arr) > 0
