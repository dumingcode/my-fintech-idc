from stock.task import run_hs_given_stock_adj_price_task

# 校验针对个股获取历史数据


def test_given_stock_hisprice_task():
    assert run_hs_given_stock_adj_price_task('000001', 1) is False
    assert run_hs_given_stock_adj_price_task('000001.SZ', 1) is True
