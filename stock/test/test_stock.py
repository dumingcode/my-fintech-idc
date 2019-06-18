from stock import task

# 校验针对个股获取历史数据


def test_given_stock_hisprice_task():
    assert task.run_his_given_stock_adj_price_task('000001', 1) is False
    assert task.run_his_given_stock_adj_price_task('000001.SZ', 1) is True

# 更新分红除权股票前复权价格


# def test_stock_dividend_share():
#     assert task.run_his_dividend_stock_price_task(3) is True
