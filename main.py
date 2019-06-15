import logging

from stock.task import run_hs_stock_adj_price_task
from mongo.dal import updateOne

if __name__ == "__main__":
    logging.basicConfig(filename='idc.log', level=logging.INFO)
    run_hs_stock_adj_price_task(10)
    # updateOne()
