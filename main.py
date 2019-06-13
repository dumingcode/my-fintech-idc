from stock.price import get_adj_price
from stock.basic import get_hs_stock_list

if __name__ == "__main__":
    df = get_hs_stock_list()
    print(df.head())