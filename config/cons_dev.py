CONFIG = {
    'ENV': 'dev',
    'TOKEN': '6e2c0b43db77b8f42ba85a11a9adbfc65e0a5b5cd92b563eb44a7923',
    'MONGODB': {
        'uri': 'mongodb://127.0.0.1:27017/stock'
    },
    # 更新BACK_TRACK_DAYS天前的股票前复权数据
    'BACK_TRACK_DAYS': 600,
    'REDIS': {
        'port': 6379,
        'host': 'localhost',
        'keyPrefix': 'myfintech-',
        'password': None
    },
    'ES': {
        'url': 'http://127.0.0.1:9200/'
    },
    'CB_URL': 'https://www.jisilu.cn/data/cbnew/#cb'
}
