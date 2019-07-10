CONFIG = {
    'ENV': 'dev',
    'TOKEN': 'xx',
    'MONGODB': {
        'uri': 'mongodb://127.0.0.1:27017/stock'
    },
    # 更新BACK_TRACK_DAYS天前的股票前复权数据
    'BACK_TRACK_DAYS': 600,
    'REDIS': {
        'port': 6379,
        'host': 'localhost',
        'keyPrefix': 'myfintech-'
    }
}
