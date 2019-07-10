import pymongo
from loguru import logger

from config import cons as ct


def updateOne(filter, col: str, doc, upsert=False):
    client = pymongo.MongoClient(ct.conf('MONGODB')['uri'])
    db = client.stock
    updateResult = db[col].update_one(filter, {"$set": doc}, upsert)
    client.close()
    return ((0 if updateResult.upserted_id is None else 1) +
            updateResult.modified_count)


# 查询个股52周最低价


def queryMinLowPrice(col: str, code: str, date: int) -> []:
    client = pymongo.MongoClient(ct.conf('MONGODB')['uri'])
    db = client.stock
    query = [{
        '$match': {'code': code, 'date': {'$gt': date}}
    },
        {
        '$group': {'_id': 'min', 'min_value': {'$min': '$low'}}
    }
    ]
    result = None
    docs = db[col].aggregate(query)
    for doc in docs:
        result = doc
    client.close()
    return result
