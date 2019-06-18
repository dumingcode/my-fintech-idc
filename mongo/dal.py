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
