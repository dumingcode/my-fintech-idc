import pymongo
from loguru import logger

from config import cons as ct


def updateOne(filter, doc, upsert=False):
    client = pymongo.MongoClient(ct.MONGODB['url'], ct.MONGODB['port'])
    db = client.stock
    updateResult = db.hisprice.update_one(filter, {"$set": doc}, upsert)
    client.close()
    return ((0 if updateResult.upserted_id is None else 1) +
            updateResult.modified_count)
