import pymongo

from config import cons as ct


def updateOne(filter, doc, upsert=False):
    client = pymongo.MongoClient(ct.MONGODB['url'], ct.MONGODB['port'])
    db = client.stock
    updateResult = db.hisprice.update_one(filter, {"$set": doc}, upsert)
    client.close()
    print(updateResult.upserted_id)
    return updateResult.upserted_id
