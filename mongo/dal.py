import pymongo

from config import cons as ct


def updateOne(filter, doc, upsert=False):
    client = pymongo.MongoClient(ct.MONGODB['url'], ct.MONGODB['port'])
    db = client.stock
    updateResult = db.hisprice.update_one(filter, doc, upsert)
    client.close()
    print(updateResult.modified_count)
    return updateResult.modified_count
