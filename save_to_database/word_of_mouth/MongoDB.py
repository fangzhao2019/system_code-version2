# Requires pymongo 3.6.0+
from pymongo import MongoClient

client = MongoClient("mongodb://47.92.211.251:30000/")
database = client["new_carDataset"]
collection = database["ruihu8"]
BATCH_SIZE=100


def getAll():
    query = {}
    projection = {}
    projection["car"] = 1.0
    projection["formatOpinionSet"] = 1.0
    cursor = collection.find(query, projection=projection, limit=20,batch_size=BATCH_SIZE)
    return cursor


def getCommentByCarName(str):
    query = {}
    projection = {}
    query["car"] = str
    projection["car"] = 1.0
    projection["formatOpinionSet"] = 1.0
    cursor = collection.find(query, projection=projection,batch_size=BATCH_SIZE)
    return cursor


def getRegionInfo(str):
    query = {}
    projection = {}
    query["car"] = str
    projection["car"] = 1.0
    projection["city"] = 1.0
    projection["province"] = 1.0
    projection["formatOpinionSet.优点"] = 1.0
    projection["formatOpinionSet.缺点"] = 1.0
    cursor = collection.find(query, projection=projection,batch_size=BATCH_SIZE)
    return cursor


def getMapOpinion(str):
    query = {}
    projection = {}
    query["car"] = str
    projection["car"] = 1.0
    projection["city"] = 1.0
    projection["province"] = 1.0
    projection["formatOpinionSet.优点"] = 1.0
    projection["formatOpinionSet.缺点"] = 1.0
    cursor = collection.find(query, projection=projection,batch_size=BATCH_SIZE)
    return cursor


def getPrice(str):
    query = {}
    projection = {}
    query["car"] = str
    projection["car"] = 1.0
    projection["car_specific"] = 1.0
    projection["province"] = 1.0
    projection["price"] = 1.0
    projection["time"] = 1.0
    cursor = collection.find(query, projection=projection,batch_size=BATCH_SIZE)
    return cursor


def dbClose():
    client.close()
