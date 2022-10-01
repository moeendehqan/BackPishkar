import json
import pymongo
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def login(data):
    if pishkarDb['username'].find({'phone':data['phone']})==None:
        return json.dumps({'replay':False})
    else:
        return json.dumps({'replay':True})
