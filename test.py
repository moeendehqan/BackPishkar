import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

fee = pd.DataFrame(pishkarDb['issuing'].find())
fee = fee.fillna(0)
fee = fee[fee['شماره الحاقیه']>0]
print(fee)
for i in fee.index:
    pishkarDb['issuing'].delete_one({'_id':fee['_id'][i]})


pishkarDb['issuing'].update_many({},{'$set':{'additional':'اصلی'}})
fee = pd.DataFrame(pishkarDb['issuing'].find())
print(fee)