import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

fee = pd.DataFrame(pishkarDb['Fees'].find({'comp':'کارآفرین 1'}))
for i in fee.index:
    print(i)
    pishkarDb['Fees'].update_one({'_id':fee['_id'][i]},{'$set':{'comp':'کارآفرین'}})
