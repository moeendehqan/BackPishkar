import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def get(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dicFildGet = {'_id':0,'UploadDate':1,'comp':1,'بيمه گذار':1,'بيمه گذار':1,'رشته':1,'مورد بیمه':1,'کل کارمزد محاسبه شده':1,'شماره بيمه نامه':1}
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},dicFildGet))
        assing = pd.DataFrame(pishkarDb['assing'].find({'username':username}))
        if len(assing)>0:
            df = df.set_index('شماره بيمه نامه').join(assing.set_index('شماره بيمه نامه'),how='left')
        else:
            df['مشاور'] = ''

        df['بيمه گذار'] = [x.split(' کد ')[0] for x in df['بيمه گذار']]
        print(df.isnull().sum())
        df = df.fillna('')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()
