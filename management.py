import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def profile(data):
    user = cookie(data)
    user = json.loads(user)
    if user['replay']:
        user = user['user']
        useNew = data['userNew']
        pishkarDb['username'].update_many({'phone':user['phone']},{'$set':useNew},upsert=False)
        return json.dumps({'replay':True,'msg':'تغییرات با موفقیت ثبت شد'})
    else:
        return ErrorCookie()


def cunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    if user['replay']:
        print(user)
        cheakNC = pishkarDb['cunsoltant'].find_one({'nationalCode':data['cunsoltant']['nationalCode']})
        cheakP = pishkarDb['cunsoltant'].find_one({'phone':data['cunsoltant']['phone']})
        if cheakNC ==None:
            if cheakP == None:
                data['cunsoltant']['username'] = user['user']['phone']
                pishkarDb['cunsoltant'].insert_one(data['cunsoltant'])
                return json.dumps({'replay':True})
            else:
                phone = data['cunsoltant']['phone']
                return json.dumps({'replay':False,'msg':f'شماره همراه {phone} قبلا ثبت شده است'})
        else:
            nationalCode = data['cunsoltant']['nationalCode']
            return json.dumps({'replay':False,'msg':f'کد ملی {nationalCode} قبلا ثبت شده است'})
    else:
        return ErrorCookie()

def getcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    if user['replay']:
        df = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':user['user']['phone']},{'_id':0}))
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()

def delcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    if user['replay']:
        pishkarDb['cunsoltant'].delete_many({'nationalCode':data['nationalCode']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()