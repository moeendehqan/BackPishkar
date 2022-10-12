import json
import pymongo
import crypto
import timedate
import ast
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
        pass
    else:
        return ErrorCookie()