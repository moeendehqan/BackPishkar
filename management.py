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


def addinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        columns = data['insurer']
        columns['username'] = username
        find = pishkarDb['insurer'].find_one({'نام':columns['نام']})==None
        if find:
            pishkarDb['insurer'].insert_one(columns)
            return json.dumps({'replay':True,'msg':'ثبت شده'})
        else:
            return json.dumps({'replay':False,'msg':'بیمه گر با این نام قبلا ثبت شده'})
    else:
        return ErrorCookie()



def getinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        insurerList = [x['نام'] for x in (pishkarDb['insurer'].find({'username':username},{'_id':0,'نام':1}))]
        return json.dumps({'replay':True, 'list':insurerList})
    else:
        return ErrorCookie()
    
def delinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        print(data)
        pishkarDb['insurer'].delete_many({'username':username,'نام':data['name']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()