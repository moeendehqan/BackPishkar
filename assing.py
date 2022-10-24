import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def NCtName(cl,nc):
    if str(nc)!='nan':
        df = cl[cl['nationalCode']==nc]
        df['full'] = df['gender'] +' '+ df['fristName'] +' '+ df['lastName']
        df = df['full'][df.index.max()]
        return df
    else:
        return 'بدون مشاور'

def get(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dicFildGet = {'_id':0,'UploadDate':1,'comp':1,'بيمه گذار':1,'رشته':1,'مورد بیمه':1,'کل کارمزد محاسبه شده':1,'شماره بيمه نامه':1}
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},dicFildGet))
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'هیچ فایل کارمزدی موجود نیست'})
        assing = pd.DataFrame(pishkarDb['assing'].find({'username':username},{'username':0,'_id':0}))
        if len(assing)>0:
            cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
            df = df.set_index('شماره بيمه نامه').join(assing.set_index('شماره بيمه نامه'),how='left').reset_index()
            df['consultant' ] = [NCtName(cl_consultant,x) for x in df['consultant']]
        else:
            df['consultant'] = 'بدون مشاور'
        df['بيمه گذار'] = [x.split(' کد ')[0] for x in df['بيمه گذار']]
        print(data['showAll'])
        print(df)
        if data['showAll']==False:
            df = df[df['consultant']=='بدون مشاور']
        df = df.fillna('')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()

def getinsurnac(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = (pishkarDb['Fees'].find_one({'username':username, 'شماره بيمه نامه':data['code']},{'_id':0, 'رشته':1, 'مورد بیمه':1, 'بيمه گذار':1, 'تاریخ صدور بیمه نامه':1, 'comp':1, 'شماره بيمه نامه':1}))
        assing = pishkarDb['assing'].find_one({'username':username,'شماره بيمه نامه':data['code']},{'_id':0})
        print(assing)
        if assing==None:
            df['Consultant'] ={'name':'ندارد', 'code':0}
        else:
            cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
            df['Consultant'] = {'name':NCtName(cl_consultant,assing['consultant']),'code':assing['consultant']}
        df = {k:v if not (str(v)=='nan') else '-' for k,v in df.items() }
        return json.dumps({'replay':True, 'dic':df})
    else:
        return ErrorCookie()


def set(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        if str(data['consultant'])!='0':
            pishkarDb['assing'].insert_one({'username':username, 'شماره بيمه نامه':data['code'], 'consultant':data['consultant']})
        else:
            pishkarDb['assing'].delete_one({'username':username, 'شماره بيمه نامه':data['code']})
        return json.dumps({'o':'o'})
    else:
        return ErrorCookie()