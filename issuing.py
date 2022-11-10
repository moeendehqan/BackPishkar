import json
from multiprocessing.reduction import duplicate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def addfile(cookier,file,comp):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.read_excel(file)
        msg = ''
        requiedCulomns = ['رشته','مورد بیمه','کد رایانه صدور بیمه نامه','پرداخت کننده حق بیمه',
            'شماره بيمه نامه','تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','تاریخ سررسید',
            'تاريخ سند دريافتي','وضعيت وصول','مبلغ کل حق بیمه','مبلغ تسویه شده','بدهی باقی مانده']
        for rc in requiedCulomns:
            inculomns = rc in df.columns
            if inculomns==False:
                return json.dumps({'replay':False,'msg':f'فایل فاقد ستون ضروری "{rc}" است'})
        df['comp'] = comp
        df['username'] = username

        beforDuplicatesLen = len(df)
        df = df.drop_duplicates(['کد رایانه صدور بیمه نامه','تاریخ سررسید'])
        AfterDuplicatesLen = len(df)

        if (beforDuplicatesLen!=AfterDuplicatesLen):
            msg = msg + f'{beforDuplicatesLen - AfterDuplicatesLen} رکورد تکراری بوده و حذف شده' +  '\n'
        dff = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'comp':comp},{'_id':0,'کد رایانه صدور بیمه نامه':1,'تاریخ سررسید':1}))
        if len(dff)>0:
            dff['act'] = 1
            df =df.set_index(['کد رایانه صدور بیمه نامه','تاریخ سررسید']).join(dff.set_index(['کد رایانه صدور بیمه نامه','تاریخ سررسید']))
            df = df.reset_index()
            duplicateLen = int(df['act'].sum())
            if duplicateLen>0:
                msg = msg + f'{duplicateLen} رکورد قبلا ثبت شده بود که بروز رسانی شد'
                dropList = df[df['act']==1][['کد رایانه صدور بیمه نامه','تاریخ سررسید']]
                for d in dropList.index:
                    print(dropList.loc[d])
                    pishkarDb['issuing'].delete_one({'username':username,'comp':comp,'کد رایانه صدور بیمه نامه':int(dropList['کد رایانه صدور بیمه نامه'][d]),'تاریخ سررسید':dropList['تاریخ سررسید'][d]})
            df = df.drop(columns=['act'])

        df = df.to_dict(orient='records')
        pishkarDb['issuing'].insert_many(df)
        msg = 'ثبت شد \n ' + msg
        return json.dumps({'replay':True,'msg':msg})
    else:
        return ErrorCookie()

def getdf(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0}))
        df = df[['کد رایانه صدور بیمه نامه','تاریخ سررسید','رشته','مورد بیمه','پرداخت کننده حق بیمه',
            'تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','وضعيت وصول','مبلغ کل حق بیمه','مبلغ تسویه شده',
            'بدهی باقی مانده','comp']]
        df = df.fillna('')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()