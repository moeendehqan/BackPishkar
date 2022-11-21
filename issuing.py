import json
import timedate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
from assing import NCtName
import timedate
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
        if len(df)==0:
            return json.dumps({'replay':False})
        df = df[['تاریخ عملیات','مبلغ کل حق بیمه','comp']]
        df = df.fillna('')
        df['دوره عملیات'] = [timedate.dateToPriod(x) for x in df['تاریخ عملیات']]
        df['تعداد'] = 1
        df['تاریخ عملیات عددی'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        dff = df.groupby(by=['دوره عملیات','comp']).sum().drop(columns=['تاریخ عملیات عددی'])
        dff['از تاریخ'] = df.groupby(by=['دوره عملیات','comp']).min()['تاریخ عملیات عددی']
        dff['تا تاریخ'] = df.groupby(by=['دوره عملیات','comp']).max()['تاریخ عملیات عددی']
        dff = dff.reset_index()
        dff['از تاریخ'] = [timedate.intToDate(x) for x in dff['از تاریخ']]
        dff['تا تاریخ'] = [timedate.intToDate(x) for x in dff['تا تاریخ']]
        dff = dff.to_dict(orient='records')
        return json.dumps({'replay':True,'df':dff})
    else:
        return ErrorCookie()

def getcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dfIssuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0}))
        dfAssing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username},{'_id':0,'username':0}))
        if len(dfIssuing)==0:
            return json.dumps({'replay':False, 'msg':'هیچ صدوری ثبت نشده است'})
        df = dfIssuing.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','comp'],keep='last')
        if len(dfAssing) == 0:
            df['cunsoltant'] = ''
        else:
            dfIssuing = dfIssuing.set_index(['comp','کد رایانه صدور بیمه نامه'])
            print(dfAssing)
            dfAssing = dfAssing.set_index(['comp','کد رایانه صدور بیمه نامه'])
            df = dfIssuing.join(dfAssing,how='left').reset_index()
            df = df.fillna('')
            if data['showAll']==False:
                df = df[df['cunsoltant']=='']
                df['cunsoltantName'] = ''
            else:
                cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
                df['cunsoltantName'] = [NCtName(cl_consultant,x) for x in df['cunsoltant']]
        df = df.fillna('')
        df = df.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','comp'],keep='last')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()


def assingcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        print(data)
        pishkarDb['AssingIssuing'].delete_many({'username':username,'comp':data['InsurenceData']['comp'],'کد رایانه صدور بیمه نامه':data['InsurenceData']['کد رایانه صدور بیمه نامه']})
        pishkarDb['AssingIssuing'].insert_one({'username':username,'comp':data['InsurenceData']['comp'],'کد رایانه صدور بیمه نامه':data['InsurenceData']['کد رایانه صدور بیمه نامه'],'cunsoltant':data['consultant']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getissuingmanual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username}))
        if len(df)>0:
            df = df[['رشته','کد رایانه صدور بیمه نامه','مورد بیمه','پرداخت کننده حق بیمه','شماره بيمه نامه','تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','تاریخ سررسید','مبلغ کل حق بیمه','مبلغ تسویه شده','بدهی باقی مانده','comp']]
        df = df.fillna('')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def addissuingmanual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        IssuingDict = data['IssuingDict']
        try:
            IssuingDict['مبلغ کل حق بیمه'] = int(IssuingDict['مبلغ کل حق بیمه'])
            IssuingDict['مبلغ تسویه شده'] = int(IssuingDict['مبلغ تسویه شده'])
            IssuingDict['بدهی باقی مانده'] = int(IssuingDict['بدهی باقی مانده'])
        except:
            return json.dumps({'replay':False,'msg':'مبلغ کل حق بیمه، مبلغ تسویه شده، بدهی باقی مانده میبایست از نوع عددی باشد'})
        try:
            IssuingDict['تاریخ عملیات'] = timedate.dateToStandard(IssuingDict['تاریخ عملیات'])
            IssuingDict['تاریخ سررسید'] = timedate.dateToStandard(IssuingDict['تاریخ سررسید'])
            IssuingDict['تاريخ بيمه نامه يا الحاقيه'] = timedate.dateToStandard(IssuingDict['تاريخ بيمه نامه يا الحاقيه'])
        except:
            return json.dumps({'replay':False,'msg':'تاریخ عملیات، تاریخ سررسید، تاريخ بيمه نامه يا الحاقيه میبایست از نوع تاریخ باشد'})
        IssuingDict['username'] = username
        dupl = pishkarDb['issuing'].find_one({'username':username,'comp':IssuingDict['comp'],'کد رایانه صدور بیمه نامه':IssuingDict['کد رایانه صدور بیمه نامه'],'تاریخ سررسید':IssuingDict['تاریخ سررسید']})
        if dupl!=None:
            return json.dumps({'replay':False,'msg':'این صدور قبلا ثبت شده است'})
        pishkarDb['issuing'].insert_one(IssuingDict)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
    
def delfile(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'comp':data['data']['comp']}))
        df['دوره عملیات'] = [timedate.dateToPriod(x) for x in df['تاریخ عملیات']]
        df = df[df['دوره عملیات']==data['data']['دوره عملیات']]
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'موردی یافت نشد'})
        for i in df.index:
            pishkarDb['issuing'].delete_one({'_id':df['_id'][i]})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()