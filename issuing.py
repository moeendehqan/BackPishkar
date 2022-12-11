import json
import timedate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
from assing import NCtName
import timedate
client = pymongo.MongoClient()
pishkarDb = client['pishkar']





def addfileNoneAdditional(cookier,file,comp):
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
        df['additional'] = 'اصلی'
        df = df.to_dict(orient='records')
        pishkarDb['issuing'].insert_many(df)
        msg = 'ثبت شد \n ' + msg
        return json.dumps({'replay':True,'additional':False,'msg':msg})
    else:
        return ErrorCookie()

def addfilewitheAdditional(cookier,file,comp,additional):
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
        df['additional'] = 'اصلی'
        additionals = str(additional).split('},{')
        additionals = [json.loads('{'+(x.replace('{','').replace('}',''))+'}') for x in additionals]
        for i in df.index:
            for j in additionals:
                if df['comp'][i] == j['comp'] and df['کد رایانه صدور بیمه نامه'][i] == j['کد رایانه صدور بیمه نامه'] and df['شماره الحاقیه'][i] == j['شماره الحاقیه']:
                    print('000000')
                    df['additional'][i] = j['additional']
        df = df.to_dict(orient='records')
        pishkarDb['issuing'].insert_many(df)
        msg = 'ثبت شد \n ' + msg
        return json.dumps({'replay':True,'additional':False,'msg':msg})
    else:
        return ErrorCookie()

def CheackAdditional(cookier,file,comp,additional):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        try:df = pd.read_excel(file)
        except: return json.dumps({'replay':False,'msg':'خطا، فایل دارای ایراد است'})
        msg = ''
        requiedCulomns = ['رشته','مورد بیمه','کد رایانه صدور بیمه نامه','پرداخت کننده حق بیمه',
            'شماره بيمه نامه','شماره الحاقیه','تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','تاریخ سررسید',
            'تاريخ سند دريافتي','وضعيت وصول','مبلغ کل حق بیمه','مبلغ تسویه شده','بدهی باقی مانده']
        for rc in requiedCulomns:
            inculomns = rc in df.columns
            if inculomns==False:
                return json.dumps({'replay':False,'msg':f'فایل فاقد ستون ضروری "{rc}" است'})
        df['comp'] = comp
        df['username'] = username
        df = df[df['شماره الحاقیه']!=0]
        print(df)
        if len(df)==0:
            return addfileNoneAdditional(cookier,file,comp)
        if additional =='null':
            df = df.drop_duplicates(['کد رایانه صدور بیمه نامه','تاریخ سررسید'])
            df = df[['کد رایانه صدور بیمه نامه','شماره الحاقیه','comp']]
            df['additional'] = 'اضافی'
            df = df.to_dict(orient='records')
            return json.dumps({'replay':True,'additional':df})
        else:
            return addfilewitheAdditional(cookier,file,comp,additional)
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
        df = df[['تاریخ عملیات','مبلغ کل حق بیمه','comp','کد رایانه صدور بیمه نامه']]
        df = df.fillna('')
        df['دوره عملیات'] = [timedate.dateToPriod(x) for x in df['تاریخ عملیات']]
        df['تعداد'] = 1
        df['تاریخ عملیات عددی'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        dfcount = df.drop_duplicates(subset=['کد رایانه صدور بیمه نامه'])
        dfcount['count'] = 1
        dfcount = dfcount.groupby(by=['دوره عملیات','comp']).sum()[['count','مبلغ کل حق بیمه']]
        dff = df.groupby(by=['دوره عملیات','comp']).sum().drop(columns=['تاریخ عملیات عددی','مبلغ کل حق بیمه'])
        dff['از تاریخ'] = df.groupby(by=['دوره عملیات','comp']).min()['تاریخ عملیات عددی']
        dff['تا تاریخ'] = df.groupby(by=['دوره عملیات','comp']).max()['تاریخ عملیات عددی']
        dff = dff.join(dfcount)
        dff = dff.dropna()
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
            df = df[['رشته','کد رایانه صدور بیمه نامه','مورد بیمه','پرداخت کننده حق بیمه','شماره بيمه نامه','تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','تاریخ سررسید','مبلغ کل حق بیمه','مبلغ تسویه شده','بدهی باقی مانده','comp','additional']]
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
            pishkarDb['issuing'].delete_many({'username':username,'comp':IssuingDict['comp'],'کد رایانه صدور بیمه نامه':IssuingDict['کد رایانه صدور بیمه نامه'],'تاریخ سررسید':IssuingDict['تاریخ سررسید']})
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

def delissuingmanual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['issuing'].delete_many({'username':username,'comp':data['dict']['comp'],'کد رایانه صدور بیمه نامه':data['dict']['کد رایانه صدور بیمه نامه'],'تاریخ سررسید':data['dict']['تاریخ سررسید'],'شماره بيمه نامه':data['dict']['شماره بيمه نامه']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def getadditional(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'شماره بيمه نامه':1,'رشته':1,'کد رایانه صدور بیمه نامه':1,'مورد بیمه':1,'پرداخت کننده حق بیمه':1,'additional':1,'تاريخ بيمه نامه يا الحاقيه':1,'مبلغ کل حق بیمه':1,'comp':1}))
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه نامه ای یافت نشد'})
        add = df[df['additional']!='اصلی']
        add['count'] = 1
        add = add.groupby(by=['کد رایانه صدور بیمه نامه','comp']).sum()
        add = add[['count']]
        df = df.drop_duplicates(subset=['comp','کد رایانه صدور بیمه نامه'])
        df = df[df['additional']=='اصلی']
        df = df.set_index(['کد رایانه صدور بیمه نامه','comp'])
        df = df.join(add,how='left')
        df['count'] = df['count'].fillna(0)
        df = df.reset_index()
        print(df)
        df = df.fillna('')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def addaditional(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        additionalDict = data['additionalDict']
        df = pishkarDb['issuing'].find_one({'username':username,'کد رایانه صدور بیمه نامه': additionalDict['کد رایانه صدور بیمه نامه'], 'comp':additionalDict['comp']},{'_id':0})
        df['additional'] = additionalDict['additional']
        if(additionalDict['additional']=='برگشتی'):
            if(additionalDict['مبلغ کل حق بیمه']>df['مبلغ کل حق بیمه']):
                return json.dumps({'replay':True,'msg':'حق بیمه الحاقیه برگشتی نمیتواند بیشتر از حق بیمه اصلی باشد'})
        df['مبلغ کل حق بیمه'] = additionalDict['مبلغ کل حق بیمه']
        df['مبلغ تسویه شده'] = additionalDict['مبلغ تسویه شده']
        df['بدهی باقی مانده'] = additionalDict['بدهی باقی مانده']
        df['تاریخ سررسید'] = additionalDict['تاریخ سررسید']
        df['تاریخ عملیات'] = additionalDict['تاریخ عملیات']
        df['تاريخ بيمه نامه يا الحاقيه'] = additionalDict['تاريخ بيمه نامه يا الحاقيه']
        df['شماره الحاقیه'] = additionalDict['شماره الحاقیه']
        df['شماره بيمه نامه'] = additionalDict['شماره بيمه نامه']
        pishkarDb['issuing'].insert_one(df)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
