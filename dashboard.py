import json
import timedate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import random
import string
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def issunigSum(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'ردیف':0,
            'شعبه واحد صدور بیمه نامه یا الحاقیه':0, 'واحدصدور بیمه نامه':0, 'نمایندگی بیمه نامه':0,
            'كد رايانه بيمه نامه':0, 'کد رایانه عملیات': 0, 'تاريخ سند دريافتي':0 ,'تاريخ واگذاري':0 ,
            'تاريخ وضعیت وصول':0, 'وضعيت وصول':0 , 'شماره سند دريافتي':0 ,'شرح دريافتي':0 , 'شماره حساب':0,
            'حساب واگذاري':0, 'بانك':0, 'شرح عملیات':0,'username':0, 'additional':0}))
        df['تاریخ عملیات عددی'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        OnToday = df['تاریخ عملیات عددی'].max()
        OnFirst  = df['تاریخ عملیات عددی'].min()
        LenAllPeriod = timedate.DiffTwoDateInt(OnToday,OnFirst)
        OnPeroid = timedate.deltaToDayInt(data['period'],OnToday)
        OnStart = timedate.deltaToDayInt(data['period'],OnPeroid)
        SumToday = df[df['تاریخ عملیات عددی']>OnPeroid]['مبلغ کل حق بیمه'].sum()
        SumLast = df[df['تاریخ عملیات عددی']<=OnPeroid][df['تاریخ عملیات عددی']>=OnStart]['مبلغ کل حق بیمه'].sum()
        SumAll = df['مبلغ کل حق بیمه'].sum()
        MeanDays = SumAll / LenAllPeriod
        MeanPeriod = int(MeanDays * int(data['period']))
        ToMeanPeriod = round((int(SumToday) / MeanPeriod) - 1,2)
        if SumToday ==0 or SumLast==0:
            grow = 0
        else:
            grow = int(((SumToday / SumLast)-1)*10000)/100
        return json.dumps({'replay':True,'df':{'OnPeroid':int(SumToday),'LastPeriod':int(SumLast),'Grow':grow,'MeanPeriod':MeanPeriod, 'ToMeanPeriod':ToMeanPeriod}})
    else:
        return ErrorCookie()

def issunigFeild(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        stndrd = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0, 'field':1, 'groupMain':1,'groupSub':1}))
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'ردیف':0,
            'شعبه واحد صدور بیمه نامه یا الحاقیه':0, 'واحدصدور بیمه نامه':0, 'نمایندگی بیمه نامه':0,
            'كد رايانه بيمه نامه':0, 'کد رایانه عملیات': 0, 'تاريخ سند دريافتي':0 ,'تاريخ واگذاري':0 ,
            'تاريخ وضعیت وصول':0, 'وضعيت وصول':0 , 'شماره سند دريافتي':0 ,'شرح دريافتي':0 , 'شماره حساب':0,
            'حساب واگذاري':0, 'بانك':0, 'شرح عملیات':0,'username':0, 'additional':0}))
        df['تاریخ عملیات'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        df = df.fillna('')
        df['Field'] =  df['رشته'] + ' ('+df['مورد بیمه']+')'
        df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
        df = df.set_index(['Field']).join(stndrd.set_index(['field']),how='left').reset_index()
        OnToday = df['تاریخ عملیات'].max()
        OnPeroid = timedate.deltaToDayInt(data['period'],OnToday)
        df = df[df['تاریخ عملیات']>OnPeroid]
        df = df.groupby(by=['groupSub']).sum()[['مبلغ کل حق بیمه']].reset_index()
        labels = df['groupSub'].to_list()
        dataa = df['مبلغ کل حق بیمه'].to_list()
        stri = string.ascii_lowercase+string.ascii_uppercase+string.digits
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]}})
    else:
        return ErrorCookie()

def issuniginsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'ردیف':0,
            'شعبه واحد صدور بیمه نامه یا الحاقیه':0, 'واحدصدور بیمه نامه':0, 'نمایندگی بیمه نامه':0,
            'كد رايانه بيمه نامه':0, 'کد رایانه عملیات': 0, 'تاريخ سند دريافتي':0 ,'تاريخ واگذاري':0 ,
            'تاريخ وضعیت وصول':0, 'وضعيت وصول':0 , 'شماره سند دريافتي':0 ,'شرح دريافتي':0 , 'شماره حساب':0,
            'حساب واگذاري':0, 'بانك':0, 'شرح عملیات':0,'username':0, 'additional':0}))
        df['تاریخ عملیات'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        df = df.fillna('')
        OnToday = df['تاریخ عملیات'].max()
        OnPeroid = timedate.deltaToDayInt(data['period'],OnToday)
        df = df[df['تاریخ عملیات']>OnPeroid]
        df = df.groupby(by=['comp']).sum()[['مبلغ کل حق بیمه']].reset_index()
        labels = df['comp'].to_list()
        dataa = df['مبلغ کل حق بیمه'].to_list()
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]}})
    else:
        return ErrorCookie()


def FeeSum(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'رشته':1,
            'مورد بیمه':1, 'کد رایانه صدور':1, 'كارمزد قابل پرداخت':1, 'کل مبلغ وصول شده': 1,
            'UploadDate':1 ,'comp':1 , 'تاریخ صدور بیمه نامه':1}))
        df['UploadDate'] = [timedate.PriodStrToInt(x) for x in df['UploadDate']]
        df = df.groupby(by=['UploadDate']).sum().reset_index()
        df = df[df.index<int(data['period'])]
        df = df.sort_values(by=['UploadDate'],ascending=False)
        labels = df['UploadDate'].to_list()
        labels = [timedate.PeriodIntToPeriodStr(x) for x in labels]
        dataa = df['كارمزد قابل پرداخت'].to_list()
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]}})
    else:
        return ErrorCookie()
    


def FeeFeild(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        stndrd = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0, 'field':1, 'groupMain':1,'groupSub':1}))
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'رشته':1, 'مورد بیمه':1,
            'كارمزد قابل پرداخت':1,'UploadDate':1, 'comp': 1,}))
        df = df.fillna('')
        df['Field'] =  df['رشته'] + ' ('+df['مورد بیمه']+')'
        df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
        df = df.set_index(['Field']).join(stndrd.set_index(['field']),how='left').reset_index()
        df['UploadDate'] = [timedate.PriodStrToInt(x) for x in df['UploadDate']]
        start = int(timedate.FeeFeildStart(df['UploadDate'].max(),data['period']))
        df = df[df['UploadDate']>=start]
        df = df.groupby(by=['groupSub']).sum()[['كارمزد قابل پرداخت']].reset_index()
        labels = df['groupSub'].to_list()
        dataa = df['كارمزد قابل پرداخت'].to_list()
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]}})
    else:
        return ErrorCookie()

def FeeInsurence(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'كارمزد قابل پرداخت':1,
            'UploadDate':1, 'comp': 1,}))
        df = df.fillna('')
        df['UploadDate'] = [timedate.PriodStrToInt(x) for x in df['UploadDate']]
        start = int(timedate.FeeFeildStart(df['UploadDate'].max(),data['period']))
        df = df[df['UploadDate']>=start]
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        df['comp'] = [insurec[x] for x in df['comp']]
        df = df.groupby(by=['comp']).sum()[['كارمزد قابل پرداخت']].reset_index()
        labels = df['comp'].to_list()
        dataa = df['كارمزد قابل پرداخت'].to_list()
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]}})
    else:
        return ErrorCookie()