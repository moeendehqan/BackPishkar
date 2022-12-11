import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import timedate
import numpy as np
from reports import comparisomForFrocast
import random
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def NCtName(cl,nc):
    if str(nc)!='nan':
        df = cl[cl['nationalCode']==nc]
        if len(df)>0:
            df['full'] = df['fristName'] +' '+ df['lastName']
            df = df['full'][df.index.max()]
            return df
        else:
            return 'بدون مشاور'
    else:
        return 'بدون مشاور'
    
def getfees(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        feild = pd.DataFrame(pishkarDb['Fees'].find({'username':username}))[['رشته','مورد بیمه']]
        feild = feild.drop_duplicates().reset_index().drop(columns=['index']).fillna('')
        feild['feild'] = feild['رشته']+' ('+feild['مورد بیمه']+')'
        feild = list(feild['feild'])
        feild = [str(x).replace(' ()','') for x in feild]
        consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username,'nationalCode':data['nationalCode']},{'_id':0,'username':0}))
        for f in feild:
            if f not in consultant.columns:
                consultant[f] = 0
        for i in ['fristName','lastName','ConsultantSelected','nationalCode','gender','phone','code','childern','freetaxe','salaryGroup','insureWorker','insureEmployer']:
            try: consultant = consultant.drop(columns=[i])
            except: pass
        consultant = consultant.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':consultant})
    else:
        return ErrorCookie()
            

def setfees(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dic = data['fees']

        del dic['salary']
        pishkarDb['cunsoltant'].update_one({'username':username,'nationalCode':data['nc']},{'$set':data['fees']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getatc(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username,'salary':True},{'_id':0,'fristName':1,'lastName':1,'nationalCode':1,'gender':1}))
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ مشاوری تعریف نشده'})
        actDf = pd.DataFrame(pishkarDb['act'].find({'username':username,'period':data['today']['Show']},{'_id':0}))
        if len(actDf)==0:
            df['act'] = 0
            df['period'] = data['today']['Show']
        else:
            df = df.set_index('nationalCode').join(actDf.set_index('nationalCode')).reset_index()
            df['period'] = data['today']['Show']
        df = df[['nationalCode','fristName','lastName','gender','act','period']].fillna(0)
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})

    else:
        return ErrorCookie()


def setatc(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        act = (data['listatc'])
        for i in act:
            pishkarDb['act'].delete_many({'username':username,'nationalCode':i['nationalCode'],'period':i['period']})
            pishkarDb['act'].insert_one({'username':username,'nationalCode':i['nationalCode'],'period':i['period'],'act':i['act']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()



def actcopylastmonth(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        copy = pd.DataFrame(pishkarDb['act'].find({'username':username},{'_id':0}))
        copy['period'] = [timedate.PriodStrToInt(x) for x in copy['period']]
        copy = copy[copy['period']==copy['period'].max()]
        copy['period'] = data['date']['Show']
        copy = copy.to_dict(orient='records')
        pishkarDb['act'].insert_many(copy)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def forcastfee(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    data['OnBase']='Title'
    if user['replay']:
        backward = timedate.PeriodAndForwardPeriodInt(data['datePeriod']['Show'])
        issuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'username':0})).set_index(['comp','کد رایانه صدور بیمه نامه'])
        issuing['InNowPeriodS'] = [timedate.DateToPeriodInt(x)==backward for x in issuing['تاریخ سررسید']]
        issuing['InNowPeriodA'] = [timedate.DateToPeriodInt(x)==backward for x in issuing['تاریخ عملیات']]
        issuing['amount'] = [int(x)>0 for x in issuing['مبلغ تسویه شده']]
        issuing['SA'] = issuing['InNowPeriodA'] * issuing['amount'] 
        issuing['NowPeriod'] = issuing['InNowPeriodS'] * issuing['SA']
        issuing = issuing[issuing['NowPeriod']==True]
        assing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username},{'_id':0,'username':0})).set_index(['comp','کد رایانه صدور بیمه نامه'])
        comparisomValue = json.loads(comparisomForFrocast(data))
        fees = pd.DataFrame(comparisomValue['df']).drop(columns=['rate','OutLine','groupMain','groupSub','index'])
        fees = fees.set_index(['comp','Title'])
        fees = fees.to_dict(orient='dict')['RealFeeRate']
        df = issuing.join(assing).reset_index()
        df['رشته'] = df['رشته'].fillna('')
        df['مورد بیمه'] = df['مورد بیمه'].fillna('')
        df['Title'] =  df['رشته'] + ' ('+df['مورد بیمه']+')'
        df['Title'] = [str(x).replace(' ()','') for x in df['Title']]
        df['fees'] = 0
        for i in df.index:
            field = (df['comp'][i],df['Title'][i])
            try:
                df['fees'][i] = fees[field]
            except:
                pass
        df['additional'] = ((df['additional'] != 'برگشتی')*1)
        df['additional'] = [int(str(x).replace('0','-1')) for x in df['additional']]
        df['fees'] = df['fees'] * df['additional']
        df['مبلغ تسویه شده'] = [int(x) for x in df['مبلغ تسویه شده']]
        df['بدهی باقی مانده'] = [int(x) for x in df['بدهی باقی مانده']]
        df['GetA'] = (df['fees'] * df['مبلغ تسویه شده']) * df['SA']
        df['GetB'] = (df['fees'] * df['بدهی باقی مانده']) * df['InNowPeriodS']
        df['GetTotal'] = df['GetA'] + df['GetB']
        df['Count'] = 1
        df = df.groupby(by=['cunsoltant']).sum().reset_index()[['cunsoltant','GetTotal','Count']]
        cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
        df['cunsoltant'] = [NCtName(cl_consultant,x) for x in df['cunsoltant']]
        df['GetTotal'] = [int(x) for x in df['GetTotal']]
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def addintegration(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        lst = [x['code'] for x in data['ConsultantSelected']]
        if len(lst)!=len(set(lst)):
            return json.dumps({'replay':False, 'msg':'مشاوران میبایست غیر تکراری باشند'})
        inDb = pishkarDb['cunsoltant'].find_one({'username':username,'lastName':data['name']})
        if inDb!=None:
            return json.dumps({'replay':False, 'msg':'نام گروه تلفیق تکراری است'})
        dic = {'fristName':'تلفیق','lastName':data['name'],'nationalCode':str(random.randint(1000000000,9999999999)),'gender':'گروه','salary':False,'childern':0,'freetaxe':0,'insureWorker':0,'insureEmployer':0,'username':username}
        dic['ConsultantSelected'] = data['ConsultantSelected']
        pishkarDb['cunsoltant'].insert_one(dic)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getintegration(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username,'fristName':'تلفیق','gender':'گروه'},{'_id':0,'fristName':1,'lastName':1,'nationalCode':1,'gender':1,'ConsultantSelected':1}))
        df['count'] = 0
        df['ditaile'] = ''
        cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
        for i in df.index:
            listconsultant = df['ConsultantSelected'][i]
            newlist = ''
            df['count'][i] = len(listconsultant)
            for j in listconsultant:
                consultant = NCtName(cl_consultant,j['code'])
                fee = j['fee']
                newlist = newlist + '(' + str(consultant)+' %' + str(fee) + ') '
            df['ditaile'][i] = newlist
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()

def delintegration(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        nationalCode = data['code']['nationalCode']
        pishkarDb['cunsoltant'].delete_many({'username':username,'nationalCode':nationalCode})
        pishkarDb['AssingIssuing'].delete_many({'username':username,'AssingIssuing':nationalCode})
        pishkarDb['benefit'].delete_many({'username':username,'nationalCode':nationalCode})
        pishkarDb['assing'].delete_many({'username':username,'assing':nationalCode})
        print(nationalCode)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()