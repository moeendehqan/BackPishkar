import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import timedate
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


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
        consultant = consultant.drop(columns=['fristName','lastName','nationalCode','gender','phone','code','childern','freetaxe','salaryGroup','insureWorker','insureEmployer'])
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
        print(dic)
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