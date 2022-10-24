import json
from attr import field
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
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
        consultant = consultant.drop(columns=['fristName','lastName','nationalCode','gender','phone','code','childern'])
        consultant = consultant.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':consultant})
    else:
        return ErrorCookie()
            

def setfees(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        print(data['fees'])
        pishkarDb['cunsoltant'].update_one({'username':username,'nationalCode':data['nc']},{'$set':data['fees']})
        return json.dumps({'o':"o"})
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
        else:
            df = df.set_index('nationalCode').join(actDf.set_index('nationalCode')).reset_index()
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})

    else:
        return ErrorCookie()


def setatc(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        act = str(data['act'])
        if len(act)==0:act = 0
        else: act = int(act)
        if act>31:
            return json.dumps({'replay':False, 'msg':'حداکثر کارکرد 31 روز میباشد'})
        if pishkarDb['act'].find_one({'username':username, 'nationalCode':data['nc'], 'period':data['period']['Show']})==None:
            pishkarDb['act'].insert_one({'username':username, 'nationalCode':data['nc'],'act':data['act'],'period':data['period']['Show']})
        else:
            pishkarDb['act'].update_one({'username':username, 'nationalCode':data['nc'], 'period':data['period']['Show']},{'$set':{'act':data['act']}})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
