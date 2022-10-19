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
        consultant = consultant.drop(columns=['fristName','lastName','nationalCode','gender','phone','code'])
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