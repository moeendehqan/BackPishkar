import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
client = pymongo.MongoClient()
pishkarDb = client['pishkar']



def uploadfile(date,cookier,file,comp):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        duplic = pd.DataFrame(pishkarDb['Fees'].find({'UploadDate':date,'comp':comp,'username':username}))
        if len(duplic) == 0:
            df = pd.read_excel(file)
            df['UploadDate'] = date
            df['comp'] = comp
            df['username'] = username
            df= df.to_dict(orient='records')
            pishkarDb['Fees'].insert_many(df)
            return json.dumps({'replay':True, 'len':len(df)})
        else:
            return json.dumps({'replay':False, 'msg':f'فایل گزارش شرکت ({comp}) برای ({date}) قبلا ثبت شده است.'})
    else:
        return ErrorCookie()


def getfeesuploads(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username}))
        if len(df)>0:
            df = df[['comp','UploadDate']].drop_duplicates()
            df = df.to_dict(orient='records')
            return json.dumps({'df':df})
        else:
            return json.dumps({'df':None})
    else:
        return ErrorCookie()

def delupload(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    print(data)
    if user['replay']:
        pishkarDb['Fees'].delete_many({'username':username,'UploadDate':data['date'],'comp':data['comp']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
