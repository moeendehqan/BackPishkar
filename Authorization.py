import json
import timedate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
from assing import NCtName
import timedate
client = pymongo.MongoClient()
pishkarDb = client['pishkar']



lincensList = {'اطلاعات پایه':['کاربران','مشاوران','تلفیق مشاوران','مدیریت شعب','بیمه گر','کارمزد استاندارد (آیین نامه)'],
                'اطلاعات حقوق و دستمزد':['گروه های حقوق و دستمزد','حداقل حقوق','کارمزد مشاوران','جدول مالیات'],
                'مدیریت صدور':['افزودن فایل صدور','افزودن دستی صدور','تخصیص مشاور صدور'],
                'مدیریت کارمزد':['افزودن کارمزدها','تخصیص مشاوران'],
                'مدیریت حقوق و دستمزد':['عملکرد ماهانه شعب','کارکرد پرسنل','سایر مزایا (غیرنقدی)'],
                'گزارش حقوق و دستمزد':['ماهانه خلاصه','ماهانه کلی'],
                'گزارشات مشاوران':['پرفراژ','پیشبینی کارمزد های آتی'],
                'گزارشات مدیریتی':['مقایسه کارمزد (غیرزندگی)','داشبورد']
                }

def Authorization():
    #user = cookie(data)
    #user = json.loads(user)
    username = '09131533223'
    if True:#user['replay']:
        df = pd.DataFrame(pishkarDb['sub'].find({'username':username}))
        for i in lincensList.keys():
            key = lincensList[i]
            for k in key:
                lincens = i + ' - ' + k
                if lincens not in df.columns:
                    df[lincens] = False
    else:
        return ErrorCookie()

def lincenslist(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        lincenslist = []
        for i in lincensList.keys():
            key = lincensList[i]
            for k in key:
                lincens = i + ' - ' + k
                lincenslist.append(lincens)
        return json.dumps({'replay':True,'lincenslist':lincenslist})
    else:
        return ErrorCookie()


def getsub(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['sub'].find({'username':username},{'_id':0}))
        print(df)
        try:df['name'] = df['name'].fillna('')
        except:df['name'] = ''
        df = df.fillna(False)
        if len(df)==0:
            return json.dumps({'replay':False})
        getlincenslist = json.loads(lincenslist(data))['lincenslist']
        for i in getlincenslist:
            if i not in df.columns:
                df[i] = False
        for i in df.columns:
            if i not in getlincenslist and i!='username' and i!='subPhone' and i!='name':
                df = df.drop(columns=[i])
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df,'lincenslist':getlincenslist})
    else:
        return ErrorCookie()