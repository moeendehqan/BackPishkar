import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def comparisom(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        fee = pd.DataFrame(pishkarDb['Fees'].find({'username':username,'UploadDate':data['datePeriod']['Show']},{'_id':0,'comp':1,'شرح':1,'رشته':1,'مورد بیمه':1,'کل کارمزد محاسبه شده':1,'کل مبلغ وصول شده':1}))
        feeStandard = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0}))
        if len(fee)==0:
            return json.dumps({'replay':False,'msg':'فایل کارمزد تا کنون بارگذاری نشده است'})
        if len(feeStandard)==0:
            return json.dumps({'replay':False,'msg':'هیچ دسته بندی یافت نشد'})
        fee = fee.fillna('')
        feeStandard = feeStandard.fillna(0)
        fee['Field'] =  fee['رشته'] + ' ('+fee['مورد بیمه']+')'
        fee['Field'] = [str(x).replace(' ()','') for x in fee['Field']]
        df = fee.set_index(['Field']).join(feeStandard.set_index(['field']),how='left').reset_index()
        df = df[df['groupMain']!='بیمه زندگی'].drop(columns=['index','رشته','مورد بیمه','baseRateLive','firstFeeRateLive','secendFeeRateLive'])
        df['Added'] = [("الحاقيه" not in x) or ("شماره الحاقيه 0" in x) for x in df['شرح']]
        df = df[df['Added']==True]
        df = df[df['کل مبلغ وصول شده']!='']
        df = df[df['کل کارمزد محاسبه شده']!='']
        df['کل کارمزد محاسبه شده'] = [int(x) for x in df['کل کارمزد محاسبه شده']]
        df['کل مبلغ وصول شده'] = [int(x) for x in df['کل مبلغ وصول شده']]
        df['rate'] = [int(x) for x in df['rate']]
        df['RealFeeRate'] = (df['کل کارمزد محاسبه شده'] / df['کل مبلغ وصول شده'])
        df = df.drop(columns=['شرح','date','username','dateshow','Added','کل مبلغ وصول شده','کل کارمزد محاسبه شده'])
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        df['comp'] = [insurec[x] for x in df['comp']]
        df = df.groupby(by=['comp','groupMain','groupSub']).mean()
        df['RealFeeRate'] = [int(x*10000)/100 for x in df['RealFeeRate']]
        df['OutLine'] = df['RealFeeRate'] - df['rate']
        df['OutLine'] = [round(x,2) for x in df['OutLine']]
        df['rate'] = [round(x,0) for x in df['rate']]
        df = df.reset_index()
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()