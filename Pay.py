import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import datetime
import timedate
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def feeRate(feild,consultant):
    try: r = int(consultant[feild])/100
    except: r = 0
    return r

def get(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        fixPay = {}
        consultant = pishkarDb['cunsoltant'].find_one({'username':username,'nationalCode':data['consultant']},{'_id':0})
        if consultant['salary']:
            act = pishkarDb['act'].find_one({'username':username,'nationalCode':data['consultant'],'period':data['period']['Show']},{'_id':0})
            yearBasePay = str(data['period']['Show']).split('-')[0].replace(' ','')
            salary = pishkarDb['salary'].find_one({'username':username,'year':yearBasePay})
            taxDf = pishkarDb['taxe'].find_one({'username':username,'year':yearBasePay})
            if act == None: return json.dumps({'replay':False,'msg':'کارکرد ثبت نشده است'})
            if salary == None: return json.dumps({'replay':False,'msg':'سطوح پایه برای سال انتخابی ثبت نشده است'})
            if taxDf == None: return json.dumps({'replay':False,'msg':'سطوح معافیت های مالیاتی برای سال انتخابی ثبت نشده است'})
            fixPay['income'] = {'base':int(act['act']) * int(salary['daily'])}
            paydate = datetime.datetime.fromtimestamp(int(data['period']['date'])/1000)
            employmentDate = datetime.datetime.fromtimestamp(consultant['employment']/1000)
            if (paydate - employmentDate).days >365: fixPay['income']['sanavat'] = int(act['act']) * int(salary['sanavat'])
            else: fixPay['income']['sanavat'] = 0
            fixPay['income']['subsidy'] = round(int(act['act'])/30) * int(salary['subsidy'])
            fixPay['income']['homing'] = round(int(act['act'])/30) * int(salary['homing'])
            fixPay['income']['childern'] = round(((int(act['act'])/30) * int(salary['childern'])) * int(consultant['childern']))
            fixPay['output'] = {'insurance':round((fixPay['income']['base']+fixPay['income']['sanavat']+fixPay['income']['subsidy']+fixPay['income']['homing'])*0.07)}
            inTax = (fixPay['income']['base'] + fixPay['income']['sanavat'] + fixPay['income']['subsidy'] + fixPay['income']['childern']) - fixPay['output']['insurance']
            taxDf = pd.DataFrame.from_dict({'level':[taxDf['incomeLevel1'],taxDf['incomeLevel2'],taxDf['incomeLevel3'],taxDf['incomeLevel4'],taxDf['incomeLevel5'],taxDf['incomeLevel6'],taxDf['incomeLevel7']],'rate':[taxDf['taxeLevel1'],taxDf['taxeLevel2'],taxDf['taxeLevel3'],taxDf['taxeLevel4'],taxDf['taxeLevel5'],taxDf['taxeLevel6'],taxDf['taxeLevel7']]})
            taxDf = taxDf.replace('',0)
            taxDf['level'] = [int(x) for x in taxDf['level']]
            taxDf['rate'] = [int(x)/100 for x in taxDf['rate']]
            taxDf = taxDf[taxDf>0].dropna()
            if len(taxDf) == 0 : fixPay['output']['taxe'] = 0
            else:
                taxDf['inTax'] = inTax - taxDf['level']
                taxDf = taxDf[taxDf['inTax']>0]
                taxDf['taxe'] = taxDf['inTax'] * taxDf['rate']
                fixPay['output']['taxe'] = round(taxDf['taxe'].sum())
        dfFees = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'username':0}))
        dfFees = dfFees.drop_duplicates(subset=['شماره بيمه نامه']).set_index('شماره بيمه نامه')
        dfFees = dfFees[dfFees['UploadDate']==data['period']['Show']]
        dfFees['تاریخ صدور بیمه نامه'] = [timedate.diffTime2(x) for x in dfFees['تاریخ صدور بیمه نامه']]
        dfFees = dfFees[dfFees['تاریخ صدور بیمه نامه']<=1825]
        assing = pd.DataFrame(pishkarDb['assing'].find({'username':username,'consultant':consultant['nationalCode']},{'_id':0,'username':0})).set_index('شماره بيمه نامه')
        if len(assing)==0: fixPay['revard'] = 0
        else:
            dfFees = dfFees.join(assing,how='left')
            dfFees = dfFees.dropna(subset=['consultant'])
            if len(assing)==0: fixPay['revard'] = 0
            else:
                dfFees['feild'] = dfFees['رشته'].fillna('')+' ('+dfFees['مورد بیمه'].fillna('')+')'
                dfFees['feild'] = [str(x).replace(' ()','') for x in dfFees['feild']]
                dfFees['feeRate'] = [feeRate(x,consultant) for x in dfFees['feild']]
                dfFees['fee'] = dfFees['feeRate'] * dfFees['كارمزد قابل پرداخت']
                fixPay['revard'] = round(dfFees['fee'].sum())
        return json.dumps({'replay':True,'pay':fixPay})
    else:
        return ErrorCookie()
    #125