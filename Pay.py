import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import datetime
import timedate
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']
import time
def feeRate(feild,consultant):
    try: r = int(consultant[feild])/100
    except: r = 0
    return r

def get(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        consultantList = list(pishkarDb['cunsoltant'].find({'username':username},{'_id':0}))
        df = pd.DataFrame()
        for c in consultantList:
            fixPay = {}
            fullName = c['gender'] +' '+ c['fristName'] +' '+ c['lastName']

            if c['salary']:
                act = pishkarDb['act'].find_one({'username':username,'nationalCode':c['nationalCode'],'period':data['period']['Show']},{'_id':0})
                yearBasePay = str(data['period']['Show']).split('-')[0].replace(' ','')
                salaryGruop = c['salaryGroup']
                salary = pishkarDb['salary'].find_one({'username':username,'year':yearBasePay,'gruop':salaryGruop})
                if salary== None: return json.dumps({'replay':False,'msg':f'گروه حقوق {salaryGruop}یافت نشد'})
                taxDf = pishkarDb['taxe'].find_one({'username':username,'year':yearBasePay})
                if act == None: return json.dumps({'replay':False,'msg':f'کارکرد {fullName} ثبت نشده است'})
                if salary == None: return json.dumps({'replay':False,'msg':'سطوح پایه برای سال انتخابی ثبت نشده است'})
                if taxDf == None: return json.dumps({'replay':False,'msg':'سطوح معافیت های مالیاتی برای سال انتخابی ثبت نشده است'})
                fixPay['base'] = int(act['act']) * int(salary['daily'])
                paydate = datetime.datetime.fromtimestamp(int(data['period']['date'])/1000)
                employmentDate = datetime.datetime.fromtimestamp(int(c['employment'])/1000)
                fixPay['payeSanavat'] = (int((paydate - employmentDate).days/365))
                fixPay['sanavat'] = int(act['act']) * round(int(salary['sanavat'])*fixPay['payeSanavat'])
                fixPay['subsidy'] = round(int(act['act'])/30) * int(salary['subsidy'])
                fixPay['homing'] = round(int(act['act'])/30) * int(salary['homing'])
                fixPay['childern'] = round(((int(act['act'])/30) * int(salary['childern'])) * int(c['childern']))
                fixPay['eydi'] = round((fixPay['base']+fixPay['sanavat'])/6)
                fixPay['insurance'] = round((fixPay['base']+fixPay['sanavat']+fixPay['subsidy']+fixPay['homing'])*0.07)
                inTax = (fixPay['base'] + fixPay['sanavat'] + fixPay['subsidy'] + fixPay['childern']) - fixPay['insurance']
                taxDf = pd.DataFrame.from_dict({'level':[taxDf['incomeLevel1'],taxDf['incomeLevel2'],taxDf['incomeLevel3'],taxDf['incomeLevel4'],taxDf['incomeLevel5'],taxDf['incomeLevel6'],taxDf['incomeLevel7']],'rate':[taxDf['taxeLevel1'],taxDf['taxeLevel2'],taxDf['taxeLevel3'],taxDf['taxeLevel4'],taxDf['taxeLevel5'],taxDf['taxeLevel6'],taxDf['taxeLevel7']]})
                taxDf = taxDf.replace('',0)
                taxDf['level'] = [int(x) for x in taxDf['level']]
                taxDf['rate'] = [int(x)/100 for x in taxDf['rate']]
                taxDf = taxDf[taxDf>0].dropna()
                if len(taxDf) == 0 : fixPay['taxe'] = 0
                else:
                    taxDf['inTax'] = inTax - taxDf['level']
                    taxDf = taxDf[taxDf['inTax']>0]
                    taxDf['taxe'] = taxDf['inTax'] * taxDf['rate']
                    fixPay['taxe'] = round(taxDf['taxe'].sum())
            else:
                fixPay = {'insurance':0,'taxe':0,'base':0,'payeSanavat':0,'sanavat':0,'subsidy':0,'homing':0,'childern':0,'eydi':0}
            fixPay['fullName'] = fullName
            fixPay['nationalCode'] = c['nationalCode']
            fixPay['freeTaxe'] = fixPay['taxe'] * int(c['freetaxe'])/100
            dfFees = pd.DataFrame(pishkarDb['Fees'].find({'username':username,'UploadDate':data['period']['Show']},{'_id':0,'username':0}))
            dfFees = dfFees.drop_duplicates(subset=['شماره بيمه نامه','کد رایانه صدور','كارمزد قابل پرداخت']).set_index('شماره بيمه نامه')
            dfFees['تاریخ صدور بیمه نامه'] = [timedate.diffTime2(x) for x in dfFees['تاریخ صدور بیمه نامه']]
            dfFees = dfFees[dfFees['تاریخ صدور بیمه نامه']<=1825]
            assing = pd.DataFrame(pishkarDb['assing'].find({'username':username,'consultant':c['nationalCode']},{'_id':0,'username':0}))
            if len(assing)==0: fixPay['reward'] = 0
            else:
                dfFees = dfFees.join(assing.set_index('شماره بيمه نامه'),how='left')
                dfFees = dfFees.dropna(subset=['consultant'])

                if len(dfFees)==0: fixPay['reward'] = 0
                else:
                    dfFees['feild'] = dfFees['رشته'].fillna('')+' ('+dfFees['مورد بیمه'].fillna('')+')'
                    dfFees['feild'] = [str(x).replace(' ()','') for x in dfFees['feild']]
                    dfFees['feeRate'] = [feeRate(x,c) for x in dfFees['feild']]
                    dfFees['fee'] = dfFees['feeRate'] * dfFees['كارمزد قابل پرداخت']
                    fixPay['reward'] = round(dfFees['fee'].sum())
            df = df.append(fixPay,ignore_index=True)
            try:print(fixPay['fullName'])
            except:
                print(c)
                time.sleep(10000)
        print(df)
        return json.dumps({'replay':True,'pay':df.to_dict(orient='records')})
    else:
        return ErrorCookie()

    
def perforator(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        assing = pd.DataFrame(pishkarDb['assing'].find({'username':username,'consultant':data['nationalCode']},{'_id':0,'username':0}))
        if len(assing)==0:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه نامه ای یافت نشد'})
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username,'UploadDate':data['date']['Show']}))
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه نامه ای یافت نشد'})
        df = df.drop_duplicates(subset=['شماره بيمه نامه','کد رایانه صدور','كارمزد قابل پرداخت']).set_index('شماره بيمه نامه')
        df['diffTime'] = [timedate.diffTime2(x) for x in df['تاریخ صدور بیمه نامه']]
        df = df[df['diffTime']<=1825]
        df = df.join(assing.set_index('شماره بيمه نامه'),how='left').reset_index()
        df = df.dropna(subset=['consultant'])
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه نامه ای یافت نشد'})
        df['feild'] = df['رشته'].fillna('')+' ('+df['مورد بیمه'].fillna('')+')'
        df['feild'] = [str(x).replace(' ()','') for x in df['feild']]
        consultant = pishkarDb['cunsoltant'].find_one({'username':username,'nationalCode':data['nationalCode']},{'_id':0})
        df['feeRate'] = [feeRate(x,consultant) for x in df['feild']]
        df['fee'] = df['feeRate'] * df['كارمزد قابل پرداخت']
        df['fee'] = [round(x) for x in df['fee']]
        df = df[['تاریخ صدور بیمه نامه','رشته','بيمه گذار','comp','شماره بيمه نامه','fee']]
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()