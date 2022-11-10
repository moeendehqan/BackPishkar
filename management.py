import json
from unicodedata import name
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import timedate

client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def NcToName(nc,username):
    name = pishkarDb['cunsoltant'].find_one({'username':username,'nationalCode':nc})
    if name != None:
        name = name['fristName'] + ' ' +name['lastName']
        return name
    else:
        return ''

def profile(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        user = user['user']
        useNew = data['userNew']
        pishkarDb['username'].update_many({'phone':user['phone']},{'$set':useNew},upsert=False)
        return json.dumps({'replay':True,'msg':'تغییرات با موفقیت ثبت شد'})
    else:
        return ErrorCookie()


def cunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        cheakNC = pishkarDb['cunsoltant'].find_one({'nationalCode':data['cunsoltant']['nationalCode']})
        cheakP = pishkarDb['cunsoltant'].find_one({'phone':data['cunsoltant']['phone']})
        if cheakNC ==None:
            if cheakP == None:
                data['cunsoltant']['username'] = user['user']['phone']
                pishkarDb['cunsoltant'].insert_one(data['cunsoltant'])
                return json.dumps({'replay':True})
            else:
                data['cunsoltant']['username'] = user['user']['phone']
                pishkarDb['cunsoltant'].update_many({'username':username,'phone':data['cunsoltant']['phone']},{'$set':data['cunsoltant']})
                return json.dumps({'replay':True})
        else:
            data['cunsoltant']['username'] = user['user']['phone']
            pishkarDb['cunsoltant'].update_many({'username':username,'nationalCode':data['cunsoltant']['nationalCode']},{'$set':data['cunsoltant']})
            return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username},{'_id':0,'fristName':1,'lastName':1,'nationalCode':1,'gender':1,'phone':1,'salary':1,'childern':1,'freetaxe':1,'salaryGroup':1,'insureWorker':1,'insureEmployer':1}))
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ مشاوری تعریف نشده'})
        df = df.fillna('')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()

def delcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        allowDel = pishkarDb['assing'].find_one({'username':username,'nationalCode':data['nationalCode']}) ==None
        if allowDel:
            pishkarDb['cunsoltant'].delete_many({'nationalCode':data['nationalCode']})
            return json.dumps({'replay':True})
        else:
            return json.dumps({'replay':False,'msg':'حذف انجام نشد، این مشاور  به بیمه نامه ای تخصیص یافته'})

    else:
        return ErrorCookie()


def addinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        columns = data['insurer']
        columns['username'] = username
        find = pishkarDb['insurer'].find_one({'نام':columns['نام'],'username':username})==None
        if find:
            pishkarDb['insurer'].insert_one(columns)
            return json.dumps({'replay':True,'msg':'ثبت شده'})
        else:
            return json.dumps({'replay':False,'msg':'بیمه گر با این نام قبلا ثبت شده'})
    else:
        return ErrorCookie()

    
def settax(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        lavel = data['level']
        lavel['username'] = username
        if pishkarDb['taxe'].find_one({'username':username,'year':lavel['year']}) == None:
            pishkarDb['taxe'].insert_one(lavel)
        else:
            pishkarDb['taxe'].update_one({'username':username,'year':lavel['year']},{'$set':lavel})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def gettax(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['taxe'].find({'username':username},{'_id':0,'username':0}))
        df = df.replace('',0)
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie() 
    
def deltax(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['taxe'].find_one_and_delete({'username':username,'year':data['year']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie() 
    
def getinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        insurerList = pd.DataFrame((pishkarDb['insurer'].find({'username':username},{'_id':0,'نام':1})))
        insurerList = insurerList.to_dict(orient='records')
        return json.dumps({'replay':True, 'list':insurerList})
    else:
        return ErrorCookie()
    
def delinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['insurer'].delete_many({'username':username,'نام':data['name']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def salary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        salary = data['salary']
        salary['username'] = username
        if pishkarDb['salary'].find_one({'username':username, 'year':salary['year'],'gruop':salary['gruop']})==None:
            pishkarDb['salary'].insert_one(salary)
            return json.dumps({'replay':True , 'msg':'ثبت شده'})
        else:
            pishkarDb['salary'].update_one({'username':username, 'year':salary['year'],'gruop':salary['gruop']},{'$set':salary})
            return json.dumps({'replay':True, 'msg':'بروز رسانی شد'})
    else:
        return ErrorCookie()

def getsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = [x for x in pishkarDb['salary'].find({'username':username},{'_id':0})]
        return json.dumps({'df':df})
    else:
        return ErrorCookie()

def delsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['salary'].delete_one({'username':username, 'year':data['year']})
        return json.dumps({'replat':True})
    else:
        return ErrorCookie()

def setsub(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']

    if user['replay']:
        if pishkarDb['sub'].find_one({'username':username,'subPhone':data['sub']['phone']}) ==None:
            pishkarDb['sub'].insert_one({'username':username,'subPhone':data['sub']['phone'],'allowManagement':data['sub']['allowManagement'],'allowDesk':data['sub']['allowDesk']})
            return json.dumps({'replay':True})
        else:
            return json.dumps({'replay':False})
    else:
        return ErrorCookie()

def getsub(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['sub'].find({'username':username},{'_id':0}))
        if len(df)==0:
            return json.dumps({'replay':False})
        df = df.to_dict(orient='records')

        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()


def getgroupsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = list(pishkarDb['salary'].find({'username':username},{'_id':0,'gruop':1}))
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'هیچ گروه حقوق و دسمتزدی یافت نشد'})
        df = [x['gruop'] for x in df]
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()

def addbranche(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        subList = [x['subconsultant'] for x in data['SubConsultantList']]
        subList = list(set(subList))
        if data['managementBranche'] in subList:
            return json.dumps({'replay':False, 'msg':'مدیر شعبه در مشاوران شعبه تکرار شده است'})
        branche = pd.DataFrame(pishkarDb['branches'].find({}))
        if len(branche)>0:
            brancheSubList = []
            for i in list(branche[branche['BranchesName']!=data['BranchesName']]['SubConsultantList']):
                for j in i:
                    brancheSubList.append(j)
            for i in subList:
                if i in brancheSubList:
                    name = NcToName(i,username)
                    return json.dumps({'replay':False,'msg':f'{name} در شعبه دیگری ثبت شده'})
        if pishkarDb['branches'].find_one({'username':username,'BranchesName':data['BranchesName']})!=None:
            pishkarDb['branches'].update_one({'username':username,'BranchesName':data['BranchesName']},{'$set':{'managementBranche':data['managementBranche'],'SubConsultantList':subList}})
            return json.dumps({'replay':True})
        if pishkarDb['branches'].find_one({'username':username,'management':data['managementBranche']})!=None:
            return json.dumps({'replay':False, 'msg':'مدیر شعبه تکراری است'})
        pishkarDb['branches'].insert_one({'username':username,'BranchesName':data['BranchesName'],'managementBranche':data['managementBranche'],'SubConsultantList':subList})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def getbranche(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['branches'].find({'username':username},{'_id':0}))
        if len(df)>0:
            df['managementBranche'] = [NcToName(x,username) for x in df['managementBranche']]
            for i in df.index:
                if len(df['SubConsultantList'][i])==0:
                    df['SubConsultantList'][i] = ''
                else:
                    listsubi = ''
                    for j in df['SubConsultantList'][i]:
                        listsubi = listsubi + ' , ' +(NcToName(j,username))
                    df['SubConsultantList'][i] = listsubi[3:]

            df = df.to_dict(orient='records')
            return json.dumps({'replay':True,'df':df})
        else:
            return json.dumps({'replay':True,'msg':'هیچ شعبه ای ثبت نشده'})
    else:
        return ErrorCookie()


def delbranche(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dl = pishkarDb['branches'].find_one_and_delete({'username':username,'BranchesName':data['name']})
        if dl != None:
            return json.dumps({'replay':True})
        else:
            return json.dumps({'replay':False,'msg':'شعبه یافت نشد'})
    else:
        return ErrorCookie()

def addminsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pishkarDb['minimumSalary'].find_one({'username':username,'year':data['minimum']['year']})
        if df == None:
            pishkarDb['minimumSalary'].insert_one({'username':username,'year':data['minimum']['year'],'value':data['minimum']['value']})
        else:
            pishkarDb['minimumSalary'].update_one({'username':username,'year':data['minimum']['year']},{'$set':{'value':data['minimum']['value']}})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getminsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = [x for x in pishkarDb['minimumSalary'].find({'username':username},{'_id':0,'username':0})]
        if len(df)==0:
            return json.dumps({'replay':False})
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def delminsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['minimumSalary'].delete_one({'username':username,'year':data['year']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()