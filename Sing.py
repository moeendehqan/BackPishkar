import json
import pymongo
import crypto
import timedate

client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def login(data):
    phone = data['phone']
    if pishkarDb['username'].find_one({'phone':phone})==None:
        regDate = str(timedate.toDay())
        limDate = str(timedate.deltaTime(30))
        pishkarDb['username'].insert_one({'phone':phone, 'name':'', 'email':'', 'address':'', 'phonework':'', 'company':'','registerdate':regDate, 'limitDate':limDate, 'management':True})
    return json.dumps({'replay':True,'Cookie':crypto.encrypt(phone).decode()})

def cookie(data):
    cookie = str(data['cookie']).encode()
    cookie = crypto.decrypt(cookie)
    user = pishkarDb['username'].find_one({'phone':cookie},{'_id':0})
    if user==None:
        return json.dumps({'replay':False})
    else:
        limAccunt = timedate.diffTime(user['limitDate'])
        user['limAccunt'] = limAccunt
        return json.dumps({'replay':True,'user':user})

def ErrorCookie():
    return json.dumps({'replay':False,'msg':'خطا در شناسایی کاربر لطفا مجدد تلاش کنید'})
    