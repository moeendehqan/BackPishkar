import json
import pymongo
import crypto
import timedate
from flask import send_file
from captcha import captchaGenerate
import base64

client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def login(data):
    phone = data['phone']
    if pishkarDb['username'].find_one({'phone':phone})==None:
        if pishkarDb['sub'].find_one({'subPhone':phone})==None:
            regDate = str(timedate.toDay())
            limDate = str(timedate.deltaTime(30))
            pishkarDb['username'].insert_one({'phone':phone, 'name':'', 'email':'', 'address':'', 'phonework':'', 'company':'','registerdate':regDate, 'limitDate':limDate, 'management':True})
        else:
            sub = pishkarDb['sub'].find_one({'subPhone':phone})
            return json.dumps({'replay':True,'Cookie':crypto.encrypt(sub['username']).decode()})
    return json.dumps({'replay':True,'Cookie':crypto.encrypt(phone).decode()})

def cookie(data):
    try:cookie = str(data['cookie']).encode()
    except:cookie = str(data).encode()
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
    


def captcha(data):
    cg = captchaGenerate()
    return json.dumps({'cptcha':cg[0],'img':cg[1]})