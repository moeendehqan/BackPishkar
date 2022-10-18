import json
from flask import Flask, request
import pymongo
import pandas as pd
import warnings
from flask_cors import CORS
import management
import Sing
import sms
import timedate
import feesreports
import assing

warnings.filterwarnings("ignore")
client = pymongo.MongoClient()
app = Flask(__name__)
CORS(app)


@app.route('/sing/verificationphone',methods = ['POST', 'GET'])
def verificationphone():
    data = request.get_json()
    return sms.VerificationPhone(data)

@app.route('/sing/login',methods = ['POST', 'GET'])
def login():
    data = request.get_json()
    return Sing.login(data)

@app.route('/sing/cookie',methods = ['POST', 'GET'])
def cookie():
    data = request.get_json()
    return Sing.cookie(data)

#----------------- Management -----------------
@app.route('/management/profile',methods = ['POST', 'GET'])
def management_profile():
    data = request.get_json()
    return management.profile(data)

@app.route('/management/cunsoltant',methods = ['POST', 'GET'])
def management_cunsoltant():
    data = request.get_json()
    return management.cunsoltant(data)

@app.route('/management/getcunsoltant',methods = ['POST', 'GET'])
def management_getcunsoltant():
    data = request.get_json()
    return management.getcunsoltant(data)

@app.route('/management/delcunsoltant',methods = ['POST', 'GET'])
def management_delcunsoltant():
    data = request.get_json()
    return management.delcunsoltant(data)

@app.route('/management/addinsurer',methods = ['POST', 'GET'])
def management_addinsurer():
    data = request.get_json()
    return management.addinsurer(data)

@app.route('/management/getinsurer',methods = ['POST', 'GET'])
def management_getinsurer():
    data = request.get_json()
    return management.getinsurer(data)

@app.route('/management/delinsurer',methods = ['POST', 'GET'])
def management_delinsurer():
    data = request.get_json()
    return management.delinsurer(data)

#----------------- General -----------------
@app.route('/general/today',methods = ['POST', 'GET'])
def general_today():
    return json.dumps({'today':str(timedate.toDay())})
#----------------- feesreports -----------------
@app.route('/feesreports/uploadfile',methods = ['POST', 'GET'])
def feesreports_uploadfile():
    date = request.form['date']
    cookie = request.form['cookie']
    file =  request.files['feesFile']
    comp = request.form['comp']
    return feesreports.uploadfile(date,cookie,file,comp)

@app.route('/feesreports/getfeesuploads',methods = ['POST', 'GET'])
def feesreports_getfeesuploads():
    data = request.get_json()
    return feesreports.getfeesuploads(data)

@app.route('/feesreports/delupload',methods = ['POST', 'GET'])
def feesreports_delupload():
    data = request.get_json()
    return feesreports.delupload(data)

@app.route('/feesreports/getinsurer',methods = ['POST', 'GET'])
def feesreports_getinsurer():
    data = request.get_json()
    return feesreports.getinsurer(data)
#----------------- assing -----------------
@app.route('/assing/get',methods = ['POST', 'GET'])
def assing_get():
    data = request.get_json()
    return assing.get(data)

@app.route('/assing/getinsurnac',methods = ['POST', 'GET'])
def assing_getinsurnac():
    data = request.get_json()
    return assing.getinsurnac(data)

@app.route('/assing/set',methods = ['POST', 'GET'])
def assing_set():
    data = request.get_json()
    return assing.set(data)

if __name__ == '__main__':
    #from waitress import serve
    #serve(app, host="0.0.0.0", port=8080)
    app.run(host='0.0.0.0', debug=True)