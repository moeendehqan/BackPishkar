
from persiantools.jdatetime import JalaliDate
import datetime


def toDay():
    now = datetime.datetime.now()
    return JalaliDate(datetime.date(now.year, now.month, now.day))

def deltaTime(toDay):
    now = datetime.datetime.now()
    delta = now + datetime.timedelta(toDay)
    return JalaliDate(datetime.date(delta.year, delta.month, delta.day))


def diffTime(deltaTime):
    now = str(datetime.datetime.now().date())
    t = str(deltaTime).split('-')
    d1 = str(JalaliDate(int(t[0]), int(t[1]), int(t[2])).to_gregorian())
    res = (datetime.datetime.strptime(d1, "%Y-%m-%d") - datetime.datetime.strptime(now, "%Y-%m-%d")).days
    return res

def diffTime2(deltaTime):
    now = str(datetime.datetime.now().date())
    t = str(deltaTime).split('/')
    d1 = str(JalaliDate(int(t[0]), int(t[1]), int(t[2])).to_gregorian())
    res = (datetime.datetime.strptime(now, "%Y-%m-%d") - datetime.datetime.strptime(d1, "%Y-%m-%d")).days
    return res

def timStumpTojalali(timeStump):
    kk = str(JalaliDate.fromtimestamp((int(timeStump)/1000)))
    kk = kk.replace('-','/')
    return kk

def dateToPriod (date):
    intDate = str(date).split('/')
    year = intDate[0]
    mondth = intDate[1].replace('01','اردیبهشت').replace('02','فروردین').replace('03','خرداد').replace('04','تیر').replace('05','مرداد').replace('06','شهریور').replace('07','مهر').replace('08','آبان').replace('09','آذر').replace('10','دی').replace('11','بهمن').replace('12','اسفند')
    return year +' '+mondth

def dateToInt (date):
    intDate = str(date).split('/')
    return int(intDate[0]+intDate[1]+intDate[2])

def intToDate (date):
    intDate = str(date)
    return intDate[0:4]+'/'+intDate[4:6]+'/'+intDate[6:8]


def dateToStandard (date):
    intDate = str(date).split('/')
    if len(intDate[1])==1:
        intDate[1] = '0'+intDate[1]
    if len(intDate[2])==1:
        intDate[2] = '0'+intDate[2]
    return intDate[0]+'/'+intDate[1]+'/'+intDate[2]


def PriodStrToInt (date):
    intDate = str(date).split('-')
    try:
        year = intDate[0]
        mondth = intDate[1].replace('اردیبهشت','01').replace('فروردین','02').replace('خرداد','03').replace('تیر','04').replace('مرداد','05').replace('شهریور','06').replace('مهر','07').replace('آبان','08').replace('آذر','09').replace('دی','10').replace('بهمن','11').replace('اسفند','12')
        dataInt = (year+mondth).replace(' ','')
        dataInt = int(dataInt)
        return dataInt
    except:
        return date
