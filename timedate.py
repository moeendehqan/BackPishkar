
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

def timStumpTojalali(timeStump):
    print(timeStump)
    kk = str(JalaliDate.fromtimestamp((timeStump/1000))).replace('-','/')
    return kk