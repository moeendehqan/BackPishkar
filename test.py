import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']
dop = 0
df = pd.read_excel('Book1.xlsx')[['شماره بيمه نامه','gfhghf']]
df = df[df['gfhghf']==4420542934]
for i in df.index:
    num = df['شماره بيمه نامه'][i]
    consultant = df['gfhghf'][i]
    pishkarDb['assing'].update_one({'شماره بيمه نامه':str(num)},{'$set':{'consultant':str(consultant)}})
    print(num)
    print(i)



#for i in df.index:
#    dff =pishkarDb['pishkar'].find_one({'شماره بيمه نامه':str(df['شماره بيمه نامه'].iloc[i])})
#    print(dff)
#    if dff!=None:
#        dop = dop +1
#print(dop)