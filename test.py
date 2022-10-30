import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']
dop = 0
df = pd.read_excel('Boeeok1.xlsx')
print(df)



dff =pishkarDb['pishkar'].find_one({'شماره بيمه نامه':str('0104003360035210')})

print(dff)

#for i in df.index:
#    dff =pishkarDb['pishkar'].find_one({'شماره بيمه نامه':str(df['شماره بيمه نامه'].iloc[i])})
#    print(dff)
#    if dff!=None:
#        dop = dop +1
#print(dop)