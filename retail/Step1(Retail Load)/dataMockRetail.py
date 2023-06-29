# Program to do data mockup for retail
# This program simulates retail transactions per day and saves it to csv files (tran_hdr_<date> and tran_dtl_<date>).
# A data pipeline is developed to move these transactions to respective databases.
import random
import pandas as pd
import mysql.connector
from datetime import(
    datetime as DateTime,
    date as Date,
    time as Time,
    timedelta as TimeDelta
)
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="test123",
    database="retail"
)
mycursor = db_connection .cursor()

memberFilePath = r"C:\Users\User\Downloads\member_dim.csv"
tranHdrFilePath = r"C:\Users\User\PycharmProjects\tran_hdr\\"

def addValueListToDict(dDict,key,value):
    if (key in dDict):
        valueList = dDict[key]
        valueList.append(value)
        dDict[key] = valueList
    else:
        valueList = []
        valueList.append(value)
        dDict[key] = valueList

def getTuple(row):
    tran_dt = row['tran_dt']
    tran_id = str(row['tran_id'])
    store_id = int(row['store_id'])
    member_id= int(row['member_id'])
    myTuple = (tran_id,store_id,member_id,tran_dt)
    return  myTuple


def writeTransactionsForStore(storeDict,fp):
    for store in storeDict.keys():
        dateTimeTs = DateTime.now()
        dateTimeStr = dateTimeTs.strftime("%Y-%m-%dT%H:%M:%S")
        tranCount = 1
        memList = storeDict[store]
        #print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
        #print(memList)
        #print("LIST with Skip Index::: ")
        if (flag == 0):
            for mem in memList[0::2]:
                num = random.randint(0, 3)
                if (num < 3):
                    num = 1
                    tranId = dateTimeStr+ "_" + str(tranCount)
                    tranCount += 1
                    tranStr = tranId + "," + store + "," + mem + "," + dateStr + "\n"
                    fp.write(tranStr)
                #print(mem, end=",")
        else:
            for mem in memList[1::2]:
                num = random.randint(0, 1)
                if (num == 1):
                    num = 1
                    tranId = dateTimeStr + str(tranCount)
                    tranCount += 1
                    tranStr = tranId + "," + store + "," + mem + "," + dateStr + "\n"
                    fp.write(tranStr)


regStoreDict = {}
prefStoreDict = {}
#pMemberList = []
#rMemeberList = []
count=0
for line in open(memberFilePath):
    if(count==0):
        count+=1
        continue
    print(line)
    memId,firstName,lastName,regStore,prefStore,regDate = line.strip().split(",")
    addValueListToDict(regStoreDict,regStore,memId)
    addValueListToDict(prefStoreDict,prefStore,memId)


### strategy 1: for every store odd list index will purchase on odd days
dateTimeTs = DateTime.now()
dateTimeStr = dateTimeTs.strftime("%Y-%m-%dT%H:%M:%S")
print("%$%#$#$#$#::::: ", dateTimeStr)
dateTs = dateTimeTs.date()
dateStr = dateTs.strftime("%Y-%m-%d")
print(dateStr)
year,month,day = dateStr.strip().split("-")
day = int(day)
if (day % 2 == 0):
    flag = 0
else:
    flag = 1

tranHdrFilePath = tranHdrFilePath + "tran_hdr_" + dateStr + ".csv"

f = open(tranHdrFilePath,"a")
writeTransactionsForStore(regStoreDict,f)
writeTransactionsForStore(prefStoreDict,f)

f.close()
tranhdrdata = pd.read_csv(tranHdrFilePath )
tranhdrdata.columns =["tran_id", "store_id", "member_id", "tran_dt"]

val = []
for index, row in tranhdrdata.iterrows():
        hdrTuple = getTuple(row)
        val.append(hdrTuple )
       # print(val)
sql="Insert into tran_hdr_file " \
            "(tran_id,store,member_id,tran_dt) " \
            "VALUES(%s,%s,%s,%s)"


mycursor.executemany(sql,val)
db_connection.commit()
print(mycursor.rowcount,"record inserted")


