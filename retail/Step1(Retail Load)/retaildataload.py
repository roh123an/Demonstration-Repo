import os.path

import mysql.connector
import random

import pandas as pd

import browseFolder as bf
import smtplib

#Changing pandas date  format to object which is required for mysql
def getTuple(row):
    tran_dt = row['tran_dt']
    tran_id = str(row['tran_id'])
    product_id = int(row['product_id'])
    amt = float(row['amt'])
    qty = int(row['qty'])
    myTuple = (tran_id, product_id, qty, amt, tran_dt)
    return myTuple

db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="test123",
    database="retail"
)
mycursor = db_connection .cursor()
path=r"C:\Users\User\PycharmProjects\tran_hdr\tran_hdr_"+ str(bf.dateStr) +".csv"
outPath = r"C:\Users\User\PycharmProjects\tran_dtl\tran_dtl_"+ str(bf.dateStr) + ".csv"
if os.path.exists(outPath):
    print("File already exist")
else:
    my_database = db_connection.cursor()
    sql_statement = "SELECT product_id,price FROM product"
    my_database.execute(sql_statement)
    productData = my_database.fetchall()

    prodDict = {}
    for prodTuple in productData:
        print(prodTuple)
        prodDict[prodTuple[0]] = prodTuple[1]
    print("&&&&&&&&&&&&&&&&&&&&&&&&&&&& printing dictionary: \n")
    print("DICT: ", prodDict)

    ##### step 1 read tran hdr file
    # pathDateFile = r"C:\Users\User\PycharmProjects\retailProject\datesToBeProcessed.csv"
    #
    # for line in open(pathDateFile):
    #         date_ts = line.strip()
    f = open(outPath, "a")  # a is use for append
    for line in open(path):
        tranId, storeId, memberId, tran_dt = line.strip().split(",")
        # print(line)
        tranSize = random.randint(1, 20)
        print(tranSize)
        for num in range(tranSize):
            productId = random.randint(1, 100)
            print(productId)
            while (1):
                if productId in prodDict:
                    break
                productId = random.randint(1, 100)

            price = prodDict[productId]
            qty = random.randint(1, 10)
            amt = price * qty
            outStr = tranId + "," + str(productId) + "," + str(qty) + "," + str(amt) + "," + str(tran_dt) + "\n"
            f.write(outStr)

    f.close()

    message = 'For ' + str(bf.date_ts) + " retail file has generated succesfully"
    client_mailid ='mandar.kale@philomathpl.com'
    test_mailid = 'amar.dhane@philomathpl.com'
    server = smtplib.SMTP('smtp.gmail.com', 587)

    server.starttls()

    server.login('anupam465mishra@gmail.com', 'lzfyirpitawiehdl')

    server.sendmail('anupam465mishra@gmail.com',client_mailid , message)
    print('Mail sent')

dtldata = pd.read_csv(outPath)
dtldata.columns =["tran_id", "product_id", "qty", "amt", "tran_dt"]
print(dtldata)
print(dtldata.columns)

val = []
for index, row in dtldata.iterrows():
        dtlTuple = getTuple(row)
        val.append(dtlTuple)
       # print(val)
sql="Insert into tran_dtl_file " \
            "(tran_id, product_id, qty, amt, tran_dt) " \
            "VALUES(%s,%s,%s,%s,%s)"


mycursor.executemany(sql,val)
db_connection.commit()
print(mycursor.rowcount,"record inserted")










