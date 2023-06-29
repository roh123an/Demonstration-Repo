import os
from datetime import(
    datetime as DateTime,
    date as Date,
    time as Time,
    timedelta as TimeDelta
)
l = []
path =r"C:\Users\User\PycharmProjects\tran_hdr\\"
for curdir, subdir, files in os.walk(path):
    #print(curdir)
    #print(files)
    for fileName in files:
        hdr1,hdr2,file = fileName.strip().split("_")
        dateStr,type = file.strip().split(".")
        #print(dateStr)
        l.append(dateStr)

thisDate = Date.today()
currentDate = thisDate.strftime("%Y-%m-%d")
#print(currentDate)

if len(l) == 0:
    print("directory is empty")
else:
    for dt in l:
        if (dt != currentDate):
            print("File is old")
        else:
            date_ts = dt
            print("currentDate is: ",date_ts)






