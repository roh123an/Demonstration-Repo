import mysql.connector
import pandas as pd
import configFile as cfg
db_connection = mysql.connector.connect(
    host=cfg.host,
    user=cfg.user,
    passwd=cfg.passwd,
    database=cfg.database
)
retailcursor = db_connection.cursor()
sql= cfg.recommendsql

retailcursor.execute(sql)

recommendScore =retailcursor.fetchall()

recommendScoredf = pd.DataFrame(recommendScore )
# print(recommendScoredf  )
recommendScoredf.columns = cfg.recommendScoreColumns
print(recommendScoredf )