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
sql = cfg.Affinitysql
retailcursor.execute(sql)

AffinityScore =retailcursor.fetchall()

Affinitydf = pd.DataFrame(AffinityScore )
# print(Affinitydf )
Affinitydf.columns = cfg.AffinityColumns
print(Affinitydf)