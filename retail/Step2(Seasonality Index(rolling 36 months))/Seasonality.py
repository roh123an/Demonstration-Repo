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
sql = cfg.seasonalitysql
retailcursor.execute(sql)

ProductSeasonality =retailcursor.fetchall()

seasonalitydf = pd.DataFrame(ProductSeasonality )
seasonalitydf.columns = cfg.seasoanlityColumns
print(seasonalitydf)