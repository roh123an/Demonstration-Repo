import configFile as cfg
from pyspark.sql import SparkSession


appName = "PySpark MySQL Example - via mysql.connector"
master = "local"

spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

retail_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/retail") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "product_year_mon_sale") \
    .option("user", "root").option("password", "test123").load()

# retail_df.show(10,False)

retail_df.createOrReplaceTempView("product_year_mon_sale")
spark.sql("""SELECT ma.product_id, ma.month_ts, ma.monthly_avg/oa.overall_avg as seasonality_index
                     FROM
                        (SELECT product_id, AVG(monthly_sale) as overall_avg,tran_dt FROM 
                        product_year_mon_sale
                         WHERE tran_dt < current_date() AND tran_dt >= date_sub(current_date(),730)
                        GROUP BY product_id,tran_dt
                         ) oa
                         JOIN
                        (SELECT product_id, month_ts, AVG(monthly_sale) as monthly_avg,tran_dt
                          FROM product_year_mon_sale 
                          WHERE tran_dt < current_date() AND tran_dt >= date_sub(current_date(), 730)
                          GROUP BY product_id, month_ts,tran_dt
                        ) ma
                    ON ma.product_id = oa.product_id """).show(truncate=False)