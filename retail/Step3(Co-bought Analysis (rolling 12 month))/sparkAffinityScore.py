import configFile as cfg
from pyspark.sql import SparkSession


appName = "PySpark MySQL Example - via mysql.connector"
master = "local"

spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

retail_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/retail") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tran_dtl") \
    .option("user", "root").option("password", "test123").load()

# retail_df.show(10,False)

retail_df.createOrReplaceTempView("tran_dtl")
spark.sql("""SELECT rc.member_id, rc.rel_product, rc.reward_score*rc.affinity_score as recommend_score
                    FROM 
                    (SELECT rp.member_id, rp.product_id,rp.reward_score, afs.rel_product, afs.affinity_score
                    FROM
                    (SELECT member_id, product_id, reward_score
                    FROM 
                    (SELECT member_id, product_id, reward_score,
                    row_number() OVER (partition by member_id order by reward_score desc) as score_rank
                    FROM member_reward_score) sc_r
                    WHERE sc_r.score_rank <=10) rp
                    JOIN
                    product_affinity_score afs 
                    ON rp.product_id = afs.product) rc
                    LEFT JOIN
                    (SELECT th.member_id, td.product_id, count(*) as trip_count
                    FROM tran_dtl td JOIN tran_hdr th ON td.tran_id = th.tran_id
                    -- WHERE year(td.tran_dt) = 2020 AND year(th.tran_dt) = 2020
                    GROUP BY th.member_id, td.product_id) mp
                    ON rc.member_id = mp.member_id AND rc.rel_product = mp.product_id
                    WHERE
                    mp.product_id IS NULL;  """).show(truncate=False)