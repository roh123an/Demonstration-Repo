#This is config file

host = "localhost"
user = "root"
passwd = "test123"
database = "retail"
seasonalitysql = """SELECT ma.product_id, ma.month_ts, ma.monthly_avg/oa.overall_avg as seasonality_index
                     FROM
                        (SELECT product_id, AVG(monthly_sale) as overall_avg,tran_dt FROM 
                        product_year_mon_sale
                         WHERE tran_dt < current_date() AND tran_dt >= date_sub(current_date(), INTERVAL 2 YEAR)
                        GROUP BY product_id
                         ) oa
                         JOIN
                        (SELECT product_id, month_ts, AVG(monthly_sale) as monthly_avg,tran_dt
                          FROM product_year_mon_sale 
                          WHERE tran_dt < current_date() AND tran_dt >= date_sub(current_date(), INTERVAL 2 YEAR)
                          GROUP BY product_id, month_ts
                        ) ma
                    ON ma.product_id = oa.product_id """

seasoanlityColumns =['product_id','month_ts','Seasonality']


Affinitysql =""" select cmb.product , cmb.rel_product ,cmb.num_basket,
                    cmb.num_basket /pd.num_basket as affinity_score
                    from
                    (SELECT 
                    td.product_id as product,
                    rel_td.product_id as rel_product,
                    count(*) as  num_basket 
                    FROM tran_dtl td
                    JOIN tran_dtl rel_td ON td.tran_id = rel_td.tran_id
                    GROUP BY td.product_id, rel_td.product_id) cmb
                    join
                    (select product_id , count(*) as num_basket 
                    from tran_dtl td 
                    group by product_id)pd
                    on pd.product_id = cmb.product;  """

AffinityColumns=['product','rel_product','num_basket','affinity_score']

recommendsql = """SELECT rc.member_id, rc.rel_product, rc.reward_score*rc.affinity_score as recommend_score
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
                    mp.product_id IS NULL;  """

recommendScoreColumns = ['member_id','rel_product','recommend_score']