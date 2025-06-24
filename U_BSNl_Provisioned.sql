WITH master AS (
  SELECT 
    reg_platform,
    COUNT(DISTINCT CASE 
      WHEN DATE(FROM_UNIXTIME(created_at)) <= CURDATE() - INTERVAL 1 DAY 
      THEN uuid 
    END) AS users_till_yesterday_date,

    COUNT(DISTINCT CASE 
      WHEN DATE(FROM_UNIXTIME(created_at)) <= CURDATE() 
      THEN uuid 
    END) AS users_till_today_date,

    COUNT(DISTINCT CASE 
      WHEN DATE(FROM_UNIXTIME(created_at)) <= CURDATE() 
      THEN uuid 
    END) - 
    COUNT(DISTINCT CASE 
      WHEN DATE(FROM_UNIXTIME(created_at)) <= CURDATE() - INTERVAL 1 DAY 
      THEN uuid 
    END) AS difference,

    ROUND(
      CASE 
        WHEN COUNT(DISTINCT CASE 
          WHEN DATE(FROM_UNIXTIME(created_at)) <= CURDATE() - INTERVAL 1 DAY 
          THEN uuid 
        END) = 0 THEN NULL
        ELSE (
          (
            COUNT(DISTINCT CASE 
              WHEN DATE(FROM_UNIXTIME(created_at)) <= CURDATE() 
              THEN uuid 
            END) - 
            COUNT(DISTINCT CASE 
              WHEN DATE(FROM_UNIXTIME(created_at)) <= CURDATE() - INTERVAL 1 DAY 
              THEN uuid 
            END)
          ) * 100.0 / 
          COUNT(DISTINCT CASE 
            WHEN DATE(FROM_UNIXTIME(created_at)) <= CURDATE() - INTERVAL 1 DAY 
            THEN uuid 
          END)
        )
      END, 2
    ) AS growth_percentage_date
  FROM users
  WHERE reg_platform IN ('BBNL', 'BSNL')
  GROUP BY reg_platform
),

m1 AS ( 
  SELECT * FROM master 
  UNION ALL
  SELECT 
    'Total' AS reg_platform, 
    SUM(users_till_yesterday_date), 
    SUM(users_till_today_date), 
    SUM(users_till_today_date) - SUM(users_till_yesterday_date), 
    ROUND(
      (
        (SUM(users_till_today_date) - SUM(users_till_yesterday_date)) * 100.0
      ) / NULLIF(SUM(users_till_yesterday_date), 0),
      2
    )
  FROM master
)

SELECT * FROM m1
ORDER BY users_till_yesterday_date ASC
