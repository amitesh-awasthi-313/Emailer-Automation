WITH master AS (
  SELECT 
    reg_platform,

    COUNT(DISTINCT CASE 
      WHEN DATE(FROM_UNIXTIME(created_at)) = CURDATE() - INTERVAL 1 DAY 
      THEN uuid 
    END) AS users_registered_yesterday,

    COUNT(DISTINCT CASE 
      WHEN DATE(FROM_UNIXTIME(created_at)) = CURDATE() 
      THEN uuid 
    END) AS users_registered_today

  FROM users
  WHERE reg_platform IN ('BBNL', 'BSNL')
  GROUP BY reg_platform
),

m1 AS (
  SELECT * FROM master
  UNION ALL
  SELECT 
    'Total' AS reg_platform,
    SUM(users_registered_yesterday) AS users_registered_yesterday,
    SUM(users_registered_today) AS users_registered_today
  FROM master
)

SELECT 
  *,
  users_registered_today - users_registered_yesterday AS difference,
  ROUND(
    CASE 
      WHEN users_registered_yesterday = 0 THEN NULL
      ELSE (users_registered_today - users_registered_yesterday) * 100.0 / users_registered_yesterday
    END,
    2
  ) AS growth_percentage
FROM m1
ORDER BY users_registered_yesterday , reg_platform ASC
