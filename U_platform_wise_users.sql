WITH master AS (
  SELECT
    qq.device_type,
   
    qq.yesterday_user_count,
     qq.total_user_count AS users,
    (qq.total_user_count - IFNULL(qq.yesterday_user_count, 0)) AS growth_count,
    ROUND(
      (
        (qq.total_user_count - IFNULL(qq.yesterday_user_count, 0)) /
        NULLIF(qq.yesterday_user_count, 0) * 100
      ),
      2
    ) AS growth_percentage
  FROM (
    SELECT
      ud.device_type,
     
      COUNT(DISTINCT CASE
        WHEN date(from_unixtime(u.created_at)) <= date_format(current_date - interval '1' day, '%Y-%m-%d')  THEN CONCAT_WS('_', ud.device_type, COALESCE(u.mobile, ''), COALESCE(u.email, ''))
      END) AS yesterday_user_count , 
       COUNT(DISTINCT CONCAT_WS('_', ud.device_type, COALESCE(u.mobile, ''), COALESCE(u.email, ''))) AS total_user_count
    FROM users AS u
    JOIN user_devices AS ud ON ud.user_id = u.uuid
    WHERE u.status = 0
      AND ud.device_type <> 0
      AND ud.device_type IN (1, 2, 3, 4, 5, 7, 8, 9, 10)
    GROUP BY ud.device_type
  ) AS qq
) , 

main as ( 

SELECT  
  CASE
    WHEN device_type = 1 THEN 'Android'
    WHEN device_type = 2 THEN 'iOS'
    WHEN device_type = 3 THEN 'Website'
    WHEN device_type = 4 THEN 'Android TV'
    WHEN device_type = 5 THEN 'Fire TV Stick'
    WHEN device_type = 7 THEN 'Apple TV'
    WHEN device_type = 8 THEN 'LG'
    WHEN device_type = 9 THEN 'Samsung'
    WHEN device_type = 10 THEN 'Jio TV'
    ELSE 'Unknown'
  END AS Platform,
 
  yesterday_user_count,
   users,
  growth_count,
  growth_percentage
FROM master

UNION ALL

SELECT
  'Total' AS Platform,
  
  SUM(yesterday_user_count),
  SUM(users),
  SUM(growth_count),
  ROUND(
    CASE 
      WHEN SUM(yesterday_user_count) = 0 THEN NULL
      ELSE SUM(growth_count) * 100.0 / SUM(yesterday_user_count)
    END,
    2
  ) AS total_growth_percentage
FROM master
) 
select * from main order by users asc