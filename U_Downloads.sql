WITH flat_downloads AS (
    SELECT 
        REPLACE(k, '_', ' ') AS platform,
        CAST(v AS BIGINT) AS downloads,
        DATE(modified_at) AS record_date
    FROM analyticsdatabase.Pb_meta_information
    CROSS JOIN UNNEST(
        CAST(json_parse(meta_value) AS map<VARCHAR, VARCHAR>)
    ) AS t (k, v)
    WHERE meta_name = 'APP_DOWNLOADS'
),
pivoted AS (
    SELECT
        platform,
        COALESCE(MAX(CASE WHEN record_date = CURRENT_DATE - INTERVAL '1' DAY THEN downloads END), 0) AS downloads_till_yesterday,
        COALESCE(MAX(CASE WHEN record_date = CURRENT_DATE THEN downloads END), 0) AS downloads_till_today
    FROM flat_downloads
    WHERE record_date IN (CURRENT_DATE, CURRENT_DATE - INTERVAL '1' DAY)
    GROUP BY platform
),
filtered AS (
    SELECT * FROM pivoted
    WHERE platform NOT IN ('Roku', 'Vidaa')
)

-- Final SELECT with UNION for TOTAL
SELECT
    platform,
    downloads_till_yesterday,
    downloads_till_today,
    downloads_till_today - downloads_till_yesterday AS growth_count,
    ROUND(
        CASE 
            WHEN downloads_till_yesterday = 0 THEN NULL
            ELSE (downloads_till_today - downloads_till_yesterday) * 100.0 / downloads_till_yesterday
        END,
        2
    ) AS growth_percentage
FROM filtered

UNION ALL

SELECT
    'Total' AS platform,
    SUM(downloads_till_yesterday),
    SUM(downloads_till_today),
    SUM(downloads_till_today) - SUM(downloads_till_yesterday),
    ROUND(
        CASE
            WHEN SUM(downloads_till_yesterday) = 0 THEN NULL
            ELSE (SUM(downloads_till_today) - SUM(downloads_till_yesterday)) * 100.0 / SUM(downloads_till_yesterday)
        END,
        2
    ) AS growth_percentage
FROM filtered

ORDER BY 
    CASE WHEN platform = 'Total' THEN 1 ELSE 0 END,  -- Push 'Total' to bottom
    growth_count DESC
