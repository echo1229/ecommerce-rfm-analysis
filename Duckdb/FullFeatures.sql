
CREATE TABLE full_user_features AS

WITH t1 AS (
    SELECT 
        user_id,
        MAX(CASE WHEN event_type = 'purchase' THEN event_time ELSE NULL END) AS Recency,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 ELSE NULL END) AS Frequency,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS monetary,
        COUNT(CASE WHEN event_type IN ('view', 'cart') THEN 1 ELSE NULL END) AS Intent
    FROM read_csv_auto('D:\实战项目\Dataanalysis\archive\*.csv')
    GROUP BY user_id
),

t2 AS (
    SELECT
        user_id,
        AVG(session_duration) AS avg_session
    FROM (
        SELECT
            user_id,
            user_session,
            (MAX(event_time) - MIN(event_time)) AS session_duration
        FROM read_csv_auto('D:\实战项目\Dataanalysis\archive\*.csv')
        WHERE user_session IS NOT NULL
        GROUP BY user_id, user_session
    ) A
    GROUP BY user_id
)

SELECT
    t1.*,
    t2.avg_session
FROM t1 
LEFT JOIN t2 ON t1.user_id = t2.user_id;