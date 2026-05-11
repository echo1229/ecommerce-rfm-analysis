CREATE TABLE user_features AS
WITH t1 AS (
SELECT user_id user_id,
	MAX(CASE WHEN event_type = 'purchase' THEN event_time ELSE NULL END) Recency,
	COUNT(CASE WHEN event_type = 'purchase' THEN  1 ELSE NULL END) Frequency,
	SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) monetary,
	SUM(CASE                                              -- 加权 Intent
		WHEN event_type = 'cart' THEN 3                   -- cart 权重 3（待校准）
		WHEN event_type = 'view' THEN 1                   -- view 权重 1
		ELSE 0
	END) AS Intent,
	MIN(event_time) AS first_seen                         -- 首次出现时间，用于新老用户区分
FROM sample_events
GROUP BY user_id),

t2 AS (
SELECT 
	user_id,
	AVG(session_duration) avg_session
FROM (SELECT 
		 user_id,
		 user_session,
		 (MAX(event_time) - MIN(event_time)) session_duration
	 FROM sample_events
	 WHERE user_session IS NOT NULL
	 GROUP BY user_id,user_session
	 ) A 
GROUP BY user_id)


SELECT 
t1.*,
t2.avg_session
FROM t1 LEFT JOIN t2 ON t1.user_id = t2.user_id;