
CREATE OR REPLACE TABLE full_user_tags AS

WITH 

MaxDate AS (
    SELECT MAX(Recency) AS anchor_time 
    FROM full_user_features
),

CleanedFeatures AS (
    SELECT 
        user_id,
        COALESCE(Frequency, 0) AS F_val,
        COALESCE(monetary, 0) AS M_val,
        COALESCE(Intent, 0) AS I_val,
        COALESCE(EXTRACT('epoch' FROM avg_session), 0) AS S_val,
        COALESCE(
            date_diff('day', Recency, (SELECT anchor_time FROM MaxDate)), 
            999 
        ) AS R_val
    FROM full_user_features
),

PotentialTags AS (
    SELECT 
        user_id,
        CASE 
            WHEN I_val >= 3 AND S_val >= 180 THEN '核心高意向潜客'
            WHEN S_val >= 180 AND I_val < 3 THEN '高时长静默潜客'
            WHEN I_val >= 3 AND S_val < 60 THEN '浅层高频交互客'
            WHEN I_val <= 2 AND S_val <= 60 THEN '尾部低价值流量'
            ELSE '常规培育客群'
        END AS 用户标签,
        '潜客' AS 用户大类
    FROM CleanedFeatures
    WHERE F_val = 0
),

ActiveTags AS (
    SELECT 
        user_id,
        CASE 
            WHEN M_val >= 900 AND R_val <= 30 THEN '核心高价值客户'  
            WHEN M_val >= 900 AND R_val > 30 THEN '重要挽留客户' 
            WHEN F_val = 1 AND M_val >= 400 AND I_val >= 4 THEN '高潜单次大客'
            WHEN M_val < 400 AND I_val >= 4 THEN '高互动普通客'
            WHEN F_val = 1 AND M_val <= 200 AND I_val <= 2 THEN '低价值流失客'
            ELSE '一般客户'
        END AS 用户标签,
        '成交客' AS 用户大类
    FROM CleanedFeatures
    WHERE F_val > 0
)

SELECT * FROM PotentialTags
UNION ALL
SELECT * FROM ActiveTags;