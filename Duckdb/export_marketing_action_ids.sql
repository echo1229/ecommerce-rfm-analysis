-- 场景 A：提取 139 万【核心高意向潜客】去发新人券
COPY (
    SELECT user_id
    FROM full_user_tags
    WHERE 用户标签 = '核心高意向潜客'
) TO 'D:\实战项目\Dataanalysis\target_high_intent.csv' (HEADER, DELIMITER ',');

-- 场景 B：提取几百万【尾部低价值流量】扔进广告投放黑名单
COPY (
    SELECT user_id
    FROM full_user_tags
    WHERE 用户标签 = '尾部低价值流量'
) TO 'D:\实战项目\Dataanalysis\exclude_low_value.csv' (HEADER, DELIMITER ',');