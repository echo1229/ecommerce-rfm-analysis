SELECT 
    用户大类,
    用户标签, 
    COUNT(*) AS 人数 
FROM full_user_tags 
GROUP BY 用户大类, 用户标签 
ORDER BY 用户大类, 人数 DESC;