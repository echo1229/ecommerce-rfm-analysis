
COPY (
    SELECT 
        t.user_id, 
        t.用户大类, 
        t.用户标签,
        COALESCE(f.monetary, 0) AS GMV
    FROM full_user_tags t
    LEFT JOIN full_user_features f ON t.user_id = f.user_id
) TO 'D:\实战项目\Dataanalysis\BI_data.csv' (HEADER, DELIMITER ',');