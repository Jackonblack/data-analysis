USE movie_analysis;

SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;
SET collation_connection = utf8mb4_unicode_ci;

-- 分析1：评分分布
SELECT
    score_level AS '评分等级',
    movie_count AS '电影数量',
    ratio AS '占比',
    business_interpretation AS '区间类型'
FROM (
    SELECT
        CASE
            WHEN score >= 9 THEN '9分及以上（经典）'
            WHEN score >= 8 THEN '8-9分（优质）'
            WHEN score >= 7 THEN '7-8分（良好）'
            ELSE '7分以下（普通）'
        END AS score_level,
        COUNT(*) AS movie_count,
        CONCAT(ROUND(COUNT(*)/(SELECT COUNT(*) FROM movie)*100,1),'%') AS ratio,
        CASE
            WHEN ROUND(COUNT(*)/(SELECT COUNT(*) FROM movie)*100,1) > 40 THEN '主流区间'
            WHEN ROUND(COUNT(*)/(SELECT COUNT(*) FROM movie)*100,1) > 20 THEN '次要区间'
            ELSE '小众区间'
        END AS business_interpretation
    FROM movie
    GROUP BY score_level
) AS temp
ORDER BY movie_count DESC;

-- 分析2：地区电影统计
SELECT
    area AS '上映地区',
    COUNT(*) AS '电影数量',
    ROUND(AVG(score),2) AS '平均评分',
    ROUND(MAX(score),1) AS '最高分',
    ROUND(MIN(score),1) AS '最低分'
FROM movie
GROUP BY area
HAVING COUNT(*)>=2
ORDER BY AVG(score) DESC;

-- 分析3：时长区间与评分
SELECT
    CASE
        WHEN duration < 60 THEN '60分钟以下'
        WHEN duration BETWEEN 60 AND 90 THEN '60-90分钟'
        WHEN duration BETWEEN 91 AND 120 THEN '91-120分钟'
        WHEN duration BETWEEN 121 AND 150 THEN '121-150分钟'
        ELSE '150分钟以上'
    END AS '时长区间',
    COUNT(*) AS '电影数量',
    ROUND(AVG(score),2) AS '平均评分'
FROM movie
WHERE duration IS NOT NULL
GROUP BY `时长区间`
ORDER BY AVG(score) DESC;

-- 分析4：高分电影列表
SELECT
    title AS '电影名称',
    score AS '评分',
    area AS '地区',
    duration AS '时长',
    release_time AS '上映时间',
    category AS '类型'
FROM movie
WHERE score >= 9
ORDER BY score DESC, duration DESC;