-- 1. 创建业务数据库，统一utf8mb4编码，避免中文乱码（MySQL 8.0默认推荐）
CREATE DATABASE IF NOT EXISTS movie_analysis 
DEFAULT CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

-- 使用该数据库
USE movie_analysis;

-- 2. 创建电影信息主表（适配当前字段体系）
CREATE TABLE IF NOT EXISTS movie (
    movie_id INT PRIMARY KEY AUTO_INCREMENT COMMENT '电影唯一ID',
    title VARCHAR(100) NOT NULL COMMENT '电影标题',
    score DECIMAL(3,1) NOT NULL COMMENT '电影评分',
    category TEXT COMMENT '电影类型(JSON格式)',
    area VARCHAR(50) NOT NULL COMMENT '上映地区',
    duration INT COMMENT '电影时长（分钟）',
    release_time VARCHAR(20) NOT NULL COMMENT '上映时间',
    crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据爬取时间',
    -- 索引优化（适配高频分析场景）
    KEY idx_score (score) COMMENT '评分查询索引',
    KEY idx_area (area) COMMENT '地区统计索引',
    KEY idx_release_time (release_time) COMMENT '上映时间分析索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='电影信息主表';