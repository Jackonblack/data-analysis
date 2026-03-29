###导入依赖
import pandas as pd
import pymysql
import logging
import json
import os
import numpy as np
import ast  

###基本配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "Mia@521.",
    "database": "movie_analysis",
    "charset": "utf8mb4"
}
CLEAN_DATA_PATH = os.path.join(BASE_DIR, "movie_clean_data.csv")
BATCH_SIZE = 20

###日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "mysql_insert.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

###函数1：电影类型字符串转列表
def parse_category_str(category_str):
    if pd.isna(category_str):
        return ["未知类型"]
    try:
        return ast.literal_eval(str(category_str))   #尝试用ast.literal_eval解析
    except:
        try:
            return json.loads(str(category_str))   #尝试用json.loads解析
        except:
            return ["未知类型"]

###函数2：数据清洗，空值过滤
def clean_nan_values(df):
    df = df.copy()
    df["评分"] = df["评分"].fillna(0.0)
    df["时长"] = df["时长"].fillna(0)
    df["标题"] = df["标题"].fillna("未知标题")
    df["地区"] = df["地区"].fillna("未知地区")
    df["上映时间"] = df["上映时间"].fillna("未知上映时间")
    df["类型"] = df["类型"].apply(parse_category_str)
    return df

###函数3：将数据批量插入MySQL数据库
def batch_insert_data(clean_df):
    clean_df = clean_nan_values(clean_df)
    total_count = len(clean_df)
    logging.info(f"待插入电影数据总量：{total_count}条")
    success_count = 0

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        cursor = conn.cursor()
        movie_insert_sql = """
        INSERT INTO movie (title, score, category, area, duration, release_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for i in range(0, total_count, BATCH_SIZE):
            batch_df = clean_df.iloc[i:i+BATCH_SIZE]
            batch_data = []
            for _, row in batch_df.iterrows():
                batch_data.append((
                    str(row["标题"]) if pd.notna(row["标题"]) else "未知标题",
                    float(row["评分"]) if pd.notna(row["评分"]) else 0.0,
                    json.dumps(row["类型"], ensure_ascii=False),
                    str(row["地区"]) if pd.notna(row["地区"]) else "未知地区",
                    int(row["时长"]) if pd.notna(row["时长"]) and str(row["时长"]).isdigit() else None,
                    str(row["上映时间"]) if pd.notna(row["上映时间"]) else "未知上映时间"
                ))
            cursor.executemany(movie_insert_sql, batch_data)
            success_count += len(batch_data)
            logging.info(f"已插入：{success_count}/{total_count}条")
        
        conn.commit()
        logging.info("事务提交成功")
    except Exception as e:
        conn.rollback()
        logging.error(f"事务异常，已回滚：{str(e)}")
        raise e
    finally:
        conn.close()
        logging.info("数据库连接已关闭")

    logging.info(f"电影表插入完成，累计成功插入：{success_count}条")

###程序入口
if __name__ == "__main__":
    clean_df = pd.read_csv(CLEAN_DATA_PATH, encoding="utf-8-sig")
    batch_insert_data(clean_df)
    
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM movie")
        result = cursor.fetchone()
        movie_count = result[0] if result else 0
        logging.info(f"【入库验证】电影表总数据量：{movie_count}条")
    finally:
        conn.close()