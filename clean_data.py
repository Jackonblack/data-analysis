###导入依赖
import re
import logging
import pandas as pd
import json
import os

###基本配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(BASE_DIR, "movie_raw_data.json")
CLEAN_DATA_SAVE_PATH = os.path.join(BASE_DIR, "movie_clean_data.csv")

###日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "clean_data.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

###函数：数据清洗
def clean_movie_data(raw_df):
    df = raw_df.copy()
    logging.info(f"【清洗开始】原始数据量：{len(df)}条")

    df = df[
        (df["评分"] >= 0) & (df["评分"] <= 10) &  # 评分范围校验
        (df["标题"].str.len() > 0)  # 非空标题
    ]
    logging.info(f"【步骤1完成】异常值过滤后数据量：{len(df)}条")

    if "时长" in df.columns:
        df["时长"] = df["时长"].str.replace("分钟", "").str.strip()
        logging.info(f"【步骤2完成】时长标准化完成")

    df["地区"] = df["地区"].fillna("未知地区")
    df["时长"] = df["时长"].fillna("未知时长")
    df["上映时间"] = df["上映时间"].fillna("未知上映时间")
    df["类型"] = df["类型"].apply(lambda x: x if isinstance(x, list) and len(x) > 0 else ["未知类型"])
    logging.info(f"【步骤3完成】空值处理完成")

    df = df.drop_duplicates(subset=["标题"], keep="first")
    logging.info(f"【步骤4完成】去重后数据量：{len(df)}条")

    df["评分"] = df["评分"].round(1)
    logging.info(f"【清洗完成】最终有效数据量：{len(df)}条")
    return df

###程序入口
if __name__ == "__main__":
    with open(RAW_DATA_PATH, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    raw_df = pd.DataFrame(raw_data)
    clean_df = clean_movie_data(raw_df)
    clean_df.to_csv(CLEAN_DATA_SAVE_PATH, index=False, encoding="utf-8-sig")
    logging.info(f"清洗后的数据已保存至：{CLEAN_DATA_SAVE_PATH}")