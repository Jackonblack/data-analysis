import re
import logging
import pandas as pd
import json
import os

# ===================== 清洗配置区（完全保留你的配置） =====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(BASE_DIR, "movie_raw_data.json")
CLEAN_DATA_SAVE_PATH = os.path.join(BASE_DIR, "movie_clean_data.csv")
# ======================================================

# 日志配置（完全保留你的配置）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "clean_data.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def clean_movie_data(raw_df):
    """
    电影数据清洗主函数（保留你的函数结构，仅适配新字段）
    :param raw_df: 原始数据DataFrame
    :return: 清洗后的标准化DataFrame
    """
    df = raw_df.copy()
    logging.info(f"【清洗开始】原始数据量：{len(df)}条")

    # 1. 异常值过滤（保留你的业务规则过滤，仅适配新字段）
    df = df[
        (df["评分"] >= 0) & (df["评分"] <= 10) &  # 评分范围校验
        (df["标题"].str.len() > 0)  # 非空标题
    ]
    logging.info(f"【步骤1完成】异常值过滤后数据量：{len(df)}条")

    # 2. 新字段简单标准化（新增：适配当前字段）
    # 时长标准化：去除"分钟"字样，仅保留数字
    if "时长" in df.columns:
        df["时长"] = df["时长"].str.replace("分钟", "").str.strip()
        logging.info(f"【步骤2完成】时长标准化完成")

    # 3. 空值兜底处理（仅保留对现有字段的处理）
    df["地区"] = df["地区"].fillna("未知地区")
    df["时长"] = df["时长"].fillna("未知时长")
    df["上映时间"] = df["上映时间"].fillna("未知上映时间")
    # 类型字段空值处理：空列表转为["未知类型"]
    df["类型"] = df["类型"].apply(lambda x: x if isinstance(x, list) and len(x) > 0 else ["未知类型"])
    logging.info(f"【步骤3完成】空值处理完成")

    # 4. 业务去重（修改：按标题去重，不再有年份）
    df = df.drop_duplicates(subset=["标题"], keep="first")
    logging.info(f"【步骤4完成】去重后数据量：{len(df)}条")

    # 5. 数据类型统一（保留你的评分处理）
    df["评分"] = df["评分"].round(1)
    logging.info(f"【清洗完成】最终有效数据量：{len(df)}条")
    return df

# 程序入口（完全保留你的结构）
if __name__ == "__main__":
    # 读取原始数据
    with open(RAW_DATA_PATH, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    raw_df = pd.DataFrame(raw_data)
    # 执行清洗
    clean_df = clean_movie_data(raw_df)
    # 保存清洗后的数据
    clean_df.to_csv(CLEAN_DATA_SAVE_PATH, index=False, encoding="utf-8-sig")
    logging.info(f"清洗后的数据已保存至：{CLEAN_DATA_SAVE_PATH}")