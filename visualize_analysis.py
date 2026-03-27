import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import os

# ===================== 配置区 =====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "Mia@521.",  # 填你真实密码
    "database": "movie_analysis",
    "charset": "utf8mb4"
}
# ======================================================

plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False

def get_data_from_mysql(sql):
    conn = pymysql.connect(**MYSQL_CONFIG)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

# ---------------------- 可视化1：评分分布饼图 ----------------------
def plot_score_distribution():
    sql = """
    SELECT 
        CASE WHEN score >=9 THEN '9分及以上'
             WHEN score >=8 THEN '8-9分'
             WHEN score >=7 THEN '7-8分'
             ELSE '7分以下' END AS score_level,
        COUNT(*) AS movie_count
    FROM movie
    GROUP BY score_level
    """
    df = get_data_from_mysql(sql)

    # 修复：转成列表，彻底解决类型警告
    labels = df["score_level"].tolist()
    values = df["movie_count"].tolist()

    plt.figure(figsize=(8, 6))
    plt.pie(
        values,
        labels=labels,  # 现在是列表，无类型错误
        autopct="%1.1f%%",
        startangle=90
    )
    plt.title("电影评分分布", fontsize=14, fontweight="bold")
    plt.savefig(os.path.join(BASE_DIR, "评分分布饼图.png"), bbox_inches="tight", dpi=300)
    plt.close()
    print("✅ 评分分布饼图已生成")

# ---------------------- 可视化2：时长区间与评分 ----------------------
def plot_duration_score():
    sql = """
    SELECT 
        CASE WHEN duration <60 THEN '60分钟以下'
             WHEN duration BETWEEN 60 AND 90 THEN '60-90分钟'
             WHEN duration BETWEEN 91 AND 120 THEN '91-120分钟'
             WHEN duration BETWEEN 121 AND 150 THEN '121-150分钟'
             ELSE '150分钟以上' END AS duration_range,
        ROUND(AVG(score),2) AS avg_score
    FROM movie
    WHERE duration IS NOT NULL
    GROUP BY duration_range
    ORDER BY avg_score DESC
    """
    df = get_data_from_mysql(sql)
    plt.figure(figsize=(10, 6))
    bars = plt.bar(df["duration_range"], df["avg_score"], color="#4ECDC4")
    for bar in bars:
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, h+0.05, f"{h}", ha="center")
    plt.ylim(0, 10)
    plt.title("各时长区间平均评分", fontsize=14)
    plt.savefig(os.path.join(BASE_DIR, "时长与评分柱状图.png"), bbox_inches="tight", dpi=300)
    plt.close()
    print("✅ 时长与评分柱状图已生成")

# ---------------------- 可视化3：地区电影数量 ----------------------
def plot_area_count():
    sql = """
    SELECT area, COUNT(*) AS cnt
    FROM movie
    GROUP BY area
    HAVING cnt >=2
    ORDER BY cnt DESC
    """
    df = get_data_from_mysql(sql)
    plt.figure(figsize=(12, 6))
    plt.bar(df["area"], df["cnt"], color="#FF6B6B")
    plt.title("各地区电影数量", fontsize=14)
    plt.xticks(rotation=45)
    plt.savefig(os.path.join(BASE_DIR, "地区电影数量柱状图.png"), bbox_inches="tight", dpi=300)
    plt.close()
    print("✅ 地区电影数量图已生成")

# ---------------------- 主程序 ----------------------
if __name__ == "__main__":
    plot_score_distribution()
    plot_duration_score()
    plot_area_count()
    print("\n🎉 所有图表生成完成！")