# 导入依赖库
import requests
import time
import json
import logging
from bs4 import BeautifulSoup
import os

# ===================== 爬虫配置区（适配Windows 11） =====================
# 项目根目录（自动获取当前文件所在目录）
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_URL = "https://ssr1.scrape.center/page/{}"  # 真实可用URL
MAX_PAGES = 10
REQUEST_DELAY = 1
# 请求头（完全保留你的）
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": "https://scrape.center/"
}
# 输出文件（完全和你一致）
RAW_DATA_SAVE_PATH = os.path.join(BASE_DIR, "movie_raw_data.json")
# ======================================================

# 日志配置（完全保留你的）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "crawl.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def crawl_single_page(page_num):
    """
    爬取单页数据（结构和你完全一致）
    """
    page_data = []
    # 第1页特殊处理
    url = BASE_URL.format(page_num) if page_num > 1 else "https://ssr1.scrape.center/"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=15, verify=False)
        response.raise_for_status()
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")

        # 匹配真实电影卡片
        movie_cards = soup.find_all("div", class_="el-card item m-t is-hover-shadow")
        if not movie_cards:
            logging.warning(f"第{page_num}页未找到电影卡片")
            return page_data

        for card in movie_cards:
            try:
                movie_item = {}
                body = card.find("div", class_="el-card__body")
                if not body:
                    continue

                # 标题
                title_tag = body.find("a", class_="name").find("h2", class_="m-b-sm")
                movie_item["标题"] = title_tag.text.strip().split(" - ")[0]

                # 评分
                score_tag = body.find("p", class_="score m-t-md m-b-n-sm")
                movie_item["评分"] = float(score_tag.text.strip()) if score_tag else 0.0

                # 类型
                categories = body.find_all("button", class_="category")
                movie_item["类型"] = [cat.text.strip() for cat in categories]

                # 地区、时长
                info_divs = body.find_all("div", class_="info")
                area_duration = info_divs[0].text.strip().split(" / ") if len(info_divs) >= 1 else ["", ""]
                movie_item["地区"] = area_duration[0].strip()
                movie_item["时长"] = area_duration[1].strip() if len(area_duration) > 1 else ""

                # 上映时间
                movie_item["上映时间"] = info_divs[1].text.strip().replace("上映", "") if len(info_divs) >= 2 else ""

                page_data.append(movie_item)

            except Exception as e:
                logging.error(f"单条解析失败: {str(e)[:50]}")
                continue

        logging.info(f"第{page_num}页爬取完成，有效数据：{len(page_data)}条")
        return page_data

    except Exception as e:
        logging.error(f"第{page_num}页爬取失败：{str(e)[:50]}")
        return page_data

def crawl_all_pages():
    all_data = []
    logging.info(f"开始爬取，最大页数：{MAX_PAGES}")
    for page in range(1, MAX_PAGES + 1):
        page_data = crawl_single_page(page)
        all_data.extend(page_data)
        time.sleep(REQUEST_DELAY)

    with open(RAW_DATA_SAVE_PATH, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    logging.info(f"爬取完成！总计：{len(all_data)}条")
    return all_data

# 程序入口
if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    crawl_all_pages()