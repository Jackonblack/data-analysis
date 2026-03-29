###导入依赖
import requests
import time
import json
import logging
from bs4 import BeautifulSoup
import os

###基本配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))   #获取根目录
BASE_URL = "https://ssr1.scrape.center/page/{}"  #用于重构页面URL
MAX_PAGES = 10   #设置爬取最大页数
REQUEST_DELAY = 1   #避免请求集中
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": "https://scrape.center/"   #引用页
}   #配置HTTP请求头
RAW_DATA_SAVE_PATH = os.path.join(BASE_DIR, "movie_raw_data.json")   #设置原始文件输出路径

###日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "crawl.log"), encoding="utf-8"),   #FileHandler-文件处理器
        logging.StreamHandler()   #StreamHandler-控制台处理器
    ]
)

###函数1：爬取单页数据
def crawl_single_page(page_num):
    page_data = []
    url = BASE_URL.format(page_num) if page_num > 1 else "https://ssr1.scrape.center/"   #第一页特殊处理
    try:
        response = requests.get(url, headers=HEADERS, timeout=15, verify=False)
        response.raise_for_status()   #检查状态码，非2XX会报错
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")   #解析HTML
        movie_cards = soup.find_all("div", class_="el-card item m-t is-hover-shadow")   #匹配电影卡片
        if not movie_cards:   #空值检测
            logging.warning(f"第{page_num}页未找到电影卡片")
            return page_data

        for card in movie_cards:   #遍历电影卡片
            try:
                movie_item = {}
                body = card.find("div", class_="el-card__body")
                if not body:
                    continue
                #标题
                title_tag = body.find("a", class_="name").find("h2", class_="m-b-sm")   #type: ignore   #分级查找
                movie_item["标题"] = title_tag.text.strip().split(" - ")[0]   #type: ignore
                #评分
                score_tag = body.find("p", class_="score m-t-md m-b-n-sm")
                movie_item["评分"] = float(score_tag.text.strip()) if score_tag else "未知"   #增加空值处理
                #类型
                categories = body.find_all("button", class_="category")   #全量查找
                movie_item["类型"] = [cat.text.strip() for cat in categories]
                #地区、时长
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

###函数2：逐页爬取全部数据
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

###程序入口
if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    crawl_all_pages()