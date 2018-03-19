import requests
import chardet
import time
import json
import pymongo
from multiprocessing import Pool
from config import *
from pyquery import PyQuery as pq
from json.encoder import JSONEncoder
from urllib.parse import urlencode
from requests.exceptions import ConnectionError
headers = {
    'Host': 'www.guokr.com',
    'Referer': 'https://www.guokr.com/scientific/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.27 Safari/537.36'
    }

#配置mongodb数据库
client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]


def get_index_html(offset,timestamp):
    base_url = 'https://www.guokr.com/apis/minisite/article.json?'
    data = {
        'retrieve_type': 'by_subject',
        'limit': '20',
        'offset': offset,
        '_': timestamp
    }
    url = base_url+urlencode(data)
    try:
        response =requests.get(url,headers=headers)
        if response.status_code == 200:
            response.encoding = chardet.detect(response.content)['encoding']
            return response.text
        return None
    except ConnectionError:
        print('Failed to connection')
        return None


def parse_index_page(html):
    try:
        data = json.loads(html,encoding='utf-8')
        if data and 'result' in data.keys():
            for i in range(0,20):
                yield data['result'][i]['url']
    except JSONEncoder:
        return None


def get_detail_html(url):
    try:
        response =requests.get(url,headers=headers)
        if response.status_code == 200:
            response.encoding = chardet.detect(response.content)['encoding']
            return response.text
        return None
    except ConnectionError:
        print('Failed to connection')
        return None


def parse_detail_page(html):
    doc = pq(html)
    author = doc('#authorName').text()
    author_type = doc('body > div.container.article-page > div.side > div.author-info.clearfix > div > span.author-introduction').text()
    article_title = doc('#articleTitle').text()
    article_issue_time = doc('body > div.container.article-page > div.main > div.content > div.content-th > div > span').text()
    article_content = doc('#articleContent > div > div:nth-child(2) > p').text()
    data = {
        'author':author,
        'author_type':author_type,
        'article_title':article_title,
        'article_issue_time':article_issue_time,
        'article_content':article_content
    }
    save_to_mongo(data)

def save_to_mongo(data):
    if db[MONGO_TABLE].insert(data):
        print('存储到MongoDB数据库成功',data)
    else:
        print('存储失败',data)


def main(offset):
    page = 0
    timestamp = int(round(time.time())*1000)
    html = get_index_html(offset,timestamp)
    for url in parse_index_page(html):
        print('正在爬取第{}页'.format(page))
        html = get_detail_html(url)
        parse_detail_page(html)
        page+=1


if __name__ == '__main__':
    pool = Pool()
    offset = [0] + [i * 20 + 18 for i in range(0, 10)]
    pool.map(main,offset)
