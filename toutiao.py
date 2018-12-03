from urllib.parse import urlencode
import requests
import re
import json
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
import os
from hashlib import md5
from multiprocessing.pool import Pool
from config import *
import pymongo
headers={
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'
}
client = pymongo.MongoClient(MONGO_URL,connect=False)
db = client[MONGO_DB]

#获取索引页信息
def get_page_index(offset,keyword):
    data={
        'offset': offset,
        'format': 'json',
        'keyword': keyword,
        'autoload': 'true',
        'count': 20,
        'cur_tab': 3, #提取图集在这里填3 综合填1，这里填图集时方便提取url
        'from': 'gallery',  #提取图集在这里填gallery
    }
    url = 'https://www.toutiao.com/search_content/?' + urlencode(data)
    try:
        response = requests.get(url,headers=headers)
        if response.status_code==200:
            return response.text
        return None
    except RequestException:
        print("请求索引页出错")
        return None

#提取所有详情页的url
def parse_page_index(html):
    data = json.loads(html)
    if data and 'data' in data.keys():
        for item in data.get('data'):
            yield item.get('article_url')

#请求详情页
def get_parse_detail(url):
    try:
        response = requests.get(url,headers=headers)
        if response.status_code==200:
            return response.text
        return None
    except RequestException:
        print("请求详情页出错")
        return None

#提取详情页信息
def parse_page_detail(html,url):
    #利用bs4提取标题
    soup = BeautifulSoup(html,'lxml')
    title = soup.select('title')[0].get_text()
    #利用正则匹配，提取url，记得加上‘\’
    pattern = re.compile('gallery: JSON.parse\("(.*?)"\)',re.S)
    result = re.search(pattern,html)
    if result:
        images_data = json.loads(result.group(1).replace('\\',''))#清洗
        if images_data and 'sub_images' in images_data.keys():
            sub_images = images_data.get('sub_images')#提取sub列表
            images = [item.get('url') for item in sub_images]#提取列表中的url
            for image in images:download_image(image)#遍历，调用download_image下载
            #返回字典格式
            return {
                'title':title,
                'url':url,
                'images':images
            }

#存储到MONGODB
def save_to_mongo(result):
    if db[MONGO_TABLE].insert(result):
        print("存储到MongoDB成功",result)
        return True
    return False

#下载图片
def download_image(url):
    try:
        print("正在下载",url)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            save_image(response.content)
        return None
    except RequestException:
        print("请求图片出错",url)
        return None

#保存到本地
def save_image(content):
    #md5去重
    file_path = '{0}/{1}.{2}'.format(os.getcwd(),md5(content).hexdigest(),'jpg')
    if not os.path.exists(file_path):
        with open(file_path,'wb')as f:
            f.write(content)
            f.close()

def main(offset):
    html = get_page_index(offset,KEYWORD)
    for url in parse_page_index(html):
        html = get_parse_detail(url)
        if html:
            result=parse_page_detail(html,url)
            if result:
                save_to_mongo(result)


if __name__ == '__main__':
    gropus = [x*20 for x in range(GROUP_START,GROUP_END+1)] #构造url，爬取多页
    pool = Pool() #多线程
    pool.map(main,gropus)