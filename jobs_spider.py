#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BOSS Zhipin 招聘数据爬虫
"""

import requests
import re
from lxml import etree
import time
import random
import pymongo
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from city_data.city_clean import city as get_city_list


def get_page(page, city_code):
    """获取页面内容"""
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    print(f'正在爬取第 {page} 页 (城市代码: {city_code})')
    url = f'https://www.zhipin.com/c{city_code}-p100511/?page={page}&ka=page-{page}'
    try:
        response = requests.get(url, headers=header, timeout=10)
        return response.text
    except Exception as e:
        print(f'获取页面失败: {e}')
        return None


def average(job_salary):
    """计算薪资均值"""
    pattern = re.compile(r'\d+')
    try:
        res = re.findall(pattern, job_salary)
        if len(res) >= 2:
            avg_salary = (int(res[0]) + int(res[1])) / 2
        elif len(res) == 1:
            avg_salary = int(res[0])
        else:
            avg_salary = 0
    except Exception:
        avg_salary = 0
    return avg_salary


def save_to_mongo(data, db, collection_name):
    """保存到MongoDB"""
    try:
        db[collection_name].insert_one(data)
    except Exception as e:
        print(f'MongoDB 保存失败: {e}')


def parse(html, city, provence, page, db, collection_name):
    """解析页面数据"""
    if html is None:
        return
    
    try:
        data = etree.HTML(html)
        items = data.xpath('//*[@id="main"]/div/div[2]/ul/li')
        
        if not items:
            print(f'未找到职位信息')
            return
        
        for item in items:
            try:
                title_list = item.xpath('./div/div[1]/h3/a/div[1]/text()')
                salary_list = item.xpath('./div/div[1]/h3/a/span/text()')
                company_list = item.xpath('./div/div[2]/div/h3/a/text()')
                exp_list = item.xpath('./div/div[1]/p/text()[2]')
                degree_list = item.xpath('./div/div[1]/p/text()[3]')
                scale_list = item.xpath('./div/div[2]/div/p/text()[3]')
                
                if not all([title_list, salary_list, company_list, exp_list, degree_list, scale_list]):
                    continue
                
                job_title = title_list[0].strip()
                job_salary = salary_list[0].strip()
                job_company = company_list[0].strip()
                job_experience = exp_list[0].strip()
                job_degree = degree_list[0].strip()
                company_scale = scale_list[0].strip()
                
                avg_salary = average(job_salary)
                signal = city + str(page)
                
                print(f'{provence}|{city}|{job_title}|{job_salary}|{avg_salary}')
                
                job = {
                    'signal': signal,
                    '省': provence,
                    '城市': city,
                    '职位名称': job_title,
                    '职位薪资': job_salary,
                    '公司名称': job_company,
                    '工作经验': job_experience,
                    '学历要求': job_degree,
                    '公司规模': company_scale,
                    '平均薪资': avg_salary
                }
                
                save_to_mongo(job, db, collection_name)
                
            except Exception as e:
                print(f'解析单条记录失败: {e}')
                continue
                
    except Exception as e:
        print(f'解析页面失败: {e}')


def jobspider(city_code, city, provence, db, collection_name):
    """爬虫主函数"""
    MAX_PAGE = 30
    
    for i in range(1, MAX_PAGE + 1):
        job_signal = city + str(i)
        
        try:
            html = get_page(i, city_code)
            parse(html, city, provence, i, db, collection_name)
            print('-' * 80)
            time.sleep(random.randint(2, 4))
        except Exception as e:
            print(f'爬取页面 {i} 失败: {e}')
            break


def main():
    """主函数"""
    print('=' * 80)
    print('BOSS Zhipin 招聘数据爬虫')
    print('=' * 80)
    print()
    
    MONGO_URL = 'localhost'
    MONGO_DB = 'zhipin'
    MONGO_COLLECTION = 'jobs'
    
    try:
        client = pymongo.MongoClient(MONGO_URL, port=27017, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print('MongoDB 连接成功')
    except Exception as e:
        print(f'MongoDB 连接失败: {e}')
        print('请确保 MongoDB 已启动')
        return
    
    db = client[MONGO_DB]
    
    try:
        city_list = get_city_list()
        print(f'准备爬取 {len(city_list)} 个城市的数据\n')
        
        for idx, city_info in enumerate(city_list):
            city_code = city_info[0]
            provence = city_info[1]
            city_name = city_info[2]
            
            print(f'\n[{idx + 1}/{len(city_list)}] 正在处理: {provence} - {city_name}')
            print('=' * 80)
            
            try:
                jobspider(city_code, city_name, provence, db, MONGO_COLLECTION)
            except Exception as e:
                print(f'城市爬虫错误: {e}')
                continue
        
        print('\n' + '=' * 80)
        print('爬虫运行完成！')
        print('=' * 80)
        
        collection = db[MONGO_COLLECTION]
        count = collection.count_documents({})
        print(f'已收集 {count} 条职位数据')
        
    finally:
        client.close()


if __name__ == '__main__':
    main()
