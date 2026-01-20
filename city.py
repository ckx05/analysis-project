#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
城市数据模块
"""

import pandas as pd
import os


def city():
    """读取城市CSV数据"""
    csv_path = os.path.join(os.path.dirname(__file__), 'city.csv')
    try:
        city_data = pd.read_csv(csv_path, encoding='utf-8')
        required_cols = ['code', '省', '市']
        data = city_data[['code', '省', '市']]
        return data.values.tolist()
    except Exception as e:
        print(f'读取城市数据失败: {e}')
        return [
            ['101010100', '北京', '北京市'],
            ['101020100', '上海', '上海市'],
            ['101280100', '广东', '广州市'],
            ['101280600', '广东', '深圳市'],
            ['101210100', '浙江', '杭州市'],
        ]


if __name__ == '__main__':
    result = city()
    print(f'获取到 {len(result)} 个城市')
    for i, row in enumerate(result[:5]):
        print(f'{i+1}. 代码:{row[0]}, 省:{row[1]}, 市:{row[2]}')
