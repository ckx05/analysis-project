"""
爬虫运行包装脚本
"""
import sys
import os

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 现在导入并运行爬虫
from spiders.jobs_spider import jobspider
from city_data.city import city as get_city_list

if __name__ == '__main__':
    print("=" * 80)
    print("开始爬虫 - BOSS Zhipin 招聘数据收集")
    print("=" * 80)
    print()
    
    # 获取市ID
    city_list = get_city_list()
    print(f"准备爬取 {len(city_list)} 个城市的数据\n")
    
    for idx, c in enumerate(city_list):
        city_code = c[0]
        provence = c[1]
        city_name = c[2]
        print(f"\n[{idx+1}/{len(city_list)}] 正在处理: {provence} - {city_name}")
        print("-" * 80)
        try:
            jobspider(city_code, city_name, provence)
        except Exception as e:
            print(f"错误: {e}")
            continue
    
    print("\n" + "=" * 80)
    print("爬虫运行完成！")
    print("=" * 80)
