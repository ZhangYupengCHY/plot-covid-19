# -*- coding: utf-8 -*-
"""
Proj: pyecharts
Created on:   2020/7/1 18:30
@Author: RAMSEY

"""
import warnings


from public_function import conn_db

"""
通过Johns Hopkins University的数据源(截止到2020年7月1日)
来可视化全球疫情地图
"""

warnings.filterwarnings(action='ignore')


if __name__ == '__main__':
    # 初始化连接数据库
    conn_mysql = conn_db.ConnMysql()
    sql = "SELECT * FROM covid_19_cases limit 10000"
    covid_19_data = conn_mysql.read_table(sql)
    print(covid_19_data.columns)
    print(covid_19_data.head(5))



