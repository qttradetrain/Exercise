# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 11:15:43 2020

@author: LENOVO
"""

import os
from sqlalchemy import create_engine
import pymysql
import pandas as pd

def tomysql(address):
    for i in os.listdir(address):
        tmp = os.path.abspath(address)
        first = os.path.join(tmp,i)
        for j in os.listdir(first):
            second = os.path.join(first,j)
            for n in os.listdir(second):
                third = os.path.join(second,n)
                for info in os.listdir(third):
                    info = os.path.join(third,info) #将路径与文件名结合起来就是每个文件的完整路径
                    try:
                        data = pd.read_csv(info,sep=',', encoding = "gbk")
                        engine = create_engine("mysql+pymysql://root:csj1212831910@localhost:3306/f_tick_2017?charset=utf8mb4")
                        data.to_sql(name=i+n,con=engine,if_exists='append',index=False,index_label=False)
                    except:
                        continue
moth_2017 = os.listdir('D:/Data/data/f_tick_2017')
for moth in moth_2017:
    address = os.path.join('D:/Data/data/f_tick_2017',moth)
    tomysql(address)
    
#连接mysql
conn = pymysql.connect(host='localhost', user='root',password='csj1212831910',database='f_tick_2017',charset="utf8")
sql_1 = "select * from dc20170103"
#利用pandas直接获取数据
data = pd.read_sql(sql_1, conn)
conn.close()