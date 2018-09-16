#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  5 17:20:52 2018

@author: cheating
"""
from __future__ import print_function
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from pymongo import MongoClient
import urllib.parse
import datetime
from bs4 import BeautifulSoup
import requests
from linebot import (LineBotApi, WebhookHandler, exceptions)
from linebot.exceptions import (InvalidSignatureError)
from linebot.models import *

###############################################################################
#                         股票機器人 Airflow股價自動推波                       #
###############################################################################

#自己APP的token
token='NvcdSNno+FJMhr5F0Nv5VJ+mXkg8va8BfCttefd7x4DVVhacvrh5l3olFrbJCBvb08gMbI3e+HTWv+9BCZAOao68R5lDszFQeWYPdWoW6l/YA0pH0o2caQNmITFJdPEfZBqmc3Om7gCEuiOiQc/0mAdB04t89/1O/w1cDnyilFU='
#自己的ID
ID='Uc88c85f361a7fe893af5caa7650ac125'

########### dag所有參數，就放在這裡面 ###########
args = {
    'owner': 'cheating', #這個dag的擁有者
    'start_date': airflow.utils.dates.days_ago(0) #開啟時，設定往前幾天開始執行
}

########### dag設定檔 ###########
dag = DAG(
    dag_id='Stock', #dag的名稱
    default_args=args, #把上方的參數放進去
    schedule_interval='10 * * * * *') #多久執行一次

username = urllib.parse.quote_plus('dear983604') 
password = urllib.parse.quote_plus('YoWei19870507')
host = 'ds163870.mlab.com' #主機位置
port = '63870' #port號碼
dbname='stockmaster'
collection='stockmaster'

line_bot_api = LineBotApi('NvcdSNno+FJMhr5F0Nv5VJ+mXkg8va8BfCttefd7x4DVVhacvrh5l3olFrbJCBvb08gMbI3e+HTWv+9BCZAOao68R5lDszFQeWYPdWoW6l/YA0pH0o2caQNmITFJdPEfZBqmc3Om7gCEuiOiQc/0mAdB04t89/1O/w1cDnyilFU=')
yourid='Uc88c85f361a7fe893af5caa7650ac125'
#資料庫連接
def constructor():
    client = MongoClient('mongodb://%s:%s@%s:%s/%s?authMechanism=SCRAM-SHA-1' % (username, password, host, port,dbname))
    db = client[collection]
    return db

# 抓你的股票
def show_user_stock_fountion():  
    db=constructor()
    collect = db['fountion']
    cel=list(collect.find({"data": 'care_stock'}))
    return cel

########### 查看當前價格 ###########在Heroku執行查自己股票方法
def look_price(stock='3624', bs='>', price=31):
#    data = show_user_stock_fountion()
#    for i in data:
#        stock=i['stock']
#        bs=i['bs']
#        price=i['price']
        
    url = 'https://tw.stock.yahoo.com/q/q?s=' + stock 
    list_req = requests.get(url)
    soup = BeautifulSoup(list_req.content, "html.parser")
    getstock= soup.find('b').text #裡面所有文字內容
    if bs == '<':
        if float(getstock) < price:
            get=stock + '的價格：' + getstock
            line_bot_api.push_message(yourid, TextSendMessage(text=get))
    else:
        if float(getstock) > price:
            get=stock + '的價格：' + getstock
            line_bot_api.push_message(yourid, TextSendMessage(text=get))

####每10秒做job
second_5_j = schedule.every(10).seconds.do(job)

#    # 先到yahoo爬取該股票的資料
#    url = 'https://tw.stock.yahoo.com/q/q?s=' + stock
#    list_req = requests.get(url)
#    soup = BeautifulSoup(list_req.content, "html.parser")
#    getstock= soup.find('b').text
#    #開始進行價格判斷
#    if bs == '<': #判斷大小於
#         if float(getstock) < price:
#            get=stock + '的價格：' + getstock
#            line_bot_api = LineBotApi(token)
#            line_bot_api.push_message(ID, TextSendMessage(text=get))
#    else:
#         if float(getstock) > price:
#            get=stock + '的價格：' + getstock
#            line_bot_api = LineBotApi(token)
#            line_bot_api.push_message(ID, TextSendMessage(text=get))

########### 設定你關心的股票 ###########
#data=[{'stock' : '2002','bs' : '<', 'price': 20},
#      {'stock' : '2330','bs' : '>', 'price': 10}
#      ]

data = show_user_stock_fountion()

########### 主程式 ###########
for i in data: #把一個一個股票丟進去執行
    task = PythonOperator(
        task_id='stock'+i['stock'], #設定dag小分支的名稱
        python_callable=look_price, #指定要執行的function
        op_kwargs={'stock' : i['stock'],'bs' : i['bs'], 'price': i['price']}, #丟參數進去function
        dag=dag #把上方的設定檔案丟入主程式
        )
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
#-------------------------------- 神秘的小房間 ---------------------------------
#    
#username = urllib.parse.quote_plus('cheatingstock')
#password = urllib.parse.quote_plus('makeamoney')
#dbname='stock'
#collection='stock'  
#
#today = datetime.date.today()
#oneday=datetime.timedelta(days=1)
#if today.weekday()==6:
#    yesterday=today-oneday-oneday
#elif today.weekday()==0:
#    yesterday=today-oneday-oneday-oneday
#else:
#    yesterday=today-oneday
#
#args = {
#    'owner': 'cheating',
#    'start_date': airflow.utils.dates.days_ago(2)
#}
## dag設定檔
#dag = DAG(
#    dag_id='Stock',
#    default_args=args,
#    schedule_interval='10 * * * * *')
#
##資料庫連接
#def constructor():
#    client = MongoClient('mongodb://%s:%s@35.227.62.133/%s?authMechanism=SCRAM-SHA-1' % (username, password,dbname))
#    db = client[collection]
#    return db
#
## 抓使用者設定它關心的股票
#def fountion_cache():
#    db=constructor()
#    collect = db['fountion']
#    cel=list(collect.find({"data":'care_stock'}))
#
#    return cel
#
#        
## 查看當前價格
#def look_price(stock='3624', bs='>', price=31):
#    url = 'https://tw.stock.yahoo.com/q/q?s=' + stock
#    list_req = requests.get(url)
#    soup = BeautifulSoup(list_req.content, "html.parser")
#    getstock= soup.find('b').text
#    if bs == '<':
#         if float(getstock) < price:
#            get=stock + '的價格：' + getstock
#            line_bot_api = LineBotApi('zC07Cc+XsTqPeiqW19GliRGuAV+0MRcnrvklwXkQl15YouarLvIDRkNJpEdfy0kwLcTkiH58JxMGIAkYTEGbzOUZCz7bKwaZQZFx74N7FVJYGgvMbY+3aOum912u+/GLu5ZITYi3YSDioYif3AmGUgdB04t89/1O/w1cDnyilFU=')
#            line_bot_api.push_message('Ubd2f8cb71eb51467fe4554e92c3edcdb', TextSendMessage(text=get))
#    else:
#         if float(getstock) > price:
#            get=stock + '的價格：' + getstock
#            line_bot_api = LineBotApi('zC07Cc+XsTqPeiqW19GliRGuAV+0MRcnrvklwXkQl15YouarLvIDRkNJpEdfy0kwLcTkiH58JxMGIAkYTEGbzOUZCz7bKwaZQZFx74N7FVJYGgvMbY+3aOum912u+/GLu5ZITYi3YSDioYif3AmGUgdB04t89/1O/w1cDnyilFU=')
#            line_bot_api.push_message('Ubd2f8cb71eb51467fe4554e92c3edcdb', TextSendMessage(text=get))
#    data=fountion_cache()
#
#data=fountion_cache()
#
##主程式
#for i in data:
#    task = PythonOperator(
#        task_id='stock'+i['stock'],
#        python_callable=look_price,
#        op_kwargs={'stock' : i['stock'],'bs' : i['bs'], 'price': i['price']},
#        dag=dag)
    