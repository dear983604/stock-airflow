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
token='你自己的token'
#自己的ID
ID='你自己的ID'

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

########### 查看當前價格 ###########
def look_price(stock='3624', bs='>', price=31):
    # 先到yahoo爬取該股票的資料
    url = 'https://tw.stock.yahoo.com/q/q?s=' + stock
    list_req = requests.get(url)
    soup = BeautifulSoup(list_req.content, "html.parser")
    getstock= soup.find('b').text
    #開始進行價格判斷
    if bs == '<': #判斷大小於
         if float(getstock) < price:
            get=stock + '的價格：' + getstock
            line_bot_api = LineBotApi(token)
            line_bot_api.push_message(ID, TextSendMessage(text=get))
    else:
         if float(getstock) > price:
            get=stock + '的價格：' + getstock
            line_bot_api = LineBotApi(token)
            line_bot_api.push_message(ID, TextSendMessage(text=get))

########### 設定你關心的股票 ###########
data=[{'stock' : '2002','bs' : '<', 'price': 20},
      {'stock' : '2330','bs' : '>', 'price': 10}
      ]

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
    