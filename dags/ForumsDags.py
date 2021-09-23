# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: 'Python 3.9.6 64-bit (''base'': conda)'
#     name: python3
# ---

import yaml
import pandas as pd
from utils import mysqlDatabase, dcardApi
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


class forumCrawler:
    def __init__(self, database_username, database_password, database_ip, database_name, base_url, popular, max_limit):
        self.database_username = database_username
        self.database_password = database_password
        self.database_ip = database_ip
        self.database_name = database_name
        self.base_url = base_url
        self.popular = popular
        self.max_limit = max_limit
    def get_forums(self, DcardApi):
        df = DcardApi.get_Dcard_forums()
        df = df[['id', 'alias', 'name', 'subscriptionCount', 'postCount', 'isSchool', 'createdAt']]
        df.postCount = df.postCount.apply(lambda x : x['last30Days'])
        df.createdAt = df.createdAt.apply(lambda x : x[:10])
        return df
    def main(self):
        '''
        實作
        '''
        MysqlDatabase = mysqlDatabase(self.database_username, self.database_password, self.database_ip, self.database_name)
        DcardApi = dcardApi(self.base_url, self.popular, self.max_limit)
        df_forums = self.get_forums(DcardApi)
        MysqlDatabase.upsert_table(df_forums, table_name='dcard_forums')
        return df_forums


def getParams(ti):
    with open('./dags/config.yml', 'r') as stream:
        myconfig = yaml.load(stream, Loader=yaml.CLoader)
    database_username = myconfig['mysql_database']['database_username']
    database_password = myconfig['mysql_database']['database_password']
    database_ip       = myconfig['mysql_database']['database_ip']
    database_name     = myconfig['mysql_database']['database_name']
    base_url = 'https://www.dcard.tw/service/api/v2'
    popular = 'false'
    max_limit = '100'
    ##
    params = (database_username, database_password, database_ip, database_name, base_url, popular, max_limit)
    ti.xcom_push(key='params_list', value=params)


def main(ti):
    database_username, database_password, database_ip, database_name, base_url, popular, max_limit = ti.xcom_pull(key='params_list', task_ids='getParams')
    ForumCrawler = forumCrawler(database_username, database_password, database_ip, database_name, base_url, popular, max_limit)
    df_forums = ForumCrawler.main()
    print(df_forums)
    print('Well Done!!')


with DAG("dcard_forums_crawler_dag", start_date = datetime(2021, 1, 1), 
        schedule_interval = "@daily", catchup=False) as dag:
    getParams = PythonOperator(
        task_id = "getParams",
        python_callable = getParams
    )
    main = PythonOperator(
        task_id = "main",
        python_callable = main
    )
    getParams >> main


