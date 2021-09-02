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

import json
import pymysql
import requests
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine


class mysqlDatabase:
    def __init__(self, database_username, database_password, database_ip, database_name):
        self.database_username = database_username
        self.database_password = database_password
        self.database_ip       = database_ip
        self.database_name     = database_name
    def get_engine(self):
        sql_engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(self.database_username, self.database_password, self.database_ip, self.database_name))
        return sql_engine
    def select_table(self, sql):
        connection = self.get_engine()
        df = pd.read_sql(sql, con = connection)
        print('Successfully select from Bigdata table')
        return df
    def insert_table(self, df, table_name):
        connection = self.get_engine()
        df.astype(str).to_sql(name=table_name, con=connection, if_exists = 'append', index=False)
        print('Successfully insert into Bigdata table: ' + table_name)
        return df
    def upsert_table(self, df, table_name):
        connection = self.get_engine()
        df.astype(str).to_sql(name=table_name, con=connection, if_exists = 'replace', index=False)
        trans = connection.begin()
        try:
            # delete those rows that we are going to "upsert"
            sql = '''
            delete from :table_name where id in (select id from my_tmp)
            '''
            sql = sql.replace(':table_name', table_name)
            connection.execute('')
            trans.commit()
            # insert changed rows
            df.to_sql(name=table_name, con=connection, if_exists = 'append', index=False)
        except:
            # trans.rollback()
            print('oops, upsert failed!')


class dcardApi:
    def __init__(self, base_url, popular, max_limit):
        self.base_url = base_url
        self.popular = popular
        self.max_limit = max_limit

    def get_df_from_api(self, url):
        response = requests.get(url).text
        data = json.loads(response)
        df = pd.DataFrame(data)
        return df

    def get_Dcard_forums(self):
        '''
        看板資訊
        '''
        url = self.base_url + '/forums'
        df = self.get_df_from_api(url)
        return df

    def get_Dcard_posts_all(self):
        '''
        全部文章
        '''
        url = self.base_url + '/posts'
        df = self.get_df_from_api(url)
        return df

    def get_Dcard_posts(self, forums_name, before_postid=None):
        '''
        看板文章列表
        '''
        if before_postid == None:
            url = self.base_url + '/forums/' + str(forums_name) + '/posts' + '?popular=' + self.popular + '&limit=' + str(self.max_limit)
        else:
            url = self.base_url + '/forums/' + str(forums_name) + '/posts' + '?popular=' + self.popular + '&limit=' + str(self.max_limit) + '&before=' + str(before_postid)
        df = self.get_df_from_api(url)
        return df

    def get_Dcard_posts_context(self, postid):
        '''
        文章內容
        '''
        url = self.base_url + '/posts/' + str(postid)
        df = self.get_df_from_api(url)
        return df
    
    def get_Dcard_posts_links(self, postid):
        '''
        文章內引用連結
        '''
        url = self.base_url + '/posts/' + str(postid) + '/links'
        df = self.get_df_from_api(url)
        return df

    def get_Dcard_posts_comments(self, postid):
        '''
        文章留言
        '''
        url = self.base_url + '/posts/' + str(postid) + '/comments'
        df = self.get_df_from_api(url)
        return df

# +
# database_username = 'jackyfu'
# database_password = 'data8756'
# database_ip       = 'sg2nlmysql29plsk.secureserver.net'
# database_name     = 'Bigdata'
# base_url = 'https://www.dcard.tw/service/api/v2'
# popular = 'false'
# max_limit = '100'
# ##
# MysqlDatabase = mysqlDatabase(database_username, database_password, database_ip, database_name)
# DcardApi = dcardApi(base_url, popular, max_limit)
# forums_name = 'boba'
# DcardApi.get_Dcard_posts(forums_name)
