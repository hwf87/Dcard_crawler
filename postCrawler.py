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
import time
import pandas as pd
from utils import mysqlDatabase, dcardApi


class postCrawler:
    def __init__(self, database_username, database_password, database_ip, database_name, base_url, popular, max_limit):
        self.database_username = database_username
        self.database_password = database_password
        self.database_ip = database_ip
        self.database_name = database_name
        self.base_url = base_url
        self.popular = popular
        self.max_limit = max_limit
    def get_forums_list(self, MysqlDatabase, sql):
        df = MysqlDatabase.select_table(sql)
        forums_list = df['alias'].tolist()
        return forums_list
    def get_new_post(self, DcardApi, forums_alias):
        df = DcardApi.get_Dcard_posts(forums_alias)
        return df
    def get_old_post(self, DcardApi, forums_alias, before_postid):
        df = DcardApi.get_Dcard_posts(forums_alias, before_postid)
        return df
    def get_forums_before_postid(self, MysqlDatabase, forums_alias):
        sql = '''
        SELECT df.name, df.alias, dp.id, dp.title, dp.createdAT
        FROM Bigdata.dcard_posts dp
        left join Bigdata.dcard_forums df on dp.forumid = df.id
        where 1=1
        and df.alias = ':forums_alias'
        order by createdAT asc
        '''
        sql = sql.replace(':forums_alias', forums_alias)
        df = MysqlDatabase.select_table(sql)
        try:
            before_postid = df.head(1)['id'].tolist()[0]
        except:
            before_postid = None
        return before_postid
    def main(self):
        '''
        實作
        '''
        MysqlDatabase = mysqlDatabase(self.database_username, self.database_password, self.database_ip, self.database_name)
        DcardApi = dcardApi(self.base_url, self.popular, self.max_limit)
        ## 看板列表
        my_interest_forums = ['時事', '網路購物', '股票', '美妝', '工作', '考試', '穿搭', '3C', 'Apple', '感情', 
                              '美食', '理財', '居家生活', '臺灣大學', 'YouTuber']
        sql = '''
        select *
        from Bigdata.dcard_forums
        where 1=1
        and name in :my_interest_forums
        '''
        sql = sql.replace(':my_interest_forums', str(tuple(my_interest_forums)))
        forums_list = self.get_forums_list(MysqlDatabase, sql)

        ## 最新文章
        df_post = pd.DataFrame(columns=['id', 'title', 'excerpt', 'forumId', 'commentCount', 'likeCount', 'topics', 'author', 'gender', 'createdAt'])
        time_start = time.time() 
        for alias in forums_list:
            time.sleep(1)
            time_cost = time.time() - time_start
            if time_cost >= 3600:
                break
            try:
                print(alias)
                df_post = pd.concat([df_post, self.get_new_post(DcardApi, alias)])
                before_postid = PostCrawler.get_forums_before_postid(MysqlDatabase, alias)
                df_post = pd.concat([df_post, self.get_old_post(DcardApi, alias, before_postid)])
            except:
                print('connection abort, need some break')
                time.sleep(5)
                continue
        print('total time cost(sec): ', time_cost)
        df_post = df_post.fillna('')[['id', 'title', 'excerpt', 'forumId', 'commentCount', 'likeCount', 'topics', 'author', 'gender', 'createdAt']]
        MysqlDatabase.upsert_table(df_post, table_name='dcard_posts')
        return df_post


if __name__ == '__main__':
    with open('config.yml', 'r') as stream:
        myconfig = yaml.load(stream, Loader=yaml.CLoader)
    database_username = myconfig['mysql_database']['database_username']
    database_password = myconfig['mysql_database']['database_password']
    database_ip       = myconfig['mysql_database']['database_ip']
    database_name     = myconfig['mysql_database']['database_name']
    base_url = 'https://www.dcard.tw/service/api/v2'
    popular = 'false'
    max_limit = '100'
    ###
    PostCrawler = postCrawler(database_username, database_password, database_ip, database_name, base_url, popular, max_limit)
    df_post = PostCrawler.main()
