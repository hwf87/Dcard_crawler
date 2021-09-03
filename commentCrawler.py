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


class commentCrawler:
    def __init__(self, database_username, database_password, database_ip, database_name, base_url, popular, max_limit):
        self.database_username = database_username
        self.database_password = database_password
        self.database_ip = database_ip
        self.database_name = database_name
        self.base_url = base_url
        self.popular = popular
        self.max_limit = max_limit
    def get_postid_list(self, MysqlDatabase, sql):
        df = MysqlDatabase.select_table(sql)
        postid_list = df['postid'].tolist()
        return postid_list
    def get_comment(self, DcardApi, postid):
        df = DcardApi.get_Dcard_posts_comments(postid)
        return df
    def main(self):
        '''
        實作
        '''
        MysqlDatabase = mysqlDatabase(self.database_username, self.database_password, self.database_ip, self.database_name)
        DcardApi = dcardApi(self.base_url, self.popular, self.max_limit)
        ## 文章列表
        sql = '''SELECT distinct dp.postid, dp.title 
        FROM Bigdata.dcard_posts dp
        where dp.commentCount >= 30
        and not exists(
            select id from Bigdata.dcard_comments dc where dc.id = dp.postid
        )
        '''
        postid_list = self.get_postid_list(MysqlDatabase, sql)
        print('length of postid_list: ', len(postid_list))
        ## 打api拿留言
        df_comment = pd.DataFrame(columns=['id', 'anonymous', 'postId', 'createdAt', 'floor', 'content', 'likeCount', 'withNickname', 'gender', 'school', 'department'])
        time_start = time.time() 
        for postid in postid_list:
            time.sleep(1)
            time_cost = time.time() - time_start
            if time_cost >= 3600:
                break
            try:
                print(postid)
                df_comment = pd.concat([df_comment, self.get_comment(DcardApi, postid)])
            except:
                print('connection abort, need some break')
                time.sleep(5)
                continue
        print('total time cost(sec): ', time_cost)
        df_comment = df_comment.fillna('')[['id', 'anonymous', 'postId', 'createdAt', 'floor', 'content', 'likeCount', 'withNickname', 'gender', 'school', 'department']]
        MysqlDatabase.upsert_table(df_comment, table_name='dcard_comments')
        return df_comment


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
    CommentCrawler = commentCrawler(database_username, database_password, database_ip, database_name, base_url, popular, max_limit)
    df_comment = CommentCrawler.main()


