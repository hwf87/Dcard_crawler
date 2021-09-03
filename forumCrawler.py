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


def get_all_forums_and_upload():
    url_path = 'https://www.dcard.tw/service/api/v2/forums'
    df = get_df_from_dcard_api(url_path)
    df = df[['id', 'alias', 'name', 'subscriptionCount', 'postCount', 'isSchool', 'createdAt']]
    df.postCount = df.postCount.apply(lambda x : x['last30Days'])
    df.createdAt = df.createdAt.apply(lambda x : x[:10])
    # uploadDB(df, 'dcard_forums')
    return df
