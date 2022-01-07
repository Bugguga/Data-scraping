import requests
import pandas as pd
import json
import os
import os.path
import urllib.request
import time
import csv


# config
page = 1
api_url = 'https://data.42matters.com/api/v3.0/android/apps/top_google_charts.json'
category_list = pd.read_json(
    r"path_to_top_chart_categories.json", encoding="utf8")
access_token = 'XXXXXXXXXXXXXXXXXXXXXXXXXXX'
country = "TH"
date = 'DD-MM-YYYY'
dic = []


def createCSV(cat_key, country, access_token, date, page):
    URL = "https://data.42matters.com/api/v3.0/android/apps/top_google_charts.json?list_name=topselling_free&cat_key=" + \
        cat_key + "&country=" + country + "&limit=100&access_token=" + \
        access_token + "&page=" + str(page) + "&date=" + date
    print(URL)
    r = requests.get(url=URL)
    data = r.json()
    try:
        d = data["app_list"]
        for app in d:
            each = []
            try:
                each.append(app["title"])
            except:
                each.append("-")
            try:
                each.append(app["category"])
            except:
                each.append("-")
            try:
                each.append(app["privacy_policy"])
            except:
                each.append("-")
            if(each != []):
                dic.append(each)
            time.sleep(0.5)
            print(each)
    except:
        pass


def getAllLink(cat_keys, date):
    for c in cat_keys['categories']:
        page = 1
        createCSV(c['cat_key'], country, access_token, date, page)
    file_path = r'path_to_save_the_file/AllLink.csv'
    with open(file_path, 'w', newline='', encoding='utf-8') as fp:
        header = ['apk_name', 'category', 'privacy']
        writer = csv.writer(fp, delimiter=",")
        writer.writerow(header)
        writer.writerows(dic)
    fp.close()


getAllLink(category_list, date)
