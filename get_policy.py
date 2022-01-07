
import re
import string
import numpy
import multiprocessing
import urllib.request
from bs4 import BeautifulSoup
import requests
from socket import error as SocketError
import errno
import pandas as pd
import os
import numpy as np
import csv
from bs4.element import Comment


def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'div', 'head', 'title', 'a', 'option', 'link']:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8'))):
        return False
    elif re.match('<!.*>', str(element.encode('utf-8'))):
        return False
    elif re.match('\s', str(element.encode('utf-8'))):
        return False
    return True


num_cores = multiprocessing.cpu_count()
print(num_cores)
apk_pp = pd.read_csv(
    r'C:\Users\nat14\OneDrive\เอกสาร\indiv\AllLink.csv')
n = len(apk_pp)
print(n+1)
core = 0
arg_instances = []
for i in range(num_cores):
    arg_instances.append(
        (i*int(n/num_cores), (i+1)*int(n/num_cores), core))
    core += 1


def process(arg):
    start, stop, core = arg
    print('Core '+str(core)+' start!')
    df_privacy = []
    apk_name = []
    category = []
    with open(r'C:\Users\nat14\OneDrive\เอกสาร\indiv\AllLink.csv', 'r', encoding="utf8") as csvfile:

        apk_pp = csv.reader(csvfile)
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
        headers = {'User-Agent': user_agent}
        c = start
        for row in list(apk_pp)[start:stop]:
            print(c)
            print(row[2])
            if row[2] != '-':
                try:
                    request = urllib.request.Request(
                        row[2], None, headers)  # The assembled request
                    response = urllib.request.urlopen(request)
                    data = response.read()  # The data u need
                    soup = BeautifulSoup(data, 'html.parser')
                    soup = soup.find_all(text=True)
                    result = filter(visible, soup)
                    text = ' '.join([e for e in list(result)])
                    df_privacy.append(text)
                    apk_name.append(row[0])
                    category.append(row[1])
                except urllib.error.URLError:
                    df_privacy.append('-')
                    apk_name.append(row[0])
                    category.append(row[1])
                except SocketError as e:
                    if e.errno != errno.ECONNRESET:
                        df_privacy.append('-')
                        apk_name.append(row[0])
                        category.append(row[1])
                    pass
                except UnicodeEncodeError:
                    print(UnicodeEncodeError)
                    df_privacy.append('-')
                    apk_name.append(row[0])
                    category.append(row[1])
                except Exception:
                    print(Exception)
                    df_privacy.append('-')
                    apk_name.append(row[0])
                    category.append(row[1])
            else:
                df_privacy.append('-')
                apk_name.append(row[0])
                category.append(row[1])
            c += 1
    print('Core '+str(core)+' end!')
    return df_privacy, apk_name, category


if __name__ == '__main__':
    p = multiprocessing.Pool(num_cores)

    df_privacy = []
    apk_name = []
    category = []

    for x, y, n in p.map(process, arg_instances):
        df_privacy.extend(x)
        apk_name.extend(y)
        category.extend(n)

    lst_apk_name = numpy.array(apk_name)
    lst_privacy = numpy.array(df_privacy)
    lst_category = numpy.array(category)

    df_apk_name = pd.DataFrame(lst_apk_name, columns=["apk_name"])
    df_privacy_text = pd.DataFrame(lst_privacy, columns=["text"])
    df_category = pd.DataFrame(lst_category, columns=["category"])

    full_df = pd.concat([df_apk_name, df_privacy_text, df_category], axis=1)

    full_df.text = full_df.text.apply(lambda x: x.lower())
    full_df.text = full_df.text.apply(lambda x: x.translate(
        str.maketrans('', '', string.punctuation)))
    full_df.text = full_df.text.apply(
        lambda x: x.translate(str.maketrans('', '', string.digits)))

    def clean_text(x): return re.sub(r"\<(.*?)\>", "", x)
    def clean_text2(x): return re.sub(r"\/*(.)\*/", " ", x)
    def clean_text3(x): return re.sub(r'\W', ' ', x)
    def clean_text4(x): return re.sub(r'\s+br\s+', ' ', x)
    def clean_text5(x): return re.sub(r'\s+[a-z]\s+', ' ', x)
    def clean_text6(x): return re.sub(r'^b\s+', '', x)
    def clean_text7(x): return re.sub(r'\s+', ' ', x)

    full_df.text = full_df.text.apply(clean_text4).apply(
        clean_text7).apply(clean_text).apply(clean_text5)

    full_df.to_csv(
        'privacy_policy_with_clean_text_and_label.csv', encoding="utf8")
