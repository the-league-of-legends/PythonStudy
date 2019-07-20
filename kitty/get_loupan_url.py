# coding:utf-8
import requests
from lxml import etree
import time
import csv
import logging
from pyquery import PyQuery as pq

from kitty.get_loupan_price import spider_detail

"""根据城市得到起始URL，并得到总套数，为分页做准备"""


def get_list_page_url(city_name):
    start_url = "https://{}.fangdd.com/loupan/".format(city_name)

    headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) \
                 AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"}
    try:

        response = requests.get(start_url, headers=headers)
        response_text = response.text
        # print(response_text)
        sel = etree.HTML(response_text)
        total_number = sel.xpath('// *[ @ id = "root"] / main / div[3] / div[1] / p / strong')
        if len(total_number) > 0:
            total_num = int(total_number[0].text);
            print(total_num)
        else:
            print("错误地址：" + start_url)
            error_list = list();
            error_list.append("url:" + start_url)
            error_list.append("response:" + response_text)
            save_csv(error_list)
            return

        if total_num % 20 == 0:
            total_page = total_num // 20

        else:
            total_page = total_num // 20 + 1

        if total_page == 0:
            total_page = 1

        if total_page > 100:
            total_page = 100

        page_url_list = list()

        for i in range(1, total_page + 1):
            url = start_url + "?pageNo=" + str(i)
            print(url)
            page_url_list.append(url)
        return page_url_list

    except Exception as ex:
        logging.exception(ex)
        print(ex)


def save_csv(error_list):
    try:
        with open("error_info.csv", \
                  "a", encoding="utf-8-sig", newline="")as f:
            writer = csv.writer(f, dialect="excel")

            writer.writerow(error_list)
    except Exception as ex:
        logging.exception(ex)
        print(ex)

# get_list_page_url("shenzhen")
