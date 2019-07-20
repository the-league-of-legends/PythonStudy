# coding:utf-8
import requests
from lxml import etree
import time
import csv
import logging

"""爬取目标网站"""


def spider(url):
    try:
        header = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) \
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"}
        response = requests.get(url=url, headers=header)
        return response.text
    except Exception as ex:
        logging.exception(ex)
        print(ex)


"""解析html源码，提取楼盘参数"""


def spider_detail(url):
    response_text = spider(url)
    # print(response_text)
    sel = etree.HTML(response_text)
    try:
        with open("loupan.csv", \
                  "a", encoding="utf-8-sig", newline="")as f:
            writer = csv.writer(f)
            for loupan_num in range(1, 21):  # 每页楼盘最多20个，range函数左包右不包所以是（1，21）
                total_price_xpath_prefix = '//*[@id="root"]/main/div[3]/div[1]/ul/li[%d]/div/div[2]/p' % loupan_num;
                total_price_unit_eles = sel.xpath('%s/text()' % total_price_xpath_prefix)
                total_price_eles = sel.xpath('%s/strong' % total_price_xpath_prefix)
                average_price_eles = sel.xpath(
                    '//*[@id="root"]/main/div[3]/div[1]/ul/li[%d]/div/div[2]/p[2]' % loupan_num)
                loupan_total_price = '--'
                loupan_average_price = '--'
                if len(total_price_eles) > 0:
                    unit = total_price_unit_eles[0]
                    loupan_total_price = total_price_eles[0].text
                    if '元/㎡' in unit:  # 包含此字段，则代表当前获取的总价占位其实是均价
                        loupan_average_price = loupan_total_price
                        loupan_total_price = '--'
                        print("当前楼盘只有均价：" + loupan_average_price)
                    else:  # 如果不包含此字符，则说明总价就是总价，均价需要额外获取
                        if len(average_price_eles) > 0:
                            loupan_average_price = average_price_eles[0].text.strip("元/㎡")  # 去除单价的单位
                            print("均价：" + loupan_average_price)
                        else:
                            print("未获取到均价 url:" + url)
                else:
                    print("当前楼盘暂无售价 url:" + url)

                print("均价：" + loupan_average_price + " 总价：" + loupan_total_price)
                average_price = change_to_int(loupan_average_price)
                total_price = change_to_int(loupan_total_price)
                if average_price != -1 and average_price <= 1000:
                    print("---------------------------->>>>>>>>>>>>>>平均价格异常 url:" + url + " average_price：" + str(average_price))

                if total_price != -1 and total_price <= 10:
                    print("---------------------------->>>>>>>>>>>>>>总价格异常 url:" + url + " total_price：" + str(total_price))

                    # loupan_data = [loupan_average_price, loupan_total_price]
                    # writer.writerow(loupan_data)
    except Exception as ex:
        logging.exception(ex)
        print(ex)


def change_to_int(price):
    try:
        average_price = int(price)
        return average_price
    except Exception as ex:
        return -1


"""将数据按行存储在csv文件中"""


def save_csv(loupan_data):
    try:
        with open("loupan.csv", \
                  "a", encoding="utf-8-sig", newline="")as f:
            writer = csv.writer(f)

            writer.writerow(loupan_data)
    except Exception as ex:
        logging.exception(ex)
        print(ex)


# save_csv(["loupan_average_price", "loupan_total_price"])
spider_detail("https://anshun.fangdd.com/loupan/?pageNo=1")
