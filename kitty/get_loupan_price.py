# coding:utf-8
import requests
from lxml import etree

from kitty.common_utils import save_csv

"""爬取目标网站"""
file_name = "loupan_price_error_info"


def spider(url):
    try:
        header = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) \
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"}
        response = requests.get(url=url, headers=header, timeout=10)
        return response.text
    except Exception as ex:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~有异常了")
        # logging.exception(ex)
        print(ex)


"""解析html源码，提取楼盘参数"""


def spider_detail(url):
    response_text = spider(url)
    if response_text is None:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~价格获取页面请求异常 url:" + url)
        get_price_fail_url_list = list()
        get_price_fail_url_list.append("价格获取页面请求异常 url")
        get_price_fail_url_list.append(url)
        save_csv("get_price_fail_url_list", get_price_fail_url_list)
        return
    # print(response_text)
    sel = etree.HTML(response_text)
    try:
        for loupan_num in range(1, 21):  # 每页楼盘最多20个，range函数左包右不包所以是（1，21）
            total_price_xpath_prefix = '//*[@id="root"]/main/div[3]/div[1]/ul/li[%d]/div/div[2]/p' % loupan_num;
            total_price_unit_eles = sel.xpath('%s/text()' % total_price_xpath_prefix)
            total_price_eles = sel.xpath('%s/strong' % total_price_xpath_prefix)
            average_price_eles = sel.xpath(
                '//*[@id="root"]/main/div[3]/div[1]/ul/li[%d]/div/div[2]/p[2]' % loupan_num)
            loupan_name_eles = sel.xpath('//*[@id="root"]/main/div[3]/div[1]/ul/li[%d]/div/h4/a' % loupan_num)
            loupan_name = '--'
            if len(loupan_name_eles) > 0:
                loupan_name = loupan_name_eles[0].text
                print(loupan_name)

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
                print("当前楼盘暂无售价 url:" + url + " 当前楼盘：" + loupan_name)
                # print(response_text)

            print("均价：" + loupan_average_price + " 总价：" + loupan_total_price)
            average_price = change_to_int(loupan_average_price)
            total_price = change_to_int(loupan_total_price)
            if average_price != -1 and average_price <= 1000:
                save_price_error_info("平均价格异常", url, average_price, total_price)
                print("---------------------------->>>>>>>>>>>>>>平均价格异常 url:" + url + " average_price：" + str(
                    average_price))

            if total_price != -1 and total_price <= 10:
                save_price_error_info("总价异常", url, average_price, total_price)
                print("---------------------------->>>>>>>>>>>>>>总价异常 url:" + url + " total_price：" + str(total_price))
    except Exception as ex:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~解析dom树 有异常了")
        # logging.exception(ex)
        print(ex)


def change_to_int(price):
    try:
        average_price = int(price)
        return average_price
    except Exception as ex:
        return -1


def save_price_error_info(tips, url, average_price, total_price):
    info_list = list();
    info_list.append(tips)
    info_list.append(url)
    info_list.append(average_price)
    info_list.append(total_price)
    save_csv(file_name, info_list);


spider_detail("https://baoding.fangdd.com/loupan/?pageNo=7")
