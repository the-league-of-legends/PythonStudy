# coding=utf-8
# 获取城市列表（简单get请求+json解析）
import requests
import json

from kitty.get_loupan_url import get_list_page_url


def get_city_name_list():

    try:

        city_name_list = list()
        r = requests.get(url='https://suzhou.fangdd.com/component/api/data/fetchCityList', params={'baseUrl': '/loupan/'})
        r.encoding = 'utf-8'  # 得到的结果转换为编码
        jsonStr = r.text
        jsonObject = json.loads(jsonStr)  # 将已经编码的Json串解码为python对象
        letterCityMap = jsonObject.get("data").get("letterCityMap")
        for item in letterCityMap.items():
            for temp in item[1]:
                url = temp.get("url")
                cityName = url.replace('/', '')
                cityName = cityName.replace('loupan', '')
                print(cityName)
                city_name_list.append(cityName)
        return city_name_list

    except Exception as ex:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~有异常了")
        print(ex)


