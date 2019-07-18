# coding=utf-8
# 获取城市列表（简单get请求+json解析）
import requests
import json

# 带参数的GET请求
r = requests.get(url='https://suzhou.fangdd.com/component/api/data/fetchCityList', params={'baseUrl': '/loupan/'})
r.encoding = 'utf-8'  # 得到的结果转换为编码
print(r.status_code)  # 获取返回状态
print(r.url)
print(r.text)  # 打印解码后的返回数据

jsonStr = r.text
jsonObject = json.loads(jsonStr)  # 将已经编码的Json串解码为python对象
hotCityListObject = jsonObject["data"]["hotCityList"]  # 方式1
print(hotCityListObject)
# hotCityList02 = jsonObject.get("data").get("hotCityList")  # 方式2
# print(hotCityList02)
for item in hotCityListObject:
    print("text=%s,url=%s" % (item.get("text"), item.get("url")))

letterCityMap = jsonObject.get("data").get("letterCityMap")
for item in letterCityMap.items():
    print("key=%s,value=%s" % (item[0], item[1]))
    for temp in item[1]:
        print("text=%s,url=%s" % (temp.get("text"), temp.get("url")))
