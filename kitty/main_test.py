import time
from kitty.get_loupan_city import get_city_name_list
from kitty.get_loupan_price import spider_detail
from kitty.get_loupan_url import get_list_page_url


def get_price(city_name_list):
    if city_name_list is None:
        print("城市列表获取为空，终止流程")
        return
    print("共计%d个城市" % len(city_name_list))
    """csv首列写入"""
    # save_csv(["loupan_average_price", "loupan_total_price"])

    for city_name in city_name_list:
        page_url_list = get_list_page_url(city_name)
        if page_url_list is None:
            continue

        time.sleep(1)
        for pageUrl in page_url_list:
            spider_detail(pageUrl)
            time.sleep(2)


city_name_list = get_city_name_list()
get_price(city_name_list)
