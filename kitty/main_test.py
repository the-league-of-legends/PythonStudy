from kitty.get_loupan_city import get_city_name_list
from kitty.get_loupan_url import get_list_page_url

city_name_list = get_city_name_list()
for city_name in city_name_list:
    page_url_list = get_list_page_url(city_name)
    for pageUrl in page_url_list:
        print("eeeeeeezzzz"+pageUrl)
