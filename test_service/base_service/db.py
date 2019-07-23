from yt_base.tools import YTHbase, YTRedis, YTPhoenix,YTMysql
from config import global_param
from collections import OrderedDict

def query_mysql_data(sql):
    mysql = YTMysql('10.66.223.141', 'admin', 'admin','local_flow', 33306)
    data = mysql.fetchone(sql)
    return data

result = query_mysql_data('select * from tf_0200_7 limit 1')
print(result)

def query_phoenix_data(sql, format_table=False):
    result = []
    phoenix = YTPhoenix(global_param.test_env['3.0']['phoenix_server'])
    all_data = phoenix.query_fetchmany(sql)

    if not format_table:
        return all_data

    header = OrderedDict()

    for key in all_data[0]:
        header[key] = len(key)

    for each in all_data:
        for key in header:
            if len(str(each[key])) > header[key]:
                header[key] = len(str(each[key]))

    title = ''
    for key in header:
        item = '%(key)-{}s'.format(header[key]+5) % {'key': key}
        title += item

    result.append('=' * len(title))
    result.append(title)
    result.append('-' * len(title))
    for each in all_data:
        line_value = ''
        for key in header:
            if each[key] is None:
                value = '-'
            else:
                value = each[key]
            item = '%(value)-{}s'.format(header[key]+5) % {'value': value}
            line_value += item
        result.append(line_value)
    result.append('=' * len(title))
    return result


def query_redis_data(express):
    redis = YTRedis(global_param.test_env['3.0']['redis_server'])
    tmp = express.strip().split(' ')
    if len(tmp) != 2:
        return 'Express is not correct'

    key = tmp[0]
    value = tmp[1]
    result = []

    if hasattr(redis, key):
        result.append(eval('redis.' + key)(value))
        return result
    else:
        return 'Express is not supported'

