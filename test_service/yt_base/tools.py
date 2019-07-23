__author__ = 'zouhl'

'''
===================================================================
=====================       YTRedis        ========================
===================================================================
'''
try:
    import rediscluster
except:
    raise ImportError("缺少扩展包redis-py-cluster")


class YTRedis(object):
    def __init__(self, redis_nodes):
        startup_nodes = []
        for each in str(redis_nodes).split(","):
            redis_node = dict()
            host = each.split(":")[0].strip()
            port = each.split(":")[1].strip()
            redis_node["host"] = host
            redis_node["port"] = port
            startup_nodes.append(redis_node)
        self.rc = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

    def set(self, key, value):
        self.rc.set(key, value)

    def get(self, key):
        return self.rc.get(key)

    def delete(self, key):
        self.rc.delete(key)

    def hgetall(self, name):
        return self.rc.hgetall(name)

    def hget(self, name, key):
        return self.rc.hget(name, key)

    def hset(self, name, key, value):
        self.rc.hset(name, key, value)

    def hdel(self, name, key):
        self.rc.hdel(name, key)

    def keys(self, pattern):
        return self.rc.keys(pattern=pattern)

'''
===================================================================
=====================       YTRabbitMQ     ========================
===================================================================
'''
try:
    import pika
except:
    raise ImportError("缺少扩展包pika")


class YTRabbitMQ(object):
    def __init__(self, host, username, password, port=5672):
        if pika.__version__ > "0.10.0":
            pika.ConnectionParameters.DEFAULT_CREDENTIALS.username = username
            pika.ConnectionParameters.DEFAULT_CREDENTIALS.password = password
        else:
            pika.ConnectionParameters.DEFAULT_USERNAME = username
            pika.ConnectionParameters.DEFAULT_PASSWORD = password
        param = pika.ConnectionParameters(host=host, port=port)
        conn = pika.BlockingConnection(param)
        # conn = pika.BaseConnection(param)

        self.mq = conn.channel()

    def get_message(self, queue, count=1):
        msg_list = []
        index = 0

        method_frame, header_frame, body = self.mq.basic_get(queue)

        while method_frame:
            msg = body.decode()
            msg_list.append(msg)
            self.mq.basic_ack(method_frame.delivery_tag)
            if count == 1:
                return msg

            index += 1
            if index >= count:
                break
            method_frame, header_frame, body = self.mq.basic_get(queue)

        return msg_list

    def get_all_message(self, queue):
        # msg_list = []
        method_frame, header_frame, body = self.mq.basic_get(queue)

        while method_frame:
            yield body.decode()
            # msg_list.append(msg)
            self.mq.basic_ack(method_frame.delivery_tag)
            method_frame, header_frame, body = self.mq.basic_get(queue)
        # return msg_list

    def publish_a_message(self, exchange, msg, routing_key=None):
        if routing_key is None:
            routing_key = exchange
        self.mq.basic_publish(exchange=exchange, routing_key=routing_key, body=msg)

    def purge(self, queue):
        self.mq.queue_purge(queue)

    def create_a_queue(self, queue, exchange=None):
        self.mq.queue_declare(queue, auto_delete=True)
        if exchange is not None:
            self.mq.queue_bind(queue, exchange, routing_key=exchange)

    def delete_a_queue(self, queue):
        self.mq.queue_delete(queue)

    def close(self):
        self.mq.close()

'''
===================================================================
=====================       YTKafka        ========================
===================================================================
'''
try:
    from kafka import KafkaProducer
    from kafka import KafkaConsumer
except:
    raise ImportError("缺少扩展包kafka")


class YTKafkaProducer(object):
    def __init__(self, servers, topic=None):
        bootstrap_servers = []
        for each in str(servers).split(","):
            bootstrap_servers.append(each.strip())

        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic

    def send_data(self, data, topic=None):
        if topic is None:
            topic = self.topic
        self.producer.send(topic, data)

    def flush(self):
        self.producer.flush()


class YTKafkaConsumer(object):
    def __init__(self, servers, topic, group_id='test', consumer_timeout_ms=120000):
        bootstrap_servers = []
        for each in str(servers).split(","):
            bootstrap_servers.append(each.strip())

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            consumer_timeout_ms=consumer_timeout_ms)

    def consume_data(self):
        try:
            for message in self.consumer:
                yield message.value
        except KeyboardInterrupt as e:
            print(e)

    def close(self):
        self.consumer.close()


'''
===================================================================
=====================       YTHbase        ========================
===================================================================
'''
import happybase


class YTHbase(object):
    def __init__(self, server):
        self.conn = happybase.Connection(server)

    def get_row_data(self, table_name, rowkey, family=None):
        table = self.conn.table(table_name)
        row_data = table.row(rowkey)
        if family is None:
            return row_data
        return row_data[str(family).encode()]

    def scan_row_data(self, table_name, rowkey, family=None):
        row_start = rowkey + '00'
        row_stop = rowkey + '99'
        table = self.conn.table(table_name)
        for row_data in table.scan(row_start, row_stop):
            if family is None:
                return row_data
            return row_data[str(family).encode()]



'''
===================================================================
=====================          YTES        ========================
===================================================================
'''
from elasticsearch import Elasticsearch


class YTES:
    def __init__(self, host, port=9200):
        self.es = Elasticsearch(host, port=port)

    def get(self, index, doc_type, id_):
        return self.es.get(index=index, doc_type=doc_type, id=id_)

    # modified by zhangp [2018-12-6]
    def delete(self, index, doc_type, id_):
        return self.es.delete(index=index, doc_type=doc_type, id=id_)

    def search(self, index, body):
        return self.es.search(index=index, body=body)

    # modified by zhangp [2018-12-6]
    def delete_by_query(self, index, body):
        self.es.delete_by_query(index=index, body=body)


'''
===================================================================
=====================          PHOENIX        =====================
===================================================================
'''
class YTPhoenix:
    def __init__(self, host, port=8765):
        database_url = 'http://{0}:{1}/'.format(host, port)
        conn = phoenixdb.connect(database_url, autocommit=True)
        self.cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
        self.map_field = self._load_map_file()

    def _load_map_file(self):
        map_file = r'yt_base\phoenix_map\phoenix_map'
        field_map = dict()
        with open(map_file) as mf:
            for line in mf:
                if not line:
                    continue
                protocol, field_name, column_name = line.strip().upper().split('|')
                if protocol not in field_map:
                    field_map[protocol] = dict()

                field_map[protocol][column_name] = field_name

        return field_map

    def _sql_parse(self, sql):
        sql = sql.replace('\n', ' ').replace('\t', ' ')
        words = []
        word = ''
        tail = False
        for c in sql:
            if c in (" ", ",", "'", "(", ")", "=", "+", "-", ";", "\n", "\t"):
                if word:
                    words.append(word)
                word = ''
                words.append(c)
                tail = True
            else:
                word += c
                tail = False

        if not tail:
            words.append(word)

        return words

    def _map_sql(self, sql, table_name):
        protocol = table_name[3:].upper()
        if protocol not in self.map_field:
            return sql

        table_field_map = self.map_field[protocol]
        new_sql = ''
        parsed_sql = self._sql_parse(sql)
        parsed_sql_upper = []

        for each in parsed_sql:
            parsed_sql_upper.append(each.upper())

        for field in table_field_map:
            if table_field_map[field] in parsed_sql_upper:
                index = parsed_sql_upper.index(table_field_map[field])
                parsed_sql[index] = field

        for each in parsed_sql:
            new_sql += each

        return new_sql

    def _map_result(self, result, table_name):
        protocol = table_name[3:].upper()
        if protocol not in self.map_field:
            return result

        table_field_map = self.map_field[protocol]
        result_mapped = dict()

        for each in result:
            if each in table_field_map:
                result_mapped[table_field_map[each]] = result[each]
            else:
                result_mapped[each] = result[each]

        return result_mapped

    def _map_result_list(self, result_list, table_name):
        mapped_result_list = []
        for result in result_list:
            mapped_result_list.append(self._map_result(result, table_name))

        return mapped_result_list

    def query(self, sql):
        sql = sql.strip()
        table_name = sql[sql.upper().find('TF_'):].split(' ')[0]
        sql = self._map_sql(sql, table_name)
        self.cursor.execute(sql)
        return self._map_result(self.cursor.fetchone(), table_name)

    def query_fetchmany(self, sql):
        sql = sql.strip()
        table_name = sql[sql.upper().find('TF_'):].split(' ')[0]
        sql = self._map_sql(sql, table_name)
        self.cursor.execute(sql)
        if 'limit' in sql:
            return self._map_result_list(self.cursor.fetchmany(), table_name)
        else:
            return self._map_result_list(self.cursor.fetchmany(10), table_name)

'''
===================================================================
=====================          ORACLE         =====================
===================================================================
'''
import cx_Oracle
class YTOracle:
    def __init__(self, server, username, password):
        orcl_conn = cx_Oracle.connect(username, password, server)
        self.cur = orcl_conn.cursor()

    def fetchone(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchone()

'''
== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == =
== == == == == == == == == == =          MySQL              == == == == == == == == == == =
== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == =
'''
# import pymysql
# class YTMysql:
#     def __init__(self, host, port, username, password, dbname,charset="utf8"):
#         mysql_conn = pymysql.connect(host, port, username, password, dbname,charset="utf8")
#         self.cur = mysql_conn.cursor()
#
#     def fetchone(self, sql):
#         self.cur.execute(sql)
#         return self.cur.fetchone()

'''
===================================================================
=====================          MYSQL         =====================
===================================================================
'''
import pymysql
class YTMysql:
    def __init__(self,host, user, password, db, port,charset="utf8"):
        mysql_conn = pymysql.connect(host, user, password, db, port,charset="utf8")
        self.cursor = mysql_conn.cursor()
        self.map_field = self._load_map_file()

    def _load_map_file(self):  # 加载map，
        map_file = r'yt_base\mysql_map\mysql_map'
        field_map = dict()
        with open(map_file) as mf:
            for line in mf:
                if not line:
                    continue
                protocol, field_name, column_name = line.strip().upper().split('|')   # eg:['0001', 'ACKID', 'C0']
                if protocol not in field_map:
                    field_map[protocol] = dict()

                field_map[protocol][column_name] = field_name
        return field_map   # 返回{protocol:{column_name:field_name}}

    def _sql_parse(self, sql):   # sql解析
        sql = sql.replace('\n', ' ').replace('\t', ' ')
        words = []
        word = ''
        tail = False
        for c in sql:
            if c in (" ", ",", "'", "(", ")", "=", "+", "-", ";", "\n", "\t"):
                if word:
                    words.append(word)
                word = ''
                words.append(c)
                tail = True
            else:
                word += c
                tail = False

        if not tail:
            words.append(word)

        return words

    def _map_sql(self, sql, table_name):
        protocol = table_name[3:7].upper()  #tf_0200_7
        if protocol not in self.map_field:
            return sql

        table_field_map = self.map_field[protocol]
        new_sql = ''
        parsed_sql = self._sql_parse(sql)
        parsed_sql_upper = []

        for each in parsed_sql:
            parsed_sql_upper.append(each.upper())

        for field in table_field_map:
            if table_field_map[field] in parsed_sql_upper:
                index = parsed_sql_upper.index(table_field_map[field])
                parsed_sql[index] = field

        for each in parsed_sql:
            new_sql += each

        return new_sql

    def _map_result(self, result, table_name):
        protocol = table_name[3:7].upper()
        if protocol not in self.map_field:
            return result

        table_field_map = self.map_field[protocol]
        result_mapped = dict()

        for each in result:
            if each in table_field_map:
                result_mapped[table_field_map[each]] = result[each]
            else:
                result_mapped[each] = result[each]

        return result_mapped

    def _map_result_list(self, result_list, table_name):
        mapped_result_list = []
        for result in result_list:
            mapped_result_list.append(self._map_result(result, table_name))

        return mapped_result_list

    def query(self, sql):
        sql = sql.strip()
        table_name = sql[sql.upper().find('TF_'):].split(' ')[0]
        sql = self._map_sql(sql, table_name)
        self.cursor.execute(sql)
        return self._map_result(self.cursor.fetchone(), table_name)

    def query_fetchmany(self, sql):
        sql = sql.strip()
        table_name = sql[sql.upper().find('TF_'):].split(' ')[0]
        sql = self._map_sql(sql, table_name)
        self.cursor.execute(sql)
        if 'limit' in sql:
            return self._map_result_list(self.cursor.fetchmany(), table_name)
        else:
            return self._map_result_list(self.cursor.fetchmany(10), table_name)

    def fetchone(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()

if __name__ == '__main__':
    # o = YTOracle('10.66.221.42:1521/bspdb', 'bsp', 'nihaoBSP2')
    # print(o.fetchone("select * from tb_check_card_record where obj_id = '53467B9115834E47A5FB921DD0E8D7C2' order by row_create_time desc"))
    m = YTMysql(host="10.66.223.141", user="admin", password="admin",db="local_flow",port=33306,charset='utf8')
    query = m.fetchone()


# if if __name__ == '__main__':
#     m = YTMysql('10.66.223.141','33306','admin','admin','local_flow')
#     print(m.fetchone("s"))