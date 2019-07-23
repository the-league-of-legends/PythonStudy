from queue import Queue
import threading
import time
import datetime
import json
import struct

from include import KafkaOutput, AquilaTest, Verification
from result import TestResult
from config import global_param
from yt_cp import Proto808, Proto808B, Proto, ProtoObuRsu, ProtoZnsh, common
from yt_base import YTHbase, YTRedis, YTPhoenix, YTOracle
import rpc


_EXPECTED_FLAG = {
    'hbase': '[[E_HBASE]]',
    'pisces': '[[E_PISCES]]',
    'redis': '[[E_REDIS]]',
    'phoenix': '[[E_PHOENIX]]',
    'oracle': '[[E_ORACLE]]',
'oracle': '[[E_ORACLE]]'

}

_RECEIVED_FLAG = {
    'hbase': '[[R_HBASE]]',
    'pisces': '[[R_PISCES]]',
    'redis': '[[R_REDIS]]',
    'phoenix': '[[R_PHOENIX]]',
    'oracle': '[[R_ORACLE]]'
}

_VERIFY_TYPE = {
    'hbase': 'hbase',
    'pisces': 'pisces',
    'redis': 'redis',
    'phoenix': 'phoenix',
    'oracle': 'oracle'
}

_DELIMITER = '|'
_END = '[[end]]'

_INTEGRAL = 'integral'

'''
_hbase_row_key1: rowkey = device_no + '20' + terminal_time
_hbase_row_key2: rowkey = device_no + str(99999999999999 - int('20' + terminal_time))
'''

_hbase_row_key1 = [
    'road_spectrum_collection'
]

_hbase_row_key_mt = [
    'vehicle_mt_data'
]

_hbase_row_mt_io = [
    'vehicle_mt_io'
]

_hbase_row_key_rt = [
    'vehicle_track_record'
]

_verify_wait = 60


class SendData(object):
    def __init__(self, version, ter_type, ter_code):
        self.version = version
        self.ter_type = ter_type
        self.ter_code = ter_code
        self.result = TestResult()
        if self.ter_type in ['rsu']:
            self.cp = ProtoObuRsu(self.ter_code)
        if self.ter_type in ['jw']:
            self.cp = Proto808B(self.ter_code)
        if self.ter_type in ['znsh']:
            self.cp = ProtoZnsh(self.ter_code)
        else:
            self.cp = Proto808(self.ter_code)
        self.a_t = AquilaTest(
            global_param.test_env[version]['aquila_server'],
            global_param.aquila_port[ter_type]
        )

    def log(self, msg):
        self.result.case_log.append(msg)

    def send_data(self, data):
        data_buf = self.cp.gen_round_buffer(data)[0]
        self.log('SendData>>> ' + Proto.bytes_to_hex(data_buf))

        self.a_t.send_data(data_buf)
        return self.cp.bytes_to_hex(data_buf)


class ProtocolTest(SendData):
    def __init__(self, version, ter_type, ter_code, device_no, device_id, ter_id, test_case_list):
        super().__init__(version, ter_type, ter_code)
        self.device_no = device_no
        self.device_id = device_id
        self.ter_id = ter_id
        self.verification_data_q = Queue()
        self.terminate = threading.Event()

        self.without_output = 0

        self.hbase_table = None
        self.hbase_row_key = None
        self.redis_key = None
        self.phoenix_table = None
        self.phoenix_key = None
        self.phoenix_field = []

        self.oracle_table = None
        self.oracle_key = None
        self.oracle_field = []
        self.test_suite = self._gen_test_case(test_case_list)
        self.test_case_id = ''

    def _gen_test_case(self, test_case_list):
        test_suite = []

        for case in test_case_list:
            case_id, case_data, data_type, verify_type, verify_key, expected = case

            if data_type.lower().strip() == _INTEGRAL:
                data = case_data
            else:
                msg_id = common.gen_bcd(data_type.upper(), 2)
                data = self.cp.assemble_buf(msg_id, case_data)
            test_suite.append((case_id, data, verify_type, verify_key, expected))

        return test_suite

    def run(self):

        for test_case in self.test_suite:
            case_id, data, verify_type, verify_key, expected = test_case

            self._complete_last_test()

            if self._test_terminated():
                break

            self._start_new_test(case_id)

            self.hbase_row_key = None

            if verify_type not in _EXPECTED_FLAG:
                self.log('Verify type [{}] not supported'.format(verify_type))
                continue

            self.verification_data_q.put(_EXPECTED_FLAG[verify_type] + _DELIMITER + expected)
            actual_send_data = self.send_data(data)

            self.hbase_row_key = None
            if verify_type == 'redis':
                if self.version == '2.0':
                    if self.device_id is None:
                        self.log('Exception>>> Your client DO NOT support redis verify')
                        self.redis_key = None
                    else:
                        self.redis_key = verify_key + self.device_id
                        self.log('Verification>>> {0}|{1}'.format(verify_type, self.redis_key))
                else:
                    if self.ter_id is None:
                        self.log('Exception>>> Your client DO NOT support redis verify')
                        self.redis_key = None
                    else:
                        self.redis_key = verify_key + self.ter_id
                        self.log('Verification>>> {0}|{1}'.format(verify_type, self.redis_key))
            elif verify_type == 'phoenix':
                if self.device_id is None:
                    self.log('Exception>>> Your client DO NOT support phoenix verify')
                    self.phoenix_key = None
                else:
                    self.phoenix_table = verify_key
                    self.phoenix_key = self._get_phoenix_key(self.phoenix_table, actual_send_data)
                    self.phoenix_field = self._get_phoenix_field(expected)
                    self.log('Verification>>> {0}|{1}|{2}'.format(
                        verify_type,
                        self.phoenix_table,
                        self.phoenix_key)
                    )
            elif verify_type == 'hbase':
                self.hbase_row_key = self._get_hbase_row_key(verify_key, actual_send_data)
                self.hbase_table = verify_key
                self.log('Verification>>> {0}|{1}|{2}'.format(
                    verify_type,
                    self.hbase_table,
                    self.hbase_row_key)
                )
            elif verify_type == 'oracle':
                self.oracle_key = self._get_oracle_key(verify_key, actual_send_data)
                self.oracle_table = verify_key
                self.oracle_field = self._get_oracle_field(expected)
                self.log('Verification>>> {0}|{1}|{2}'.format(
                    verify_type,
                    self.oracle_table,
                    self.oracle_key)
                )
            else:
                pass

        self._terminate_all_test()

    def get_redis_data(self):
        server = global_param.test_env[self.version]['redis_server']
        redis = YTRedis(server)

        while not self.terminate.is_set():
            time.sleep(1)
            if self.redis_key is None:
                continue

            redis_data = redis.get(self.redis_key)
            redis.delete(self.redis_key)
            if redis_data:
                self._push_msg('redis', self.redis_key, redis_data)

    def _get_hbase_row_key(self, table, send_data):
        buf_time = self.cp.get_buf_time(send_data)
        if buf_time is None:
            return None

        if table in _hbase_row_key_mt:
            return self.device_no + str(99999999999999 - int('20' + buf_time))
        elif table in _hbase_row_mt_io:
            time_offset = self.cp.get_0f90_time_offset(send_data)
            return self.device_no + str(int('20' + buf_time) * 1000 - time_offset)
        else:
            return self.device_no + '20' + buf_time

    def get_hbase_data(self):
        server = global_param.test_env['2.0']['hbase_server']
        hbase = YTHbase(server)
        table = None
        row_key = None
        hbase_data = None

        while not self.terminate.is_set():
            time.sleep(1)
            if self.hbase_row_key is None or self.hbase_table is None:
                continue

            new_table = self.hbase_table
            new_row_key = self.hbase_row_key
            if table != new_table or row_key != new_row_key:
                hbase_data = None

            if new_table and new_row_key and not hbase_data:
                '''

                '''
                if new_table in _hbase_row_key_rt:
                    '''
                    0200协议 row_key 在 device_no + 'yyyymmddHHMMSS' 后加一个序列号[00~99]，
                    因此查询时使用了scan
                    '''
                    hbase_data = hbase.scan_row_data(new_table, new_row_key)
                    if hbase_data:
                        new_row_key_r, hbase_data = hbase_data
                        new_row_key_r = new_row_key_r.decode()
                        self._push_msg('hbase', new_table, hbase_data, new_row_key_r)
                else:
                    hbase_data = hbase.get_row_data(new_table, new_row_key)
                    if hbase_data:
                        self._push_msg('hbase', new_table, hbase_data, new_row_key)

                table = new_table
                row_key = new_row_key

    def _get_phoenix_key(self, phoenix_table, actual_send_data):
        phoenix_key = "device_id={0}".format(self.device_id)

        if phoenix_table in ['tf_fault_detail']:
            filter_field = " and REPORT_TIME(time, 'yy-MM-dd HH:mm:ss')='{}'".format(
                self._get_phoenix_time(actual_send_data)
            )
        elif phoenix_table in ['tf_0f80', 'tf_0200', 'tf_0f90']:
            filter_field = " and time=to_timestamp('{}','yy-MM-dd HH:mm:ss')".format(
                self._get_phoenix_time(actual_send_data)
            )
        else:
            filter_field = None

        if filter_field is not None:
            phoenix_key += filter_field

        phoenix_key += " Order By ROW_CREATE_TIME desc fetch first 1 rows only"

        if filter_field is None:
            phoenix_key += "--{}".format(self.test_case_id)

        return phoenix_key

    def _get_phoenix_time(self, send_data):
        buf_time = self.cp.get_buf_time(send_data)
        if buf_time is None:
            return None
        # 18-09-05 17:08:59
        return '{}-{}-{} {}:{}:{}'.format(
            buf_time[0: 2],
            buf_time[2: 4],
            buf_time[4: 6],
            buf_time[6: 8],
            buf_time[8: 10],
            buf_time[10:]
            )

    def _get_phoenix_field(self, expected_data):
        phoenix_field = []
        for each in expected_data.split(';'):
            if each.strip() == '':
                continue
            phoenix_field.append(each.strip().split('=')[0])

        phoenix_field.append('ROW_CREATE_TIME')
        return phoenix_field

    def get_phoenix_data(self):
        server = global_param.test_env['3.0']['phoenix_server']
        phoenix = YTPhoenix(server)
        phoenix_table = None
        phoenix_key = None
        phoenix_data = None
        last_row_date = ''

        while not self.terminate.is_set():
            time.sleep(1)
            if self.phoenix_key is None or self.phoenix_table is None:
                continue

            new_table = self.phoenix_table
            new_row_key = self.phoenix_key
            if phoenix_table != new_table or phoenix_key != new_row_key:
                phoenix_data = None

            if new_table and new_row_key and not phoenix_data:
                if '--' in self.phoenix_key:
                    self.phoenix_key = self.phoenix_key[0: self.phoenix_key.find('--')]
                sql = "SELECT {2} FROM {0} where {1}".format(
                    self.phoenix_table,
                    self.phoenix_key,
                    ','.join(self.phoenix_field)
                )
                phoenix_data = phoenix.query(sql)
                if phoenix_data:

                    if 'ROW_CREATE_TIME' in phoenix_data:
                        '''
                        对于查询条件中未设置时间的查询，
                        需要根据ROW_CREATE_TIME确认是否是新值
                        '''
                        row_date = phoenix_data['ROW_CREATE_TIME']
                        if row_date == last_row_date:
                            continue

                        last_row_date = row_date
                    self._push_msg('phoenix', new_table, phoenix_data, new_row_key)

                phoenix_table = new_table
                phoenix_key = new_row_key

    def _get_kafka_data(self, server, topic, group_id=None, filter_info=None):
        kafka = KafkaOutput(server, topic, group_id=group_id, filter_info=filter_info)
        for msg in kafka.get_data():
            try:
                msg = msg.decode()
            except:
                msg = self.cp.bytes_to_hex(msg)
            self._push_msg('kafka', topic, msg)

            if self.terminate.is_set():
                return

    def check_kafka_output(self, client_id, field_info):
        kafka_server_cp = global_param.test_env[self.version]['kafka_server_cp']
        kafka_server_app = global_param.test_env[self.version]['kafka_server_app']
        kafka_topic_cp = global_param.kafka_topic[self.version]['cp']
        kafka_topic_app = global_param.kafka_topic[self.version]['app']

        def gen_kafka_group_id():
            return 'automation_{}'.format(client_id)

        for topic in kafka_topic_cp:
            k_t = threading.Thread(
                target=self._get_kafka_data,
                args=(
                    kafka_server_app,
                    topic,
                    gen_kafka_group_id(),
                    field_info
                )
            )
            k_t.start()

        for topic in kafka_topic_app:
            k_t = threading.Thread(
                target=self._get_kafka_data,
                args=(
                    kafka_server_cp,
                    topic,
                    gen_kafka_group_id(),
                    field_info)
            )
            k_t.start()

    def _get_oracle_time(self, send_data):
        buf_time = self.cp.get_buf_time(send_data)
        if buf_time is None:
            return None
        # 18/09/05 17:08:59
        return '{}/{}/{} {}:{}:{}'.format(
            buf_time[0: 2],
            buf_time[2: 4],
            buf_time[4: 6],
            buf_time[6: 8],
            buf_time[8: 10],
            buf_time[10:]
            )

    def _get_oracle_key(self, oracle_table, send_data):
        '''
        select * from tb_check_card_record where obj_id = '53467B9115834E47A5FB921DD0E8D7C2' and to_char(terminal_time, 'yy/mm/dd hh24:mi:ss') = '18/04/18 15:18:59';
        :param oracle_table:
        :param send_data:
        :return:
        '''
        if oracle_table.upper() in ['TB_XNY_DEVICE_DATA']:
            obj_id_key = 'vehicle_id'
        else:
            obj_id_key = 'obj_id'

        oracle_key = "{0}='{1}' order by row_create_time desc".format(obj_id_key, self.device_id)

        return oracle_key

    def _get_oracle_field(self, expected_data):
        oracle_field = [each.split('=')[0] for each in expected_data.split(';')]
        oracle_field.append('ROW_CREATE_TIME')
        return oracle_field

    def get_oracle_data(self):
        server = global_param.test_env['2.0']['oracle_server']
        username, password = global_param.test_env['2.0']['oracle_user'].split('/')
        oracle = YTOracle(server, username, password)

        oracle_table = None
        oracle_key = None
        oracle_data = None

        while not self.terminate.is_set():
            time.sleep(1)
            if self.oracle_key is None or self.oracle_table is None:
                continue

            new_table = self.oracle_table
            new_key = self.oracle_key
            if oracle_table != new_table or oracle_key != new_key:
                oracle_data = None

            if new_table and new_key and not oracle_data:
                sql = "SELECT {2} FROM {0} where {1}".format(
                    self.oracle_table,
                    self.oracle_key,
                    ','.join(self.oracle_field)
                )
                oracle_data = oracle.fetchone(sql)
                if oracle_data:
                    self._push_msg('oracle', new_table, oracle_data, oracle_key)

                oracle_table = new_table
                oracle_key = new_key

    def verify(self):
        expected = None
        actual = None
        case_result = None
        verify_type = None
        verification = Verification(self.result)

        while True:
            msg = self.verification_data_q.get()
            if msg == _END:
                break

            is_expected_msg = False
            for each in _EXPECTED_FLAG:
                if _EXPECTED_FLAG[each] in msg:
                    expected = msg.split(_DELIMITER)[1]
                    if not expected:
                        self.result.test_no_expected()
                    case_result = None

                    verify_type = each
                    is_expected_msg = True

            if not is_expected_msg:
                actual = msg

            if actual:
                if not expected:
                    continue

                if not case_result:
                    case_result = None

                    actual = msg.split(_DELIMITER)[1]
                    for each in _RECEIVED_FLAG:
                        if verify_type == each and _RECEIVED_FLAG[each] in msg:
                            case_result = verification.verify_data(verify_type, expected, actual)

                    if case_result is not None:
                        if isinstance(case_result, str) or isinstance(case_result, list):
                            self.result.test_failed(case_result)
                        else:
                            self.result.test_success()

                actual = None

    def _start_new_test(self, case_id):
        self.test_case_id = case_id
        self.result.test_start(case_id)
        self.log('StartTest>>> ' + case_id)

    def _test_terminated(self):
        if self.terminate.is_set():
            return True

    def _terminate_all_test(self):
        self._complete_last_test()
        self.verification_data_q.put(_END)
        self.terminate.set()

    def _complete_last_test(self):
        sleep_time = 0
        while self.result.case_state == 0 and sleep_time < _verify_wait:
            time.sleep(1)
            sleep_time += 1

        if self.result.case_state == 0:
            self.result.test_failed(self._no_output_error())

        self.log('TestCompleted.')

    def _push_msg(self, msg_type, topic, msg, msg_id=None):

        def _bytes_to_array(bytes):
            r = []
            for each in bytes:
                r.append(int(each))
            return r

        def _bytes_to_value(bytes):
            if len(bytes) == 4:
                return struct.unpack('>L', bytes)[0]
            # elif len(bytes) >= 8:
            #     hex(int(bytes.decode()))
            else:
                return _bytes_to_array(bytes)

        def _hbase_data_transport(hbase_data):
            hbase_data_formatted = dict()
            for each in hbase_data:
                formatted_key = each.decode()
                formatted_key = formatted_key.split(':')[1]
                if topic in _hbase_row_mt_io:
                    hbase_data_formatted[formatted_key] = hbase_data[each].decode(encoding='latin')
                elif topic in _hbase_row_key_mt or topic in _hbase_row_key_rt:
                    hbase_data_formatted[formatted_key] = _bytes_to_array(hbase_data[each])
                else:
                    hbase_data_formatted[formatted_key] = _bytes_to_value(hbase_data[each])

            return json.dumps(hbase_data_formatted)

        recv_type = _RECEIVED_FLAG[msg_type]
        msg_flag = msg_type

        if msg_type == 'hbase':
            msg_formatted = _hbase_data_transport(msg)
        else:
            if isinstance(msg, dict):
                for each in msg:
                    if isinstance(msg[each], datetime.datetime):
                        msg[each] = msg[each].strftime('%y-%m-%d %H:%M:%S')
                msg_formatted = json.dumps(msg)
            else:
                msg_formatted = msg

        self.verification_data_q.put('{0}{1}{2}'.format(recv_type, _DELIMITER, msg_formatted))

        if msg_id is not None:
            topic = topic + '_' + msg_id

        self.log('GetOutput|{0}|{1}>>> {2}'.format(msg_flag, topic, msg))
        self.without_output = 0

    def _test_terminated_unexpected(self):
        self.terminate.set()
        msg = 'Exception>>> Test terminate. No kafka output for last 5 tests.'
        self.log(msg)
        return msg

    def _no_output_error(self):
        msg = 'VerifyResult>>> Failed. No message output for your check type.'
        self.log(msg)
        self.without_output += 1
        if self.without_output >= 5:
            return self._test_terminated_unexpected()
        return msg


def _gen_result(result):
    print_result = list()
    print_result.append('=' * 120)
    title = '%(id)-10s%(status)-10s%(desc)-100s' % {'id': 'ID', 'status': 'STATUS', 'desc': 'DESCRIPTION'}
    print_result.append(title)
    for each in result:
        print_result.append('-' * 120)
        case_result = '%(id)-10s%(status)-10s%(desc)-100s' % {'id': each['name'], 'status': each['status'], 'desc': each['desc']}
        print_result.append(case_result)

    print_result.append('=' * 120)
    return print_result


def proto_test_service(
        version,
        ter_type,
        ter_code,
        vin_code,
        device_id,
        device_no,
        ter_id,
        test_case_list,
        check_kafka=False
):
    """
    :param version:
        String,
        有效值["2.0", "3.0"],
        输入不合法终止测试
    :param ter_type:
        String,
        有效值["xc_2011", "xc_2013", "xny", "tm", "znjw", "jyd", "rsu", "hwsh"],
        输入不合法终止测试
    :param ter_code:
        String,
        808协议("xc_2011", "xc_2013", "xny", "znjw")时使用手机号(sim_no),
        32960("tm")时使用终端Code(ter_code),
        其他类型协议待补充
    :param vin_code:
        String,
        Vin号, 2.0校验Kafka数据时使用
    :param device_id:
        String,
        设备ID, 2.0中是vehicle_id, 3.0中是device_id
        2.0中校验Redis数据时使用
    :param device_no:
        String,
        设备号, 2.0中校验HBASE数据使用
    :param ter_id:
        String,
        终端ID, 3.0中校验Redis数据时使用
    :param test_case_list:
        List, []
        用例列表
    :param check_kafka:
        Bool, 默认False
        是否校验Kafka，当为True时，输出各分层的Kafka数据到日志中，Kafka数据获取不可靠，默认False
    :return:
        List, []
        测试结果
    """

    if not isinstance(test_case_list, list):
        test_case_list = [test_case_list]

    pt = ProtocolTest(
        version,
        ter_type,
        ter_code,
        device_no,
        device_id,
        ter_id,
        test_case_list
    )

    h_t = threading.Thread(target=pt.get_hbase_data)
    p_t = threading.Thread(target=pt.get_phoenix_data)
    r_t = threading.Thread(target=pt.get_redis_data)
    o_t = threading.Thread(target=pt.get_oracle_data)

    if version == '2.0':
        h_t.start()
        o_t.start()
    else:
        p_t.start()

    # if check_kafka:
    #     check_kafka_output(pt, field_info)

    r_t.start()

    v_t = threading.Thread(target=pt.verify)
    v_t.start()

    s_t = threading.Thread(target=pt.run)
    s_t.start()


    pt.terminate.wait()

    if version == '2.0':
        h_t.join()
        o_t.join()
    else:
        p_t.join()

    r_t.join()
    v_t.join()
    s_t.join()

    return pt.result.result


def send_data_service(
        version,
        ter_type,
        ter_code,
        data
):
    sd = SendData(version, ter_type, ter_code)
    return sd.send_data(data)