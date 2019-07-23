#!/usr/bin/env python
# -*- coding:utf-8 -*-

from include import aquila
from protocal_test_case.include import common, log
from yt_base import YTHbase, YTRedis, YTRabbitMQ, YTES
from yt_cp import Proto808, Proto
from datetime import datetime
import cx_Oracle
import time
import json


_MARK_LINE = 200
_MQ_QUEUE = 'RabbitMQ_AutoTest'

class VehicleRegTest(object):

    def __init__(self, logger, cfg_dir):
        self.logger = logger
        self.cfg_dir = cfg_dir
        self.vin_no = common.read_config(self.cfg_dir, "vehicle_info", "vin_no")
        self.phone_no = common.read_config(self.cfg_dir, "vehicle_info", "phone_no")
        self.vehicle_no = common.read_config(self.cfg_dir, "vehicle_info", "vehicle_no")
        self.vehicle_id = common.read_config(self.cfg_dir, "vehicle_info", "vehicle_id")
        self.verify_state = common.read_config(self.cfg_dir, "state", "verify_state")

        self.cp = Proto808(self.phone_no)
        server = common.read_config(self.cfg_dir, "redis_server", "server")
        self.redis = YTRedis(server)
        server = common.read_config(self.cfg_dir, "hbase_server", "server")
        self.hbase = YTHbase(server)
        server =common.read_config(self.cfg_dir, "oracle_server", "server")
        username = common.read_config(self.cfg_dir, "oracle_server", "username")
        password = common.read_config(self.cfg_dir, "oracle_server", "password")
        self.oracle = cx_Oracle.connect(username, password, server)
        server = common.read_config(self.cfg_dir, "mq_server", "server")
        username = common.read_config(self.cfg_dir, "mq_server", "username")
        password = common.read_config(self.cfg_dir, "mq_server", "password")
        self.mq = YTRabbitMQ(server, username, password)
        server = common.read_config(self.cfg_dir, "es_server", "server")
        self.es = YTES(server)

    def _replace_buffer(self, buffer, time_pos=None):
        msg_id = self.cp.get_msg_id(buffer)
        buf_bytes = Proto.hex_to_bytes(buffer)
        buf_update = self.cp.update_property(buf_bytes)
        buf_rtc = self.cp.replace_ter_code(buf_update)
        if time_pos:
            buf_rtt = self.cp.replace_ter_time(buf_rtc, time_pos)
            buf_time = self.cp.get_buf_time(buf_rtt, time_pos)
        else:
            buf_rtt = self.cp.replace_ter_time(buf_rtc)
            buf_time = self.cp.get_buf_time(buf_rtt)
        send_buf = self.cp.crc_and_transferred(buf_rtt)
        return msg_id, buf_time, send_buf

    def _send_data(self, buffer, ter_type):
        self.logger.debug('WaitTips>>> Waiting for 10 seconds while sending data...')
        self.logger.debug('SendData>>> %s' % Proto.bytes_to_hex(buffer))
        a_s = common.read_config(self.cfg_dir, "aquila_server", "server")
        a_p = common.read_config(self.cfg_dir, "aquila_port", ter_type)
        a_t = aquila.AquilaTest(a_s, int(a_p))
        a_t.send_data(buffer)
        time.sleep(5)
        # a_t.close()
        return a_t

    def _clear_redis(self, keys):
        # self.logger.debug('ClearData>>> Clearing Redis data...')
        for key in keys:
            redis_key = key  + ':VehicleId_' + self.vehicle_id
            self.redis.delete(redis_key)
        time.sleep(1)

    def _verify_redis(self, buf_time, keys):
        result = []
        expected_dict = common.redis_expected_data(buf_time, self.vehicle_id)
        self.logger.debug('WaitTips>>> Waiting for 10 seconds while getting Redis data...')
        for key in keys:
            redis_key = key  + ':VehicleId_' + self.vehicle_id
            timeout = 10
            while timeout:
                redis_data = self.redis.get(redis_key)
                time.sleep(1)
                if redis_data:
                    break
                timeout -= 1
            else:
                self.logger.error('[NullDataException]: No data obtained, Key[ %s ]...' % redis_key)
                result.append('F')
                continue
            self.logger.debug('GetRedisMsg[%s]>>> %s' % (redis_key, redis_data))
            redis_actual = json.loads(redis_data)

            for each in expected_dict[key]:
                r = common.assert_equals(self.logger, 'Redis', each, redis_actual[each], expected_dict[key][each])
                result.append(r)
        return result

    def _verify_redis_fault(self, redis_key, hash_field):
        self.logger.debug('WaitTips>>> Waiting for 10 seconds while getting Redis data...')
        redis_data = self.redis.hget(redis_key, hash_field)
        # redis_data = self.redis.hgetall(redis_key)
        # self.logger.debug('GetRedisMsg[%s]>>> %s' % (redis_key, redis_data))
        r = common.assert_exists(self.logger, 'Redis', redis_key, redis_data)
        return [r]

    def _verify_oracle(self, tables, buf_time):
        result = []
        str_time = datetime.strptime(buf_time, '%y%m%d%H%M%S')
        ter_time = '20' + str_time.strftime('%y/%m/%d %H:%M:%S')
        for table in tables:
            sql = "select * from %s where vehicle_id='%s' and terminal_time=to_date('%s', 'yyyy/mm/dd hh24:mi:ss') " \
                  "and row_create_time > sysdate-1" % (table, self.vehicle_id, ter_time)
            # print(sql)
            query_result = common.get_oracle_data(self.oracle, sql)
            r = common.assert_exists(self.logger, 'Oracle', table, query_result)
            result.append(r)
        return result

    def _verify_oracle_fault(self, table, ter_col, buf_time):
        str_time = datetime.strptime(buf_time, '%y%m%d%H%M%S')
        ter_time = '20' + str_time.strftime('%y/%m/%d %H:%M:%S')
        sql = "select * from %s where vehicle_id='%s' and %s=to_date('%s', 'yyyy/mm/dd hh24:mi:ss') " \
              "and row_create_time > sysdate-1" % (table, self.vehicle_id, ter_col, ter_time)
        query_result = common.get_oracle_data(self.oracle, sql)
        r = common.assert_exists(self.logger, 'Oracle', table, query_result)
        return [r]

    def _verify_hbase(self, msg_id, buf_time, send_buf, hbase_datas):
        result = []
        hbase_table, hbase_row_key = common.get_row_key(self.cp, self.vehicle_no, send_buf, msg_id)
        # print('GetHBaseKey[%s]>>> %s' % (hbase_table, hbase_row_key))
        self.logger.debug('WaitTips>>> May need to wait for 30 seconds while getting HBase data...')
        hbase_data = common.get_hbase_data(self.hbase, hbase_table, hbase_row_key)
        if hbase_data:
            self.logger.debug('GetHBaseMsg[%s]>>> %s' % (hbase_table, hbase_data))
            hbase_actual = json.loads(hbase_data)
            hbase_expected = common.hbase_expected_data(buf_time, self.vehicle_id, hbase_datas)

            for key in hbase_expected:
                actual =  common.parse_hbase_data(msg_id, hbase_actual[key])
                r = common.assert_equals(self.logger, 'HBase', key, actual, hbase_expected[key])
                result.append(r)
        else:
            self.logger.error('[NullDataException]: No data obtained, Key[ %s ]...' % hbase_row_key)
            result.append('F')
        return result

    def _verify_es_event(self, buf_time, event_code):
        index = 'vehicle_event_20' + datetime.now().strftime('%y%m%d')
        _id = '_'.join((self.vehicle_id, buf_time, event_code))
        query = {'query': {'match': {}}}
        query['query']['match']['_id'] = _id
        data = self.es.search(index, query)
        # print(data)
        r = common.assert_exists(self.logger, 'ElasticSearch', index, data)
        return [r]

    def _verify_es_fault(self, buf_time, fault_code):
        index = 'vehicle_fault_detail_20' + datetime.now().strftime('%y%m')
        query = '{"query":{"bool":{"must":[{"term":{"vehicle_id":"%s"}},{"term":{"fault_code_id":"%s"}},' \
                '{"term":{"report_time":"%s"}}]}}}' % (self.vehicle_id, fault_code, '20'+buf_time)
        data = self.es.search(index, json.loads(query))
        # print(data)
        r = common.assert_exists(self.logger, 'ElasticSearch', index, data)
        return [r]

    def _verify_rabbitmq(self, exchange, msg_item_code):
        result = []
        datas = self.mq.get_all_message(_MQ_QUEUE)
        for data in datas:
            # print(data)
            if self.vehicle_id in data:
                actual_vid = '"vehicle_id":"%s"' % self.vehicle_id
                actual_mic = '"msg_item_code":"%s"' % msg_item_code
                r1 = common.assert_contains(self.logger, 'RabbitMQ', exchange, actual_vid, data)
                r2 = common.assert_contains(self.logger, 'RabbitMQ', exchange, actual_mic, data)
                break
        else:
            r1 = common.assert_equals(self.logger, 'RabbitMQ', exchange, None, self.vehicle_id)
            r2 = common.assert_equals(self.logger, 'RabbitMQ', exchange, None, msg_item_code)
        self.mq.delete_a_queue(_MQ_QUEUE)
        result.append(r1)
        result.append(r2)
        return result

############################################ Test type ###################################################

    def no_route_swipe_card(self, test_case):
        buffer = test_case[3].strip().replace('\n','')
        mq_data = test_case[7].strip().replace('\n','').split('=')
        datas = [buffer, mq_data]
        if not common.check_null_data(self.logger, datas):
            return

        exchange = 'BusiPushQueue.Event'
        if self.verify_state:
            self.mq.create_a_queue(_MQ_QUEUE, exchange)
        msg_id, buf_time, send_buf = self._replace_buffer(buffer, time_pos=20)
        self._send_data(send_buf, ter_type='xc')

        oracle_tables = ['tb_check_card_record', 'tb_passenger_station_actual']
        result_1 = self._verify_oracle(oracle_tables, buf_time)
        result_2 = []
        if self.verify_state:
            result_2 = self._verify_rabbitmq(exchange, mq_data[1])
        return result_1 + result_2

    def passenger_swipe_card(self, test_case):
        buffer = test_case[3].strip().replace('\n','')
        mq_data = test_case[7].strip().replace('\n','').split('=')
        datas = [buffer, mq_data]
        if not common.check_null_data(self.logger, datas):
            return

        exchange = 'BusiPushQueue.Event'
        if self.verify_state:
            self.mq.create_a_queue(_MQ_QUEUE, exchange)
        msg_id, buf_time, send_buf = self._replace_buffer(buffer, time_pos=38)
        self._send_data(send_buf, ter_type='xc')

        oracle_tables = ['tb_check_card_record', 'tb_passenger_station_actual']
        result_1 = self._verify_oracle(oracle_tables, buf_time)
        result_2 = []
        if self.verify_state:
            result_2 = self._verify_rabbitmq(exchange, mq_data[1])
        return result_1 + result_2

    def secretary_swipe_card(self, test_case):
        buffer = test_case[3].strip().replace('\n','')
        if not common.check_null_data(self.logger, buffer):
            return

        msg_id, buf_time, send_buf = self._replace_buffer(buffer, time_pos=16)
        self._send_data(send_buf, ter_type='xc')

        oracle_tables = ['tb_check_card_record', 'tb_driver_station_actual']
        result_1 = self._verify_oracle(oracle_tables, buf_time)
        return result_1

    def driver_swipe_card(self, test_case):
        buffer = test_case[3].strip().replace('\n','')
        if not common.check_null_data(self.logger, buffer):
            return

        msg_id, buf_time, send_buf = self._replace_buffer(buffer, time_pos=16)
        self._send_data(send_buf, ter_type='xc')

        oracle_tables = ['tb_check_card_record', 'tb_driver_station_actual']
        result_1 = self._verify_oracle(oracle_tables, buf_time)
        return result_1

    def insert_pull_card(self, test_case):
        buffer = test_case[3].strip().replace('\n','')
        if not common.check_null_data(self.logger, buffer):
            return

        msg_id, buf_time, send_buf = self._replace_buffer(buffer)
        self._send_data(send_buf, ter_type='xc')

        result_1 = self._verify_oracle_fault('tb_check_card_record', 'terminal_time', buf_time)
        result_2 = self._verify_oracle_fault('tb_driver_station_actual', 'up_time', buf_time)
        return result_1 + result_2

    def vehicle_on_line(self, test_case):
        buffer = test_case[3].strip().replace('\n','')
        hbase_datas = test_case[4].replace(' ','').replace('\n','').split(',')
        datas = [buffer, hbase_datas]
        if not common.check_null_data(self.logger, datas):
            return

        redis_keys = ['IOV.BSP.VEHICLE.RT', 'IOV.BSP.VEHICLE.RT.STATS', 'IOV.BSP.VEHICLE.ONOFFLINE']
        self._clear_redis(redis_keys)

        msg_id, buf_time, send_buf = self._replace_buffer(buffer)
        self._send_data(send_buf, ter_type='xc')

        result_1 = self._verify_hbase(msg_id, buf_time, send_buf, hbase_datas)
        result_2 = self._verify_redis(buf_time, redis_keys)
        return result_1 + result_2

    def vehicle_event(self, test_case):
        buffers = test_case[3].strip().replace('\n','').split(',')
        hbase_datas = test_case[4].replace(' ','').replace('\n','').split(',')
        es_data = test_case[8].replace(' ','').replace('\n','').split('=')
        mq_data = test_case[7].strip().replace('\n','').split('=')
        datas = [buffers, hbase_datas, es_data, mq_data]
        if not common.check_null_data(self.logger, datas):
            return

        exchange = 'BusiPushQueue.Event'
        if self.verify_state:
            self.mq.create_a_queue(_MQ_QUEUE, exchange)
        msg_id = buf_time = send_buf = None
        for buffer in buffers:
            msg_id, buf_time, send_buf = self._replace_buffer(buffer)
            self._send_data(send_buf, ter_type='xc')

        result_1 = self._verify_hbase(msg_id, buf_time, send_buf, hbase_datas)
        result_2 = self._verify_es_event(buf_time, es_data[1])
        result_3 = []
        if self.verify_state:
            result_3 = self._verify_rabbitmq(exchange, mq_data[1])
        return result_1 + result_2 + result_3

    def vehicle_fault(self, test_case):
        buffers = test_case[3].strip().replace('\n','').split(',')
        hbase_datas = test_case[4].replace(' ','').replace('\n','').split(',')
        redis_data = test_case[6].replace(' ','').replace('\n','').split('=')
        es_data = test_case[8].replace(' ','').replace('\n','').split('=')
        datas = [buffers, hbase_datas, redis_data, es_data]
        if not common.check_null_data(self.logger, datas):
            return

        redis_key = 'IOV.BSP.VEHICLE.FAULT.PUSH:VehicleId_' + self.vehicle_id
        self.redis.hdel(redis_key, redis_data[1])

        begin_msg_id, begin_buf_time, begin_send_buf = self._replace_buffer(buffers[0])
        self._send_data(begin_send_buf, ter_type='xny')
        end_msg_id, end_buf_time, end_send_buf = self._replace_buffer(buffers[0])
        self._send_data(end_send_buf, ter_type='xny')

        result_1 = self._verify_hbase(end_msg_id, end_buf_time, end_send_buf, hbase_datas)
        result_2 = self._verify_oracle_fault('tb_vehicle_fault', 'last_report_time', end_buf_time)
        result_3 = self._verify_redis_fault(redis_key, redis_data[1])
        result_4 = self._verify_es_fault(end_buf_time, es_data[1])
        return result_1 + result_2 + result_3 + result_4

    def command_send(self, test_case):
        buffer = test_case[3].strip().replace('\n','')
        msg_str = test_case[7].strip()
        datas = [buffer, msg_str]
        if not common.check_null_data(self.logger, datas):
            return

        msg_dict = json.loads(msg_str)
        msg_dict['SendTime'] = '20' + datetime.now().strftime('%y%m%d%H%M%S')
        msg_dict['VehicleVin'] = self.vin_no
        msg_dict['VehicleId'] = self.vehicle_id
        msg_str = json.dumps(msg_dict)

        msg_id, buf_time, send_buf = self._replace_buffer(buffer)
        a_t = self._send_data(send_buf, ter_type='xc')
        mq_exchange = 'TerminalCommand.Send'
        self.logger.debug('PublishMsg>>> %s' % msg_str)
        self.mq.publish_a_message(mq_exchange, msg_str)
        buf_bytes = a_t.recv_data(1024)
        buf_hex = Proto.bytes_to_hex(buf_bytes)
        self.logger.debug('GetBuffer>>> %s' % buf_hex)
        buf_hex = buf_hex.replace(' ','')

        result = list()
        if ('8F94' in buf_hex) and (self.phone_no in buf_hex):
            r1 = common.assert_contains(self.logger, 'RabbitMQ', mq_exchange, '8F94', buf_hex)
            r2 = common.assert_contains(self.logger, 'RabbitMQ', mq_exchange, self.phone_no, buf_hex)
            result.append(r1)
            result.append(r2)
        else:
            expceted = '7E8F94.*0' + self.phone_no
            r = common.assert_contains(self.logger, 'RabbitMQ', mq_exchange, expceted, buf_hex)
            result.append(r)
        return result

############################################ Run Test ###################################################
def reg_test_run(case_list, cfg_dir):

    print_result = list()
    log_dir = common.read_config(cfg_dir, "log_dir", "path")
    log_level = common.read_config(cfg_dir, "log_dir", "level")
    logger = log.Log(log_dir, log_level)
    logger.info('=' * _MARK_LINE)
    logger.info('Start vehicle regression test...')
    logger.info('Tips: Wait 60 seconds for each test if no message receive.')
    logger.info('-' * _MARK_LINE)

    vrt = VehicleRegTest(logger, cfg_dir)

    for each_line in case_list:
        if not each_line[1].replace(' ',''):
            logger.debug('StartTest>>> %s' % each_line[0])
            if 'no_route_swipe_card' in each_line[2]:
                result = vrt.no_route_swipe_card(each_line)
            elif 'passenger_swipe_card' in each_line[2]:
                result = vrt.passenger_swipe_card(each_line)
            elif 'secretary_swipe_card' in each_line[2]:
                result = vrt.secretary_swipe_card(each_line)
            elif 'driver_swipe_card' in each_line[2]:
                result = vrt.driver_swipe_card(each_line)
            elif 'insert_pull_card' in each_line[2]:
                result = vrt.insert_pull_card(each_line)
            elif 'vehicle_on_line' in each_line[2]:
                result = vrt.vehicle_on_line(each_line)
            elif 'vehicle_event' in each_line[2]:
                result = vrt.vehicle_event(each_line)
            elif 'vehicle_fault' in each_line[2]:
                result = vrt.vehicle_fault(each_line)
            elif 'command_send' in each_line[2]:
                try:
                    result = vrt.command_send(each_line)
                except Exception as e:
                    print(e)
                    result = []
            else:
                result = []
                logger.error('Verification>>> Not support case type')

            if ('F' in result) or (not result):
                logger.debug('Verification>>> Failed.')
                case_result = ','.join([each_line[0], 'Failed', ''])
            else:
                logger.debug('Verification>>> Passed.')
                case_result = ','.join([each_line[0], 'Passed', ''])
            logger.debug('=' * _MARK_LINE)
            print_result.append(case_result)

    time.sleep(1)
    title = '%(id)-30s%(status)-20s%(desc)-100s' % {'id': 'ID', 'status': 'STATUS', 'desc': 'DESCRIPTION'}
    logger.info(title)

    for each in print_result:
        logger.info('-' * _MARK_LINE)
        rr = each.split(',')
        content = '%(id)-30s%(status)-20s%(desc)-100s' % {'id': rr[0], 'status': rr[1], 'desc': rr[2]}
        logger.info(content)

    logger.info('-' * _MARK_LINE)
    logger.info('End vehicle regression test...')
    logger.info('=' * _MARK_LINE)