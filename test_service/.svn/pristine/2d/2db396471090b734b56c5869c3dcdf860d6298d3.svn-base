#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'wxzhangp'

import time
import json
import struct
import codecs
import configparser
from datetime import datetime

_hbase_table_dict = {
    '0F80': 'vehicle_mt_data',
    '0F90': 'vehicle_mt_io',
    '0200': 'vehicle_track_record'
}

def read_config(cfg_dir, env, name):
    fd = open(cfg_dir)
    data = fd.read()
    # remove BOM
    if data[:3] == codecs.BOM_UTF8:
        data = data[3:]
        files = codecs.open(cfg_dir, "w")
        files.write(data)
        files.close()
    fd.close()
    cf = configparser.ConfigParser()
    cf.read(cfg_dir)
    return cf.get(env, name)

def assert_equals(logger, type_, keyword, actual, expected):
    time.sleep(1)
    if str(actual) == str(expected):
        logger.debug('Verify%sData[%s]>>> Passed,  Expected[ %s ], Actual[ %s ].' \
                     % (type_, keyword, expected, actual))
        return 'P'
    else:
        logger.debug('Verify%sData[%s]>>> Failed,  Expected[ %s ], Actual[ %s ].' \
                     % (type_, keyword, expected, actual))
        return 'F'

def assert_exists(logger, type_, keyword, actual):
    time.sleep(1)
    if actual:
        logger.debug('Verify%sData[%s]>>> Passed,  Expectation is the existence of data, Actual[ %s ].' \
                     % (type_, keyword, actual))
        return 'P'
    else:
        logger.debug('Verify%sData[%s]>>> Failed,  Expectation is the existence of data, Actual[ %s ].' \
                     % (type_, keyword,  actual))
        return 'F'

def assert_contains(logger, type_, keyword, expected, actual):
    time.sleep(1)
    if str(expected) in str(actual):
        logger.debug('Verify%sData[%s]>>> Passed,  Expect [ %s ] to be included in [ %s ].' \
                     % (type_, keyword, expected, actual))
        return 'P'
    else:
        logger.debug('Verify%sData[%s]>>> Failed,  Expect [ %s ] to be included in [ %s ].' \
                     % (type_, keyword, expected, actual))
        return 'F'

def check_null_data(logger, data):
    for each in data:
        if not each:
            logger.error('[NullDataException]:  Missing send data or verification data.')
            return False
    return True

def get_oracle_data(db, sql):
    time.sleep(1)
    try:
        cr = db.cursor()
        cr.execute(sql)
        result = cr.fetchone()
        cr.close()
        return result
    except Exception as e:
        print(e)

def get_row_key(cp, vehicle_no, send_data, msg_id):
    hbase_table = _hbase_table_dict[str(msg_id)]
    buf_time = cp.get_buf_time(send_data)
    if buf_time is None:
        return None
    if hbase_table == _hbase_table_dict['0F80']:
        hbase_row_key = vehicle_no + str(99999999999999 - int('20' + buf_time))
        return hbase_table, hbase_row_key
    elif hbase_table  == _hbase_table_dict['0F90']:
        time_offset = cp.get_0f90_time_offset(send_data)
        hbase_row_key = vehicle_no + str(int('20' + buf_time) * 1000 - time_offset)
        return hbase_table, hbase_row_key
    else:
        hbase_row_key = vehicle_no + '20' + buf_time
        return hbase_table, hbase_row_key

def push_msg(topic, msg, msgid=None):
        def _bytes_to_array(bytes):
            r = []
            for each in bytes:
                r.append(int(each))
            return r

        def _bytes_to_value(bytes):
            if len(bytes) == 4:
                return struct.unpack('>L', bytes)[0]
            else:
                return _bytes_to_array(bytes)

        def _hbase_data_transport(hbase_data):
            hbase_data_formatted = dict()
            for each in hbase_data:
                formatted_key = each.decode()
                formatted_key = formatted_key.split(':')[1]
                if topic == _hbase_table_dict['0F90']:
                    hbase_data_formatted[formatted_key] = hbase_data[each].decode(encoding='latin')
                elif topic == _hbase_table_dict['0F80'] or topic == _hbase_table_dict['0200']:
                    hbase_data_formatted[formatted_key] = _bytes_to_array(hbase_data[each])
                else:
                    hbase_data_formatted[formatted_key] = _bytes_to_value(hbase_data[each])
            return json.dumps(hbase_data_formatted)

        return _hbase_data_transport(msg)

def parse_hbase_data(msg_id ,data):
    if msg_id == '0200':
        return bytes(data).decode()
    else:   # msg_id == '0F80'
        dec_data = bytes(data).decode()
        if isinstance(dec_data, int):
            hex_number = hex(int(dec_data))
            integer_bit = int(hex_number[:4], 16)
            float_bit = int(hex_number[4:], 16)
            result = str(integer_bit) + '.' + str(float_bit)
            return result
        else:
            return dec_data

def get_hbase_data(hbase, new_table=None, new_row_key=None):
    table = None
    row_key = None
    hbase_data = None
    hbase_data_formatted = None
    timeout = 0
    while not hbase_data_formatted:
        if table != new_table or row_key != new_row_key:
            hbase_data = None

        if new_table and new_row_key and not hbase_data:
            if new_table == _hbase_table_dict['0200']:
                try:
                    hbase_data = hbase.scan_row_data(new_table, new_row_key)
                except Exception as e:
                    print(e)
                if hbase_data:
                    new_row_key_r, hbase_data = hbase_data
                    new_row_key_r = new_row_key_r.decode()
                    hbase_data_formatted = push_msg(new_table, hbase_data, new_row_key_r)
            else:
                hbase_data = hbase.get_row_data(new_table, new_row_key)
                if hbase_data:
                    hbase_data_formatted = push_msg(new_table, hbase_data, new_row_key)
            table = new_table
            row_key = new_row_key
        time.sleep(1)
        timeout += 1
        if timeout > 60:
            return None
    return hbase_data_formatted

def hbase_expected_data(buf_time, vehicle_id, hbase_datas):
    expected_data_dict = dict()
    expected_data_dict['vehicle_id'] = vehicle_id
    str_time = datetime.strptime(buf_time, '%y%m%d%H%M%S')
    expected_data_dict['timestamp'] = '20' + str_time.strftime('%y-%m-%d %H:%M:%S')
    for each in hbase_datas:
        key = each.split('=')[0]
        value = each.split('=')[1]
        expected_data_dict[key] = value
    return expected_data_dict

def redis_expected_data(buf_time, vehicle_id):
    redis_key_dict = {'IOV.BSP.VEHICLE.RT': {},'IOV.BSP.VEHICLE.RT.STATS': {},'IOV.BSP.VEHICLE.ONOFFLINE': {}}
    str_time = datetime.strptime(buf_time, '%y%m%d%H%M%S')
    redis_key_dict['IOV.BSP.VEHICLE.RT']['timestamp'] = '20' + buf_time
    redis_key_dict['IOV.BSP.VEHICLE.RT']['vehicle_id'] = vehicle_id
    redis_key_dict['IOV.BSP.VEHICLE.RT.STATS']['LastValidGpsTime'] = '20' + buf_time
    redis_key_dict['IOV.BSP.VEHICLE.ONOFFLINE']['TerminalTimeStr'] = '20' + str_time.strftime('%y%m%d %H:%M:%S')
    redis_key_dict['IOV.BSP.VEHICLE.ONOFFLINE']['VehicleId'] = vehicle_id
    return redis_key_dict
