#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'wxzhangp'

from include import AquilaTest
from yt_cp import Proto808, Proto
import time
import xlrd
import os
import codecs
import configparser

configPath = os.path.join(os.path.split(os.path.realpath(__file__))[0], "config.ini")

def read_config(env, name):
    with open(configPath) as fd:
        data = fd.read()
        if data[:3] == codecs.BOM_UTF8:
            data = data[3:]
            with codecs.open(configPath, 'w') as f:
                f.write(data)
    cf = configparser.ConfigParser()
    cf.read(configPath)
    return cf.get(env, name)

def replace_buffer(cp, buffer, time_pos=None):
    buf_bytes = Proto.hex_to_bytes(buffer)
    buf_update = cp.update_property(buf_bytes)
    buf_rtc = cp.replace_ter_code(buf_update)
    if time_pos:
        buf_rtt = cp.replace_ter_time(buf_rtc, time_pos)
    else:
        buf_rtt = cp.replace_ter_time(buf_rtc)
    send_buf = cp.crc_and_transferred(buf_rtt)
    return send_buf

def send_data(buffer, ter_type):
    print('SendData>>> %s' % Proto.bytes_to_hex(buffer))
    a_s = read_config('server_info', 'aquila_server')
    a_p = read_config('aquila_port', ter_type)
    a_t = AquilaTest(a_s, int(a_p))
    a_t.send_data(buffer)
    time.sleep(5)

def check_null_data(data):
    for each in data:
        if not each:
            print('[NullDataException]: Missing data !!!')
            return False
    return True

def run_test(test_case):
    phone_no = read_config('vehicle_info', 'phone_no')
    cp = Proto808(phone_no)

    print('=' * 200)
    print('Start send...')
    print('Tips: Wait 10 seconds for each data...')
    print('=' * 200)
    for case in test_case:
        case_name = case[0]
        is_ignore = case[1]
        ter_type = case[2]
        buf_type = case[3]
        buffers = case[4]
        time_pos = None
        if not is_ignore and check_null_data([ter_type, buffers]):
            print('CaseName>>> ', case_name)
            if buf_type:
                try:
                    time_pos = read_config('buf_type', buf_type)
                except:
                    print('[NotSupportException]: Unsupported buf_type !!!')
                    continue

            if ',' in buffers:
                for buffer in buffers.split(','):
                    send_buf = replace_buffer(cp, buffer, time_pos)
                    send_data(send_buf, ter_type)
            else:
                send_buf = replace_buffer(cp, buffers, time_pos)
                send_data(send_buf, ter_type)
        print('-' * 200)

    print('End send...')
    print('=' * 200)

if __name__ == '__main__':
    print('Please make sure the data template is ready.')
    time.sleep(3)

    case_file = read_config('file_dir', 'file_path')
    case_list = []
    excel = xlrd.open_workbook(case_file)
    table = excel.sheets()[0]
    for row in range(1, table.nrows):
        case_list.append(table.row_values(row))

    run_test(case_list)


