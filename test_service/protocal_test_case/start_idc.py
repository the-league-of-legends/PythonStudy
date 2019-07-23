#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'wxzhangp'

import os
import time
import xlrd
from protocal_test_case import vehicle_reg_test
from protocal_test_case.include import common

configPath = os.path.join(os.path.split(os.path.realpath(__file__))[0], "config", "config_idc.ini")
case_file = common.read_config(configPath, "file_dir", "file_path")

if __name__ == '__main__':
    print('Vehicle regression test must use test case template, please prepare your doc based template.')
    time.sleep(3)
    case_list = []
    excel = xlrd.open_workbook(case_file)
    table = excel.sheets()[0]
    for row in range(1, table.nrows):
        case_list.append(table.row_values(row))

    vehicle_reg_test.reg_test_run(case_list, configPath)

