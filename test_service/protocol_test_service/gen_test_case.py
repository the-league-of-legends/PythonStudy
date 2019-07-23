#coding:utf8
#目前有以下几种类型的can不支持：1.故障码 2.多包 3.多包故障码
# import xlrd
# import csv
import struct
import re
import random
from collections import namedtuple, defaultdict
from yt_cp import common

CanField = namedtuple(
    "CanField", [
        "version",
        "description",
        "byte_start",
        "byte_end",
        "bit_start",
        "bit_end",
        "endian",
        "unit",
        "offset",
        "data_type"
    ]
)

TestCase = namedtuple(
    'TestCase', [
        'case_id',
        'ignore',
        'data_type',
        'test_data',
        'verify_type',
        'verify_key',
        'expected'
    ]
)


class CanProto:
    """ Can 协议的加载类 使用excel基础表完成对所有can协议的初始化，然后根据输入的excel 输出can协议报文"""

    def __init__(self, can_definition):
        if not can_definition:
            raise Exception("No CAN proto definition document.")
        self.can_definition = can_definition
        self.can_define = defaultdict(dict)

        self._loader()

    def get_float(self, data):
        if type(data) == str:
            data = "".join(data.split())
            data = data.replace(' ', '')
            data = data.replace('－', '-')
        if not data:
            return 0.0
        if type(data) == float:
            return data
        data = "".join(data.split(" "))
        data = data.replace("-", "-")

        partten = re.compile(r"(-?\d+(?:[\./]\d+)?)")
        value_string, value_float = "", 0.0
        if re.search(partten, data):
            value_string = re.search(partten, data).groups()[0]
        else:
            return 0
        if "/" in value_string:
            try:
                son, mum = value_string.split("/")
                value_float = int(son) / int(mum)
            except:
                print("\t\t\tsplit error :", value_string)
        else:
            value_float = float(value_string)

        return value_float

    def get_version(self, data):
        if isinstance(data, str):
            data = "".join(data.split())
        return data

    def get_can_id(self, data):
        if not data:
            return None
        if isinstance(data, str):
            data = "".join(data.split()).upper()
        return str(data)

    def get_description(self, data):
        if isinstance(data, str):
            data = "".join(data.split())
        return data

    def trans_to_comma(self, my_value, tail):
        byte, bit = 0, tail
        if "." in my_value:
            byte, bit = my_value.split(".")
            try:
                byte = int(byte)
                bit = int(bit)
            except TypeError:
                raise Exception("trans_to_comma error[{}]".format(my_value))
            if bit == 0:
                bit = tail
        else:
            try:
                byte = int(my_value)
            except TypeError:
                raise Exception("trans_to_comma error[{}]".format(my_value))

        return byte, bit

    def get_bytes(self, bytes):
        if not str(bytes).strip():
            return None

        bytes = str(bytes)
        # if isinstance(bytes, float):
        #     bytes = '{0}.1-{0}.8'.format(int(bytes))
        start_byte, end_byte, start_bit, end_bit = None, None, None, None
        dem = "-"
        bytes = bytes.replace('～', dem).replace('－', dem).replace('--', dem).replace('~', dem)

        tmp = bytes.split(dem)
        if len(tmp) == 2:
            a, b = tmp
            a = a.strip()
            b = b.strip()
            if a < b:
                start_byte, start_bit = self.trans_to_comma(a, 1)
                end_byte, end_bit = self.trans_to_comma(b, 8)
            else:
                start_byte, start_bit = self.trans_to_comma(b, 1)
                end_byte, end_bit = self.trans_to_comma(a, 8)

        elif len(tmp) == 1:
            start_byte, start_bit = self.trans_to_comma(tmp[0], 1)
            end_byte, end_bit = self.trans_to_comma(tmp[0], 8)
        # print(start_byte, end_byte, start_bit, end_bit)
        return start_byte, end_byte, start_bit, end_bit

    def get_unit(self, data):
        if isinstance(data, str):
            data = "".join(data.split())
        if not data:
            return 1
        return self.get_float(data)

    def set_endian(self, data):
        if type(data) != str:
            data = str(data)
        data = "".join(data.split())
        if not data:
            return "Intel"
        return data

    def get_offset(self, data):
        if isinstance(data, str):
            data = "".join(data.split())
        if not data:
            return 0

        return self.get_float(data)

    def get_name(self, data):
        if isinstance(data, str):
            data = "".join(data.split())
        return data

    def get_data_type(self, data):
        if 'float' in data or 'double' in data:
            return float
        if 'int' in data:
            return int
        if 'bool' in data:
            return bool
        if 'string' in data:
            return str
        if not data:
            data = float
        return data

    def get_dc_name(self, data):
        if isinstance(data, str):
            data = "".join(data.split())
        return data

    def _parser_row(self, row_data):
        """解析一行can定义文档的数据，返回can id、字段名称、CanField对象"""
        version = self.get_version(row_data[0])
        can_id = self.get_can_id(row_data[1])
        if not can_id:
            raise Exception('can_id cannot be empty >>>{}'.format(row_data))
        description = self.get_description(row_data[2])
        bytes = self.get_bytes(row_data[3])
        if str(bytes).strip() == '':
            raise Exception('bytes cannot be empty >>>{}'.format(row_data))
        byte_start = bytes[0]
        byte_end = bytes[1]
        bit_start = bytes[2]
        bit_end = bytes[3]
        if byte_start == 0 or byte_end == 0:
            raise Exception('bytes is not valid >>>{}'.format(row_data))
        endian = self.set_endian(row_data[4])
        unit = self.get_unit(row_data[5])
        if int(unit) == 0:
            raise Exception('unit should not be 0 >>>{}'.format(row_data))
        offset = self.get_offset(row_data[6])
        try:
            offset = float(offset)
        except:
            raise Exception('offset {} is not valid >>>{}'.format(offset, row_data))
        field_name = self.get_name(row_data[8])
        if str(bytes).strip() == '':
            raise Exception('field_name cannot be empty >>>{}'.format(row_data))
        if len(row_data) == 12:
            data_type = float
            dc_field_name = self.get_dc_name(row_data[9])
        else:
            data_type = self.get_data_type(row_data[9])
            dc_field_name = self.get_dc_name(row_data[10])
        new_field_obj = CanField(version, description, byte_start, byte_end, bit_start, bit_end, endian, unit, offset, data_type)

        return can_id, field_name, new_field_obj, dc_field_name

    def _loader(self):
        """ 读取excel文件，将协议内容保存在baseinfo中"""
        # can_fp = xlrd.open_workbook(self.can_file)
        # table = can_fp.sheets()[0]

        row_number = 0
        for line in self.can_definition:
            parsed_data = self._parser_row(line)
            if not parsed_data:
                continue
            can_id, field_name, can_object, dc_field_name = parsed_data
            row_number += 1
            if field_name not in self.can_define[can_id]:
                self.can_define[can_id][field_name] = dict()
            self.can_define[can_id][field_name]['can_obj'] = can_object
            self.can_define[can_id][field_name]['dc_field_name'] = dc_field_name

        print('Can definition loaded.')

    # def get_packages(self, input_file):
    #     """读取输入的日志文件，每一行返回一个can报文"""
    #     input_xls = xlrd.open_workbook(input_file)
    #     table = input_xls.sheet_by_index(0)
    #     row_number = 0
    #     # print(table.get_rows())
    #     field_values = defaultdict(list)
    #     for line in table.get_rows():
    #         pkg_number, can_id, name, value = line[0:4]
    #         row_number += 1
    #         can_id = can_id.value.strip()
    #         name = name.value.strip()
    #         field_value = self.set_value(can_id, name, value.value)
    #         field_values[(pkg_number.value, can_id)].append(field_value)
    #
    #     return self.packs(field_values)

    def gen_packages(self, can_case_list):
        field_values = defaultdict(list)
        for can_case in can_case_list:
            pkg_number, can_id, field_name, dc_field_name, test_value = can_case
            field_value = self.set_value(can_id, field_name, test_value)
            field_values[(pkg_number, can_id)].append(field_value)

        return self.packs(field_values)

    def gen_expected(self, can_case_list):
        field_values = defaultdict(list)
        for can_case in can_case_list:
            pkg_number, can_id, field_name, dc_field_name, test_value = can_case
            field_values[(pkg_number, can_id)].append(dc_field_name)

        return self.packs(field_values)

    def gen_test_case(self, can_case_list):
        field_values = defaultdict(list)
        expected_values = defaultdict(list)
        for can_case in can_case_list:
            # modified by zhangp [2018-11-26]
            # begin
            pkg_number, can_id, field_name, dc_field_name, test_value, valid_data, data_type = can_case
            # print(can_id, field_name, test_value)
            field_value = self.set_value(can_id, field_name, test_value)
            field_values[(pkg_number, can_id)].append(field_value)
            if valid_data == 3 and data_type != 'enum':
                test_value = 'na'
            # end
            expected_values[(pkg_number, can_id)].append((dc_field_name, test_value))

        test_case_list = []
        for key, value in field_values.items():
            pkg_number, can_id = key
            final_value = 0
            for field_value in value:
                final_value = final_value | field_value

            test_case = defaultdict(dict)
            test_case[pkg_number]['can_data'] = self.pack(can_id, final_value)

            def expected_express(expected_item):
                dc_field_name, test_value = expected_item
                return '{0}={1}'.format(dc_field_name, test_value)
            test_case[pkg_number]['expected'] = ';'.join(expected_express(x) for x in expected_values[(pkg_number, can_id)])
            test_case_list.append(test_case)

        return test_case_list

    def get_endian(self, can_id):
        for key, value in self.can_define[can_id].items():
            # print(self.can_define[can_id][each]['can_obj'])
            value = value['can_obj']
            return value.endian

        return "Intel"

    def packs(self, field_values):
        packages = []
        for key, value in field_values.items():
            pkg_number, can_id = key
            final_value = 0

            for field_value in value:
                final_value = final_value | field_value

            packages.append([pkg_number, self.pack(can_id, final_value)])

        packages.sort(key=lambda x: x[0])

        return [x[1] for x in packages]

    def pack(self, can_id, value):
        
        if self.get_endian(can_id) == "Intel":
            value = struct.pack("Q", value)
        else:
            value = struct.pack(">Q", value)

        can_id = struct.pack(">i", int(can_id, 16))

        return self.str2hex(can_id) + " " + self.str2hex(value)

    def set_value(self, can_id, name, value):
        """
        拼装CAN报文
        :param can_id:
        :param name:
        :param value:
        :return:
        """
        field_obj = self.can_define[can_id][name].get('can_obj', None)
        if not field_obj:
            return 0
        # print(field_obj)
        # print("Target value:{}".format(value))
        #1.计算占多少位
        bits = field_obj.byte_end * 8 + field_obj.bit_end - field_obj.byte_start * 8 - field_obj.bit_start + 1
        # print("占位:{}".format(bits))

        #2.生成掩码
        mask = (1 << (bits)) - 1
        # print("掩码:{:x}".format(mask))

        #3.计算真实的value并转成int类型
        real_value = round((value - field_obj.offset)/field_obj.unit) & mask
        # print("实际填写的值:{}".format(real_value))

        #4.计算向左移动的位数
        # left_shift_bits = field_obj.bit_start + (field_obj.byte_start * 8 - 8) - 1
        if self.get_endian(can_id) == "Intel":
            left_shift_bits = field_obj.bit_start + (field_obj.byte_start * 8 - 8) - 1
        else:
            left_shift_bits = (8 - field_obj.bit_end) + (8 * 8 - field_obj.byte_end * 8)
        # print("向左移动位:{}".format(left_shift_bits))

        real_value = real_value << left_shift_bits
        # print("向左移动后的值:{}".format(real_value))

        total_mask = (1 << 64) - 1
        return real_value & total_mask


    def str2hex(self, value):
        """
        将字符串装成16进制字符串
        :param value: 被转字符串
        :return: 转换后的字符串
        """
        new_value = ["%02x" % i for i in value]

        return " ".join(new_value).upper()

    def echo_key(self):

        for key in self.can_define.keys():
            print("[{}] type = {}".format(key, type(key)))
            for name in self.can_define[key].keys():
                print("\t[{}]".format(name), type(name))

    def add_version(self, vehicle_type, version, buf_hex):
        vehicle_model = '0{} 00 00 25'.format(version)
        can_len = '01 00 0C'
        if vehicle_type == '0':
            can_count = '00 01'
            return common.merge_hex(vehicle_model, can_count, can_len, buf_hex)

        # vehicle_model = '10 00 00 25'
        can_version = '00 00 01 0{}'.format(version)
        can_count = '00 02'

        return common.merge_hex(vehicle_model, can_count, can_version, can_len, buf_hex)


def gen_test_case(can_definition, vehicle_type, can_version):
    print('Generate test case...')

    test = CanProto(can_definition)
    test_case_list = []
    test_case_header = ['id', 'ignore', 'type', 'test data', 'verify module', 'verify key', 'expected']
    test_case_list.append(test_case_header)

    for can_id in test.can_define:
        can_case_list = list()
        field_index = 0
        for field_name in test.can_define[can_id]:
            can_obj = test.can_define[can_id][field_name]['can_obj']
            dc_field_name = test.can_define[can_id][field_name]['dc_field_name']
            case_type = can_obj.data_type

            if not dc_field_name:
                print('dc field name is not supply, test ignored>>>{0}.{1}'.format(can_id, field_name))
                continue

            bits = can_obj.byte_end * 8 + can_obj.bit_end - can_obj.byte_start * 8 - can_obj.bit_start + 1

            if case_type == str:
                continue

            if bits == 1:
                test_data_list = [0, 1]
            elif bits == 2:
                test_data_list = [0, 1, 2, 3]
            else:
                if bits >= 32:
                    max_value = 2105540607
                else:
                    max_value = 2**bits-2
                test_data_list = [
                    0,
                    max_value,
                    random.randint(1, max_value-1),
                    2**bits-1
                ]

            field_offset = can_obj.offset
            field_unit = can_obj.unit
            case_index = 0

            for test_data in test_data_list:
                test_case_id = can_id + '_' + str((case_index + field_index) % 4)
                # modified by zhangp [2018-11-26]
                # begin
                if case_type == 'enum':
                    test_value = int(test_data * field_unit + field_offset)
                else:
                    test_value = case_type(test_data * field_unit + field_offset)
                can_case_list.append((test_case_id, can_id, field_name, dc_field_name, test_value, case_index,  case_type))
                # end
                case_index += 1
            field_index += 1

        for each in test.gen_test_case(can_case_list):
            for x in each:
                test_data = each[x]['can_data']
                test_data = test.add_version(vehicle_type, can_version, test_data)
                test_case = [x, '', '0f80', test_data, 'hbase', 'vehicle_mt_data', each[x]['expected']]
                test_case_list.append(test_case)

    return test_case_list
    # with open('result.csv','w', newline='') as f:
    #     writer = csv.writer(f)
    #     for row in test_case_list:
    #         writer.writerow(row)

