from collections import namedtuple, defaultdict
from yt_cp import common

ProtoField = namedtuple(
    "ProtoField", [
        "source_data_type",
        "unit",
        "offset",
        "data_type",
        "dc_field_name",
        "ff_invalid"
    ]
)

_Hbase_database = {
    '0F80': 'vehicle_mt_data',
    '0F90': 'vehicle_mt_io',
    '0200': 'vehicle_track_record',
    '0F8A': 'road_spectrum_collection'
}

class Proto:
    def __init__(self, proto_definition):
        if not proto_definition:
            raise Exception("No proto definition document.")
        self.proto_definition = proto_definition
        self.proto_define = defaultdict(dict)

        self._loader()

    def _get_msg_id(self, value):
        return value

    def _get_type(self, value):
        value = ''.join(value.split())
        if 'BCD' in value.upper():
            length = int(value[value.find('[')+1: value.find(']')])
            return 'BCD', length
        return value.upper()

    def _get_float(self, data):
        import re
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

    def _get_unit(self, data):
        if isinstance(data, str):
            data = "".join(data.split())
        if not data:
            return 1
        return self._get_float(data)

    def _get_offset(self, data):
        if isinstance(data, str):
            data = "".join(data.split())
        if not data:
            return 0

        return int(self._get_float(data))

    def _get_data_type(self, data):
        if 'float' in data.lower() or 'double' in data.lower():
            return float
        if 'int' in data.lower():
            return int
        return data.lower()

    def _get_ff_invalid(self, data):
        if isinstance(data, float):
            if data == 1.0:
                return True
            else:
                return False

        if isinstance(data, str):
            if data.lower() == 'y' or data.lower() == 'true':
                return True
            else:
                return False

        if isinstance(data, bool):
            return data

        return False

    def _parser_row(self, row_data):
        msg_id = self._get_msg_id(row_data[0])
        source_data_type = self._get_type(row_data[2])
        unit = self._get_unit(row_data[3])
        offset = self._get_offset(row_data[4])
        data_type = self._get_data_type(row_data[5])
        dc_field_name = row_data[6]
        if len(row_data) == 8:
            ff_invalid = self._get_ff_invalid(row_data[7])
        else:
            ff_invalid = False
        proto_obj = ProtoField(source_data_type, unit, offset, data_type, dc_field_name, ff_invalid)
        return msg_id, proto_obj

    def _loader(self):
        print('loading can definition ...')
        row_number = 0
        for line in self.proto_definition:
            msg_id, proto_obj = self._parser_row(line)
            row_number += 1
            self.proto_define[msg_id] = proto_obj

    def str2hex(self, value):
        """
        将字符串装成16进制字符串
        :param value: 被转字符串
        :return: 转换后的字符串
        """
        new_value = ["%02x" % i for i in value]

        return " ".join(new_value).upper()

    def real_value(self, value, msg_id):
        if isinstance(value, str):
            return value
        offset = self.proto_define[msg_id].offset
        unit = self.proto_define[msg_id].unit
        return int(round((value - offset)/unit, 3))

    def package(self, packs):
        pkg = ''
        packs.sort(key=lambda x: x.split(' ')[0])
        for pack in packs:
            pkg += pack + ' '

        return pkg.strip()

    def pack(self, msg_id, value, with_field_len=False):
        import struct

        msg_id_hex = struct.pack('B', int(msg_id, 16))

        if isinstance(value, str):
            field_len = int(len(value)/2)
            if with_field_len:
                return '{0} {1} {2}'.format(self.str2hex(msg_id_hex), self.str2hex(struct.pack('B', field_len)), common.gen_bcd(value, field_len))
            else:
                return '{0} {1}'.format(self.str2hex(msg_id_hex), common.gen_bcd(value, field_len))

        source_data_type = self.proto_define[msg_id].source_data_type

        if source_data_type == 'BYTE':
            fmt = 'B'
            field_len = 1
        elif source_data_type == 'WORD':
            fmt = '>H'
            field_len = 2
        elif source_data_type == 'DWORD':
            fmt = '>L'
            field_len = 4
        else:
            fmt = 'B'
            field_len = 1

        value = struct.pack(fmt, value)

        if with_field_len:
            field_len = struct.pack('B', field_len)
            return '{0} {1} {2}'.format(self.str2hex(msg_id_hex), self.str2hex(field_len), self.str2hex(value))
        return self.str2hex(msg_id_hex) + " " + self.str2hex(value)

    def gen_test_case(self, proto_case_list, proto_type):
        field_values = defaultdict(list)
        expected_values = defaultdict(list)
        if proto_type in ['0200']:
            with_field_len = True
        else:
            with_field_len = False
        for proto_case in proto_case_list:
            test_case_id, msg_id, dc_field_name, expected_value, valid_data, ff_invalid = proto_case
            field_value = self.real_value(expected_value, msg_id)
            field_values[test_case_id].append(self.pack(msg_id, field_value, with_field_len))

            if ff_invalid:
                if expected_value and valid_data == 3 and expected_value >= 2**8-1:
                    expected_value = 'na'
            expected_values[test_case_id].append((dc_field_name, expected_value))

        test_case_list = []
        for test_case_id, package in field_values.items():
            test_case = defaultdict(dict)
            test_case[test_case_id]['package'] = self.package(package)

            def expected_express(expected_item):
                dc_field_name, test_value = expected_item
                return '{0}={1}'.format(dc_field_name, test_value)

            test_case[test_case_id]['expected'] = ';'.join(expected_express(x) for x in expected_values[test_case_id])
            test_case_list.append(test_case)

        return test_case_list


def gen_test_case(can_definition, proto_type):
    import random
    import datetime
    import time
    test = Proto(can_definition)
    test_case_list = []
    test_case_header = ['id', 'ignore', 'type', 'test data', 'verify module', 'verify key', 'expected']
    test_case_list.append(test_case_header)
    msg_index = 0
    proto_case_list = list()
    bcd_msg_id = None
    bcd_value = None
    for msg_id in test.proto_define:
        source_data_type = test.proto_define[msg_id].source_data_type
        ff_invalid = test.proto_define[msg_id].ff_invalid
        if 'BCD' in source_data_type:
            bcd_msg_id = msg_id
            time.sleep(5)
            length = source_data_type[1]
            time_str = datetime.datetime.now().strftime('%y %m %d %H %M %S')
            bcd_value = test.str2hex(time_str.split(' ')[0: length])

        else:
            data_type = test.proto_define[msg_id].data_type

            bits = 8
            if source_data_type == 'WORD':
                bits = 16
            if source_data_type == 'DWORD':
                bits = 32
            if source_data_type.startswith('BIT'):
                tmp = source_data_type.split('_')
                if len(tmp) == 1:
                    bits = 1
                else:
                    bits = int(tmp[1])
            if data_type == 'string':
                test_data_list = [int((bits/8)*2) * '5']
            else:
                if bits == 1:
                    test_data_list = [0, 1]
                elif bits == 2:
                    test_data_list = [0, 1, 2, 3]
                else:
                    if bits >= 32:
                        max_value = 2105540607
                        invalid_value = None
                    else:
                        max_value = 2**bits-2
                        invalid_value = 2**bits-1
                    test_data_list = [
                        0,
                        max_value,
                        random.randint(1, max_value-1),
                        invalid_value
                    ]
            # if proto_type.upper() in _STATE_PROTO:
            #     test_data_list = [0, 1, 2, 3]
            # else:
            #     test_data_list = [0, 2**bits-2, random.randint(1, 2**bits-3), 2**bits-1]

            offset = test.proto_define[msg_id].offset
            unit = test.proto_define[msg_id].unit
            dc_field_name = test.proto_define[msg_id].dc_field_name

            case_index = 0
            for test_data in test_data_list:
                if test_data is None:
                    continue
                test_case_id = str((case_index + msg_index) % 4)
                if data_type == 'string':
                    expected_value = test_data
                else:
                    expected_value = data_type(test_data * unit + offset)
                if data_type == float:
                    expected_value = round(expected_value, 5)
                proto_case_list.append((test_case_id, msg_id, dc_field_name, expected_value, case_index, ff_invalid))
                if bcd_msg_id and bcd_value:
                    proto_case_list.append((test_case_id, bcd_msg_id, bcd_value, '', case_index, False))
                    bcd_msg_id = None
                    bcd_value = None
                case_index += 1



            msg_index += 1

    for each in test.gen_test_case(proto_case_list, proto_type):
        for x in each:
            test_data = each[x]['package']
            # test_data = test.add_version(vehicle_type, can_version, test_data)
            test_case = [x, '', proto_type, test_data, 'hbase', _Hbase_database[proto_type], each[x]['expected']]
            test_case_list.append(test_case)

    return test_case_list

if __name__ == '__main__':
    proto_definition = [
        [
            '0x00',
            '坡度',
            'WORD',
            '1',
            '0',
            'x'
            ],
        [
            '0x01',
            '油门开度',
            'BYTE',
            '0.4',
            '0',
            'y'
            ]
    ]