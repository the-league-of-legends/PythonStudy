
__author__ = 'zouhl'
from yt_cp import common
from collections import OrderedDict
import datetime
import time
import struct

PROTO_TYPE = {
    'xc': '808',
    'xny': '808',
    'tm': '32960',
    'jw': '808B'
}

HEX0200 = '00 00 00 00 00 04 00 03 02 4A DB 16 06 9F CF E5 03 DC 00 00 00 00 18 04 20 00 00 20'


class Proto(object):
    @staticmethod
    def hex_to_bytes(cp_hex):
        return common.hex_to_bytes(cp_hex)

    @staticmethod
    def bytes_to_hex(source):
        return common.bytes_to_hex(source)


class Proto808(Proto):
    def __init__(self, ter, replace_time=True, strftime=None):
        proto_def_header = OrderedDict()
        proto_def_header['flag'] = '7E'
        proto_def_header['msg_id'] = '00 00'
        proto_def_header['property'] = '00 00'
        proto_def_header['ter_code'] = '00 00 00 00 00 00'
        proto_def_header['msg_no'] = '00 00'
        proto_def_tail = OrderedDict()
        proto_def_tail['crc'] = '00'
        proto_def_tail['flag'] = '7E'

        self.ter = ter
        self.replace_time = replace_time
        self.strftime = strftime
        self.proto_def_header = proto_def_header
        self.proto_def_tail = proto_def_tail
        self.header_len = common.get_hex_len(proto_def_header)
        self.tail_len = common.get_hex_len(proto_def_tail)

    def get_msg_id(self, buf):
        if isinstance(buf, str):
            buf_bytes = common.hex_to_bytes(buf)
        else:
            buf_bytes = buf
        msg_id_begin_pos = common.get_hex_len(self.proto_def_header['flag'])
        msg_id_end_pos = common.get_hex_len(
            self.proto_def_header['flag'],
            self.proto_def_header['msg_id']
        )
        return common.bytes_to_hex(buf_bytes[msg_id_begin_pos: msg_id_end_pos], delimit='')

    def replace_ter_time(self, buf, time_pos=None, strftime=None):
        time_pos_dict = {
            '0200': [22],
            '0702': [1],
            '0F14': [23, 51],
            '0F17': [23],
            '0F80': [22],
            '0F89': [28, 34],
            '0F8A': [23],
            '0F90': [0],
            '0F99': [0],
            '0F9D': [0],
            '0FA1': [0],
            '0FA2': [0],
            '0FA3': [0],
            '0FA4': [0],
            '0FF0': [0, 6],
            '0FF1': [0, 6]
        }

        if isinstance(buf, str):
            buf_bytes = common.hex_to_bytes(buf)
        else:
            buf_bytes = buf
        if strftime is None:
            strftime = '%y %m %d %H %M %S'

        cp_id = self.get_msg_id(buf_bytes)

        if time_pos is None:
            if cp_id.upper() not in time_pos_dict:
                return buf_bytes
            else:
                time_pos = time_pos_dict[cp_id.upper()]
        else:
            if isinstance(time_pos, int):
                time_pos = [time_pos]
            elif not isinstance(time_pos, list):
                return buf_bytes

        for time_begin_pos in time_pos:
            time_str = datetime.datetime.now().strftime(strftime)
            time_bytes = common.hex_to_bytes(time_str)
            time_end_pos = time_begin_pos + len(time_bytes)
            buf_bytes = buf_bytes[0: self.header_len+time_begin_pos] + time_bytes + buf_bytes[self.header_len+time_end_pos:]

            if time_begin_pos != time_pos[-1]:
                time.sleep(10)

        return buf_bytes

    def read_data_from_template(self, cp_template_file, remove_msg_type=None):
        cp_bytes_list = []

        if remove_msg_type:
            with open(cp_template_file, mode="r", encoding="utf-8") as f_cap:
                for line in f_cap:
                    line = line.strip()
                    if line.startswith(common.merge_hex(self.proto_def_header['flag'], remove_msg_type)):
                        continue

                    cp_bytes = common.hex_to_bytes(line)
                    if not cp_bytes:
                        continue

                    cp_bytes_list.append(cp_bytes)
        else:
            with open(cp_template_file, mode="r", encoding="utf-8") as f_cap:

                for line in f_cap:
                    line = line.strip()
                    cp_bytes = common.hex_to_bytes(line)
                    if not cp_bytes:
                        continue

                    cp_bytes_list.append(cp_bytes)

        return cp_bytes_list

    def update_property(self, buf):
        if isinstance(buf, str):
            buf_bytes = common.hex_to_bytes(buf)
        else:
            buf_bytes = buf

        buf_length = len(buf_bytes) - (self.header_len + self.tail_len)
        fields = []
        for each in self.proto_def_header:
            if each == 'property':
                break
            fields.append(self.proto_def_header[each])

        property_begin_pos = common.get_hex_len(
            tuple(fields)
        )
        property_len = common.get_hex_len(self.proto_def_header['msg_id'])

        property_buf = struct.pack('>H', struct.unpack('>H', buf_bytes[property_begin_pos: property_begin_pos+property_len])[0] & int('FC00', 16) | buf_length)

        return buf_bytes[0: property_begin_pos] + property_buf + buf_bytes[property_begin_pos+property_len:]

    def replace_ter_code(self, buf, ter_code=None):
        """
        :return:

        ter_code: 18662227352
        ter_code_hex: 01 86 62 22 73 52
        """
        if isinstance(buf, str):
            buf_bytes = common.hex_to_bytes(buf)
        else:
            buf_bytes = buf
        fields = []
        for each in self.proto_def_header:
            fields.append(self.proto_def_header[each])
            if each == 'property':
                break

        ter_code_begin_pos = common.get_hex_len(
            tuple(fields)
        )

        # modified by zhangp [2018-12-3]
        # begin
        if not ter_code:
            ter_code = self.ter
        # end

        ter_code_len = common.get_hex_len(self.proto_def_header['ter_code'])
        ter_bytes = common.hex_to_bytes(common.gen_bcd(ter_code, ter_code_len))

        if len(buf_bytes) <= ter_code_begin_pos + ter_code_len:
            return []

        buf_bytes = buf_bytes[0: ter_code_begin_pos] + ter_bytes + buf_bytes[ter_code_begin_pos + ter_code_len:]

        return buf_bytes

    def crc_and_transferred(self, buf):
        if isinstance(buf, str):
            buf_bytes = common.hex_to_bytes(buf)
        else:
            buf_bytes = buf
        crc = 0
        data_begin_pos = common.get_hex_len(self.proto_def_header['flag'])
        data_end_pos = len(buf_bytes) - common.get_hex_len(self.proto_def_tail)
        data_bytes = buf_bytes[data_begin_pos: data_end_pos]

        for each in data_bytes:
            crc ^= int(each)

        def _transferred(cp_buf):
            cp_hex = ' '.join(["%02X" % x for x in cp_buf]).replace('7D', '7D 01').replace('7E', '7D 02')
            return Proto.hex_to_bytes(cp_hex)

        data_bytes_with_crc = _transferred(data_bytes + bytes([crc]))

        buf_bytes = bytes(buf_bytes[0: data_begin_pos]) + data_bytes_with_crc + bytes(buf_bytes[data_end_pos+1:])

        return buf_bytes

    def gen_buffer(self, buf, ter_code=None, replace_time=None, strftime=None):
        if isinstance(buf, str):
            buf_bytes = common.hex_to_bytes(buf)
        else:
            buf_bytes = buf

        buf_bytes = self.update_property(buf_bytes)

        if ter_code is None:
            ter_code = self.ter

        if isinstance(ter_code, list):
            ter_code = ter_code[0]

        buf_bytes = self.replace_ter_code(buf_bytes, ter_code)

        if replace_time is None:
            replace_time = self.replace_time

        if replace_time:
            buf_bytes = self.replace_ter_time(buf_bytes, strftime)

        buf_bytes = self.crc_and_transferred(buf_bytes)

        return common.bytes_to_hex(buf_bytes)

    def gen_round_buffer(self, source_buf, round_=0):
        buf_list = []
        if isinstance(self.ter, str):
            ter_count = 1
            self.ter = [self.ter]
        else:
            ter_count = len(self.ter)

        if isinstance(source_buf, list):
            s_buf_list = source_buf
            buf_len = len(s_buf_list)
        else:
            s_buf_list = [source_buf]
            buf_len = 1

        for i in range(ter_count):
            buf_hex = s_buf_list[(round_+i) % buf_len]
            ter_code = self.ter[i]
            buf_bytes = self.hex_to_bytes(self.gen_buffer(buf_hex, ter_code, self.replace_time, self.strftime))
            buf_list.append(buf_bytes)

        return buf_list

    def get_buf_time(self, buf, time_pos_=None):
        time_pos = {
            '0200': 22,
            '0702': 1,
            '0F14': 23,
            '0F17': 23,
            '0F80': 22,
            '0F89': 28,
            '0F8A': 23,
            '0F90': 0,
            '0F99': 0,
            '0F9D': 0,
            '0FA1': 0,
            '0FA2': 0,
            '0FA3': 0,
            '0FA4': 0,
            '0FF0': 0,
            '0FF1': 0
        }

        if isinstance(buf, str):
            buf_bytes = common.hex_to_bytes(buf)
        else:
            buf_bytes = buf

        buf_time_len = 6
        msg_id = self.get_msg_id(buf_bytes)

        # modified by zhangp [2018-12-3]
        # begin
        if msg_id not in time_pos:
            if time_pos:
                time_begin_pos = time_pos_
            else:
                return None
        else:
            time_begin_pos = time_pos[msg_id]
        time_end_pos = time_begin_pos + buf_time_len
        # end

        time_buf_hex = common.bytes_to_hex(buf_bytes[self.header_len+time_begin_pos: self.header_len+time_end_pos], delimit='')
        return time_buf_hex

    def get_0f90_time_offset(self, buf):
        if isinstance(buf, str):
            buf_bytes = common.hex_to_bytes(buf)
        else:
            buf_bytes = buf
        header_len = common.get_hex_len(self.proto_def_header)
        time_sec_offset_pos = 6
        time_milsec_offset_pos = 7
        sec = int(buf_bytes[header_len+time_sec_offset_pos])
        milsec = struct.unpack('>H', buf_bytes[header_len+time_milsec_offset_pos: header_len+time_milsec_offset_pos + 2])[0]
        return sec * 1000 + milsec

    def assemble_buf(self, msg_id, data):
        buf_time_len = 6
        self.proto_def_header['msg_id'] = msg_id
        cp_header_hex = common.merge_hex(self.proto_def_header)

        hex_0200 = HEX0200
        cp_tail = common.merge_hex(self.proto_def_tail)
        if msg_id.upper() == '0F 14':
            buf_bytes_hex = common.merge_hex(cp_header_hex, data, hex_0200, hex_0200, cp_tail)
        elif msg_id.upper() in ('0F 90', '0F 99'):
            ter_time = common.gen_bcd(0, buf_time_len)
            time_offset = '00 00 00'
            data_items_count = common.bytes_to_hex([len(data.split(' '))/2])
            buf_bytes_hex = common.merge_hex(cp_header_hex, ter_time, time_offset, data_items_count, data, cp_tail)
        elif msg_id.upper() == '0F 8A':
            data_items_count = '01'
            data_len = Proto.bytes_to_hex(struct.pack('>H', len(data.split(' '))))
            buf_bytes_hex = common.merge_hex(cp_header_hex, data_items_count, hex_0200, data_len, data, cp_tail)
        else:
            buf_bytes_hex = common.merge_hex(cp_header_hex, hex_0200, data, cp_tail)
        return buf_bytes_hex


class Proto808B(Proto808):
    def __init__(self, ter, replace_time=True, strftime=None):

        proto_def_header = OrderedDict()
        proto_def_header['flag'] = '7E'
        proto_def_header['msg_id'] = '00 00'
        proto_def_header['ver_id'] = '00 01'
        proto_def_header['property'] = '00 00'
        proto_def_header['ter_code'] = '00 00 00 00 00 00'
        proto_def_header['msg_no'] = '00 00'
        proto_def_tail = OrderedDict()
        proto_def_tail['crc'] = '00'
        proto_def_tail['flag'] = '7E'
        super().__init__(ter, replace_time, strftime)
        self.proto_def_header = proto_def_header
        self.proto_def_tail = proto_def_tail
        self.header_len = common.get_hex_len(proto_def_header)
        self.tail_len = common.get_hex_len(proto_def_tail)


class Proto32960(Proto):
    def __init__(self, replace_time=True, strftime=None):
        proto_def_header = OrderedDict()
        proto_def_header['flag'] = '23 23'
        proto_def_tail = OrderedDict()
        proto_def_tail['crc'] = '00'
        self.proto_def_header = proto_def_header
        self.proto_def_tail = proto_def_tail
        self.replace_time = replace_time
        self.strftime = strftime

    @staticmethod
    def replace_ter_code(buf_bytes, ter_code):
        """
        :return:
        """
        ter_code_begin_pos = 4
        ter_code_len = 17
        ter_bytes = bytes(ter_code.ljust(ter_code_len).encode())

        if len(buf_bytes) <= ter_code_begin_pos + ter_code_len:
            return []

        buf_bytes = buf_bytes[0: ter_code_begin_pos] + ter_bytes + buf_bytes[ter_code_begin_pos + ter_code_len:]

        return buf_bytes

    def crc_and_transferred(self, buf_bytes):
        crc = 0
        data_begin_pos = common.get_hex_len(self.proto_def_header)
        data_end_pos = len(buf_bytes) - common.get_hex_len(self.proto_def_tail)
        data_bytes = buf_bytes[data_begin_pos: data_end_pos]

        for each in data_bytes:
            crc ^= int(each)

        data_bytes_with_crc = data_bytes + bytes([crc])

        buf_bytes = bytes(buf_bytes[0: data_begin_pos]) + data_bytes_with_crc + bytes(buf_bytes[data_end_pos+1:])

        return buf_bytes

    def gen_buffer(self, buf_hex: str, ter_code=None):
        buf_bytes = common.hex_to_bytes(buf_hex)

        if ter_code is None:
            ter_code = self.ter

        if isinstance(ter_code, list):
            ter_code = ter_code[0]

        buf_bytes = self.replace_ter_code(buf_bytes, ter_code)

        buf_bytes = self.crc_and_transferred(buf_bytes)

        return common.bytes_to_hex(buf_bytes)

    def gen_round_buffer(self, source_buf, round_=0):
        buf_list = []
        if isinstance(self.ter, str):
            ter_count = 1
            self.ter = [self.ter]
        else:
            ter_count = len(self.ter)

        if isinstance(source_buf, list):
            s_buf_list = source_buf
            buf_len = len(s_buf_list)
        else:
            s_buf_list = [source_buf]
            buf_len = 1

        for i in range(ter_count):
            buf_hex = s_buf_list[(round_+i) % buf_len]
            ter_code = self.ter[i]
            buf_bytes = self.hex_to_bytes(self.gen_buffer(buf_hex, ter_code))
            buf_list.append(buf_bytes)

        return buf_list


class ProtoObuRsu(Proto):
    def __init__(self, ter, replace_time=False):
        proto_def_header = OrderedDict()
        proto_def_header['flag'] = 'FA FB'
        proto_def_header['version'] = '01'
        proto_def_header['frame_type'] = '00'
        proto_def_header['frame_len'] = '00 00'
        proto_def_tail = OrderedDict()
        proto_def_tail['crc'] = '00'
        self.ter = ter
        self.replace_time = replace_time
        self.proto_def_header = proto_def_header
        self.proto_def_tail = proto_def_tail

    def replace_ter_time(self, cp_bytes):
        time_pos = {
            '42': [46]
        }

        cp_id = self.get_msg_id(cp_bytes)

        if cp_id.upper() not in time_pos:
            return cp_bytes

        header_len = common.get_hex_len(self.proto_def_header)

        for time_begin_pos in time_pos[cp_id]:
            timestamp_sec = int(time.time() * 1000)
            time_bytes = struct.pack('>Q', timestamp_sec)
            time_end_pos = time_begin_pos + len(time_bytes)
            cp_bytes = cp_bytes[0: header_len+time_begin_pos] + time_bytes + cp_bytes[header_len+time_end_pos:]

            time.sleep(10)

        return cp_bytes

    def update_length(self, buf_bytes):
        header_len = common.get_hex_len(self.proto_def_header)
        tail_len = common.get_hex_len(self.proto_def_tail)
        buf_length = len(buf_bytes) - (header_len + tail_len)
        buf_tytes = buf_bytes[0: header_len] + struct.pack(">H", buf_length) + buf_bytes[header_len + 2:]
        return buf_tytes

    def gen_buffer(self, buf_hex: str, ter_code=None, replace_time=False):
        buf_bytes = common.hex_to_bytes(buf_hex)

        # buf_bytes = self.update_length(buf_bytes)

        if ter_code is None:
            ter_code = self.ter

        if isinstance(ter_code, list):
            ter_code = ter_code[0]

        # buf_bytes = self.replace_ter_code(buf_bytes, ter_code)

        if replace_time:
            buf_bytes = self.replace_ter_time(buf_bytes)

        # buf_bytes = self.crc_and_transferred(buf_bytes)

        return common.bytes_to_hex(buf_bytes)

    def gen_round_buffer(self, source_buf, round_=0):
        buf_list = []
        if isinstance(self.ter, str):
            ter_count = 1
            self.ter = [self.ter]
        else:
            ter_count = len(self.ter)

        if isinstance(source_buf, list):
            s_buf_list = source_buf
            buf_len = len(s_buf_list)
        else:
            s_buf_list = [source_buf]
            buf_len = 1

        for i in range(ter_count):
            buf_hex = s_buf_list[(round_+i) % buf_len]
            ter_code = self.ter[i]
            buf_bytes = common.hex_to_bytes(self.gen_buffer(buf_hex, ter_code, self.replace_time))
            buf_list.append(buf_bytes)

        return buf_list

    def get_msg_id(self, cp_bytes):
        msg_id_begin_pos = common.get_hex_len(
            self.proto_def_header['flag'],
            self.proto_def_header['version']
        )
        msg_id_end_pos = common.get_hex_len(
            self.proto_def_header['flag'],
            self.proto_def_header['msg_id'],
            self.proto_def_header['frame_type']
        )
        return common.bytes_to_hex(cp_bytes[msg_id_begin_pos: msg_id_end_pos], delimit='')

    def assemble_buf(self, frame_type, data):
        self.proto_def_header['frame_type'] = frame_type
        cp_header_hex = common.merge_hex(self.proto_def_header['header'])
        cp_tail = common.merge_hex(self.proto_def_tail)

        buf_bytes_hex = common.merge_hex(cp_header_hex, data, cp_tail)
        return buf_bytes_hex

    def get_buf_time(self, buf_bytes):

        return '20180725112754806'


class ProtoZnsh(Proto):
    def __init__(self, ter, replace_time=False):
        self.ter = ter
        self.replace_time = replace_time

    def replace_ter_code(self, buf_bytes, ter_code):
        buf_bytes = buf_bytes[0: 4] + ter_code.encode() + buf_bytes[15:]

        return buf_bytes

    def replace_ter_time(self, cp_bytes):
        date_str = datetime.datetime.now().strftime('%d%m%y')
        time_str = (datetime.datetime.now() + datetime.timedelta(hours=-8)).strftime('%H%M%S')
        datetime_str = date_str + ',' + time_str

        cp_bytes = cp_bytes[0: 29] + datetime_str + cp_bytes[42:]

        return cp_bytes

    def gen_buffer(self, buf_hex: str, ter_code=None, replace_time=True):
        buf_bytes = common.hex_to_bytes(buf_hex)

        if ter_code is None:
            ter_code = self.ter

        if isinstance(ter_code, list):
            ter_code = ter_code[0]

        buf_bytes = self.replace_ter_code(buf_bytes, ter_code)

        if replace_time:
            buf_bytes = self.replace_ter_time(buf_bytes)

        return common.bytes_to_hex(buf_bytes)

    def gen_round_buffer(self, source_buf, round_=0):
        buf_list = []
        if isinstance(self.ter, str):
            ter_count = 1
            self.ter = [self.ter]
        else:
            ter_count = len(self.ter)

        if isinstance(source_buf, list):
            s_buf_list = source_buf
            buf_len = len(s_buf_list)
        else:
            s_buf_list = [source_buf]
            buf_len = 1

        for i in range(ter_count):
            buf_hex = s_buf_list[(round_+i) % buf_len]
            ter_code = self.ter[i]
            buf_bytes = self.hex_to_bytes(self.gen_buffer(buf_hex, ter_code, self.replace_time))
            buf_list.append(buf_bytes)

        return buf_list

    def assemble_buf(self, msg_id, data):
        return data