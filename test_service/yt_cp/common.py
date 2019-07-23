from collections import Iterable


# def get_hex_data_len(hex_str):
#     return len(hex_to_bytes(hex_str))


def hex_to_bytes(hex_str):
    '''

    :param hex_str:
    :return:
    '''
    hex_str = hex_str.strip()
    try:
        buf_bytes = [int(x, 16) for x in hex_str.split(' ')]
    except:
        raise 'Invalid hex string: {}'.format(hex_str)

    return bytes(buf_bytes)


def bytes_to_hex(hex_bytes, delimit=' '):
    return delimit.join(["%02X" % x for x in hex_bytes]).strip()


def gen_bcd(data, length):
    if isinstance(data, int):
        data_hex = hex(data)[2:]
    else:
        data_hex = data
    data_format = data_hex.zfill(length * 2)
    bcd_hex = ' '.join([data_format[i*2:(i+1)*2] for i in range(length)])
    return bcd_hex


def get_hex_len(*hex_buf):

    hex_len = 0

    if len(hex_buf) == 1:
        hex_buf = hex_buf[0]

    if isinstance(hex_buf, str):
        return len(hex_to_bytes(hex_buf))

    if isinstance(hex_buf, dict):
        for each in hex_buf:
            hex_len += get_hex_len(hex_buf[each])
    elif isinstance(hex_buf, Iterable):
        for each in hex_buf:
            hex_len += get_hex_len(each)

    return hex_len


def merge_hex(*hex_buf):
    merged_bytes = bytes()

    if len(hex_buf) == 1:
        hex_buf = hex_buf[0]

    if isinstance(hex_buf, str):
        return hex_buf.strip()

    if isinstance(hex_buf, dict):
        for each in hex_buf:
            merged_bytes += hex_to_bytes(merge_hex(hex_buf[each]))
    elif isinstance(hex_buf, Iterable):
        for each in hex_buf:
            merged_bytes += hex_to_bytes(merge_hex(each))

    return bytes_to_hex(merged_bytes)
