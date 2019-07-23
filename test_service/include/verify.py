__author__ = 'zouhl'
import json
from rpc import send_rpc_msg
from yt_proto import proto_handler
import struct


class Verification:
    def __init__(self, result):
        self.result = result

    def _success(self):
        msg = 'VerifyResult>>> Passed'
        self.result.case_log.append(msg)

        return True

    def _key_exists_error(self, key):
        msg = 'VerifyResult>>> Failed|Key [{}] should not exists.'.format(key)
        self.result.case_log.append(msg)
        return msg

    def _value_error(self, key_path, expected, actual):
        msg = 'VerifyResult>>> Failed|Value error for [{0}]. Expected: {1}, Actual: {2}'.format(
            key_path, expected, actual
        )
        self.result.case_log.append(msg)
        return msg

    def _key_not_exists_error(self, key):
        msg = 'VerifyResult>>> Failed|Key [{}] not found.'.format(key)
        self.result.case_log.append(msg)
        return msg

    def _expected_not_valid_error(self, expected):
        msg = 'VerifyResult>>> Failed|Expected expression [{}] is not valid.'.format(expected)
        self.result.case_log.append(msg)
        return msg

    def _hbase_family_not_defined_error(self, family_name):
        msg = 'VerifyResult>>> Failed|Hbase family [{}] has not defined, need to update service'.format(family_name)
        self.result.case_log.append(msg)
        return msg

    def _check_hbase_data(self, expected, actual):
        # result = []
        key_path = expected.split('=')[0].strip()
        key_value = expected.split('=')[1].strip()
        tmp = key_path.split('.')
        check_family = tmp[0]

        if len(tmp) == 1:
            check_obj = None
        else:
            check_obj = tmp[1]

        if isinstance(actual, str):
            actual = json.loads(actual, encoding='latin')

        hbase_family = check_family

        def _sanitize_value(v):
            try:
                return eval(v)
            except:
                return v

        def _cmp_value(v1, v2):
            if isinstance(v1, list):

                def is_encoded(x):
                    for each in x:
                        if each < 46:
                            return False
                    return True

                if is_encoded(v1):
                    v1 = bytes(v1).decode()
                else:
                    if len(v1) == 4:
                        v1 = struct.unpack('>i', struct.pack('4B', *v1))[0]

            try:
                v1 = type(v2)(v1)
            except TypeError:
                pass

            if type(v1) != type(v2):
                return False

            if isinstance(v1, str):
                return v1 == v2

            return round(float(v1), 1) == round(float(v2), 1)

        def _key_should_not_exists():
            return key_value.lower() == 'na'

        if check_obj is None:
            if _key_should_not_exists():
                if hbase_family in actual:
                    # result.append(self._key_exists_error(key_path))
                    return self._key_exists_error(key_path)
                return None

            if hbase_family not in actual:
                # result.append(self._key_not_exists_error(key_path))
                return self._key_not_exists_error(key_path)

            proto_parsed = actual[hbase_family]

            if not _cmp_value(proto_parsed, _sanitize_value(key_value)):
                # result.append(self._value_error(key_path, key_value, proto_parsed))
                return self._value_error(key_path, key_value, proto_parsed)
        else:
            if hbase_family not in actual:
                # result.append(self._key_not_exists_error(key_path))
                return self._key_not_exists_error(key_path)
            hbase_family_data = bytes(actual[hbase_family])
            if check_family not in proto_handler.proto_handler:
                # result.append(self._hbase_family_not_defined_error(check_family))
                return self._hbase_family_not_defined_error(check_family)

            proto_parsed = proto_handler.parse_data(proto_handler.proto_handler[check_family], hbase_family_data)
            # hbase_data_parsed[check_family] = proto_parsed

            if check_obj in proto_parsed:
                if _key_should_not_exists():
                    # result.append(self._key_exists_error(key_path))
                    return self._key_exists_error(key_path)
                else:
                    if not _cmp_value(proto_parsed[check_obj], _sanitize_value(key_value)):
                        # result.append(self._value_error(key_path, key_value, proto_parsed[check_obj]))
                        return self._value_error(key_path, key_value, proto_parsed[check_obj])
            else:
                if _key_should_not_exists():
                    pass
                else:
                    # result.append(self._key_not_exists_error(key_path))
                    return self._key_not_exists_error(key_path)

        return None

    def _check_key_value_data(self, expected, actual, verify_type=None):
        def _sanitize_value(v):
            try:
                return eval(v)
            except:
                return v

        def _cmp_value(v1, v2):

            if isinstance(v2, dict) or isinstance(v2, list):
                v1 = _sanitize_value(v1)

            if isinstance(v1, int):
                v1 = float(v1)

            if isinstance(v2, int):
                v2 = float(v2)

            if type(v1) != type(v2):
                return False

            if isinstance(v2, dict):
                try:
                    v1 = json.loads(json.dumps(v1).upper())
                except Exception as e:
                    print(e)
                    print(v1)
                v2 = json.loads(json.dumps(v2).upper())
                if len(v1) != len(v2):
                    return False
                for key in v1:
                    if key.lower() not in v2:
                        return False
                    return _cmp_value(v1[key], v2[key])

            elif isinstance(v2, list):
                if len(v1) != len(v2):
                    return False
                for src_list, dst_list in zip(sorted(v1), sorted(v2)):
                    return _cmp_value(src_list, dst_list)
            else:
                try:
                    v1 = type(v2)(v1)
                except TypeError:
                    pass

                if type(v1) != type(v2):
                    return False

                if isinstance(v1, str):
                    return v1.upper() == str(v2).upper()

                return round(float(v1), 3) == round(float(v2), 3)
        key_path = expected.split('=')[0].strip()
        if verify_type == 'phoenix':
            key_path = key_path.upper()
        key_value = expected.split('=')[1].strip()

        key_layer = 0
        if isinstance(actual, str):
            try:
                actual = json.loads(actual, encoding='latin')
            except Exception as e:
                print(actual)
                print(e)
        for key in key_path.split('.'):
            key_layer += 1

            def is_last_layer():
                return key_layer == len(key_path.split('.'))

            def key_should_not_exists():
                return key_value.lower() == 'na'

            if is_last_layer():
                if key_should_not_exists():
                    if key in actual and actual[key] is not None:
                        # result.append(self._key_exists_error(key_path))
                        return self._key_exists_error(key_path)
                else:
                    if key not in actual:
                        # result.append(self._key_not_exists_error(key_path))
                        return self._key_not_exists_error(key_path)
                    if not _cmp_value(actual[key], _sanitize_value(key_value)):
                        # result.append(self._value_error(key_path, key_value, actual[key]))
                        return self._value_error(key_path, key_value, actual[key])
            else:
                if key in actual:
                    actual = actual[key]
                else:
                    # result.append(self._key_not_exists_error(key_path))
                    return self._key_not_exists_error(key_path)

        return None

    def _check_json_data(self, expected, actual):
        error_list = []

        def _cmp_dict(v1, v2):
            if type(v1) != type(v2):
                error_list.append(('type error', v1, v2))
                return None
            if isinstance(v1, dict):
                if len(v1) != len(v2):
                    error_list.append(('length error', v1, v2))
                    return None
                for key in v1:
                    if key not in v2:
                        error_list.append(('key not exists', key))
                        return None
                    _cmp_dict(v1[key], v2[key])
            elif isinstance(v1, list):
                if len(v1) != len(v2):
                    error_list.append(('length error', v1, v2))
                    return None
                for src_list, dst_list in zip(sorted(v1), sorted(v2)):
                    _cmp_dict(src_list, dst_list)
            else:
                if v1 != v2:
                    error_list.append(('value error', v1, v2))
                    return None
                else:
                    return None

        _cmp_dict(expected, actual)
        if error_list:
            return error_list
        return self._success()

    def _expected_not_valid(self, exp):
        if len(exp.split('=')) != 2:
            return self._expected_not_valid_error(exp)
        return None

    def verify_data(self, verify_type, expected, actual):
        result = []

        try:
            expected_formated = json.loads(expected, encoding='latin')
        except ValueError:
            expected_formated = expected

        if isinstance(expected_formated, dict):   # 判断expected_formated 是否是dict类型。
            try:
                actual_formated = json.loads(actual, encoding='latin')
            except ValueError:
                actual_formated = actual

            dict_cmp_result = self._check_json_data(expected_formated, actual_formated)
            if isinstance(dict_cmp_result, list):
                return self._value_error('json', True, dict_cmp_result)
            else:
                return True
        else:
            for check_field in expected.split(';'):
                if not check_field:
                    continue
                r = self._expected_not_valid(check_field)
                if r is not None:
                    return r

                if verify_type == 'hbase':
                    r = self._check_hbase_data(check_field, actual)
                else:
                    r = self._check_key_value_data(check_field, actual, verify_type)

                if r is not None:
                    result.append(r)
                    # return r

            if result:
                return result
            else:
                return self._success()