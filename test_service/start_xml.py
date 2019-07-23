from protocol_test_service import protocol_test, gen_test_case, gen_test_case_common
from base_service import db
import yt_base
from rpc.xml_rpc import RPCServer

if __name__ == "__main__":
    r = RPCServer((yt_base.get_host_ip(), 30020))
    r.register_function(protocol_test.proto_test_service, 'proto_test_service')
    r.register_function(protocol_test.send_data_service, 'send_data')
    r.register_function(gen_test_case.gen_test_case, 'gen_test_case')
    r.register_function(gen_test_case_common.gen_test_case, 'gen_test_case_common')
    r.register_function(db.query_phoenix_data, 'query_phoenix_data')
    r.serve_forever()