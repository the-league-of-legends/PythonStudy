from protocol_test_service import protocol_test, gen_test_case, gen_test_case_common
from utils import db
import yt_base
import rpc

if __name__ == "__main__":
    handler = rpc.RPCHandler()
    handler.register_function(protocol_test.proto_test_service, 'proto_test_service')
    handler.register_function(protocol_test.send_data_service, 'send_data')
    handler.register_function(gen_test_case.gen_test_case, 'gen_test_case')
    handler.register_function(gen_test_case_common.gen_test_case, 'gen_test_case_common')
    handler.register_function(db.query_phoenix_data, 'query_phoenix_data')
    handler.register_function(db.query_redis_data, 'query_redis_data')
    rpc.rpc_server(handler, (yt_base.get_host_ip(), 30010), authkey=None)