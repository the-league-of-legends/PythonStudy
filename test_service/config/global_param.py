import os
from yt_base.read_config import ReadConfig

config_file_path = os.path.split(os.path.realpath(__file__))[0]
read_config = ReadConfig(os.path.join(config_file_path, "config.ini"))

log_path = read_config.get_value('log_dir', 'path')
log_level = read_config.get_value('log_dir', 'level')

# --------------------------------------------------------------------------------------------------
# ---------------------------------------- test_env ------------------------------------------------
test_env = dict()
test_env['2.0'] = dict()
test_env['2.0']['aquila_server'] = read_config.get_value('test_env_2.0', 'aquila_server')
test_env['2.0']['hbase_server'] = read_config.get_value('test_env_2.0', 'hbase_server')
test_env['2.0']['redis_server'] = read_config.get_value('test_env_2.0', 'redis_server')
test_env['2.0']['oracle_server'] = read_config.get_value('test_env_2.0', 'oracle_server')
test_env['2.0']['oracle_user'] = read_config.get_value('test_env_2.0', 'oracle_user')
test_env['2.0']['es_server'] = read_config.get_value('test_env_2.0', 'es_server')
test_env['2.0']['kafka_server_cp'] = read_config.get_value('test_env_2.0', 'kafka_server_cp')
test_env['2.0']['kafka_server_app'] = read_config.get_value('test_env_2.0', 'kafka_server_app')
test_env['2.0']['rabbit_mq_server'] = read_config.get_value('test_env_2.0', 'rabbit_mq_server')
test_env['2.0']['rabbit_mq_user'] = read_config.get_value('test_env_2.0', 'rabbit_mq_user')

test_env['3.0'] = dict()
test_env['3.0']['aquila_server'] = read_config.get_value('test_env_3.0', 'aquila_server')
test_env['3.0']['phoenix_server'] = read_config.get_value('test_env_3.0', 'phoenix_server')
test_env['3.0']['redis_server'] = read_config.get_value('test_env_3.0', 'redis_server')
test_env['3.0']['kafka_server_cp'] = read_config.get_value('test_env_3.0', 'kafka_server_cp')
test_env['3.0']['kafka_server_app'] = read_config.get_value('test_env_3.0', 'kafka_server_app')

test_env['local'] = dict()
test_env['local']['aquila_server'] = read_config.get_value('local', 'aquila_server')
test_env['local']['phoenix_server'] = read_config.get_value('local', 'mysql_server')
test_env['local']['redis_server'] = read_config.get_value('local', 'redis_server')
# --------------------------------------------------------------------------------------------------
# ------------------------------------------- Aquila port ------------------------------------------
aquila_port = {
    "xc": 30011,
    'xny': 30020,
    'tm': 30030,
    'jw': 30040,
    'jyd': 30050,
    'rsu': 30060,
    'znsh': 30080
}
# -----------------------------------------------------------------------------------------------------
# ------------------------------------------- RocketMQ topic ------------------------------------------
rocketmq_topic = {
    'COMMAND_REQUEST_TOPIC',
    'IOV-FULL-TYPE-0200',
    'IOV-MAIN-TYPE-COMMON',
    'IOV-MAIN-TYPE-LOCATION',
    'IOV-REL-TYPE-REVISE',
    'OFFSET_MOVED_EVENT',
    'PUBSUB_EVENT_TOPIC',
    'cp.up.msg'
}
# --------------------------------------------------------------------------------------------------
# ------------------------------------------- Kafka topic ------------------------------------------
kafka_topic = {
    '2.0': {
        'cp': [
            'cp.up.msg',
            'cp.up.cap.location',
            'cp.up.cap.maintenance',
            'cp.up.cap.switch',
            'cp.up.cap.inspect',
            'cp.up.cap.common'
        ],
        'app': [
            'iov-dc-track-rtmsg',
            'iov-dc-mt-rtmsg',
            'iov-dc-mtio-msg',
            'iov-dc-powerpile-msg',
            'iov-dc-adas-alarm',
            'iov-dc-road-spectrum-collection',
            'cp.down.cap',
            'cp.down.msg.dev14'
        ]
    },
    '3.0': {
        'cp': [
            'cp.up.msg',
            'IOV-MAIN-TYPE-LOCATION',
            'IOV-MAIN-TYPE-MAINTENANCE',
            'IOV-MAIN-TYPE-SWITCH',
            'IOV-MAIN-TYPE-INSPECT',
            'IOV-MAIN-TYPE-COMMON',
            'IOV-MAIN-TYPE-TRAFFICLIGHT'
        ],
        'app': [
            'IOV-FULL-TYPE-0200',
            'IOV-FULL-TYPE-0F80',
            'IOV-FULL-TYPE-0F99',
            'IOV-FULL-TYPE-DZJG',
            'IOV-FULL-TYPE-GEMINI',
            'IOV-FULL-TYPE-PERSISTENCE',
            'IOV-NO-SPLIT-DATA',
            'IOV-REL-TYPE-BUSINESS-NOTIFY',
            'IOV-REL-TYPE-BUSINESS-WRITE',
            'IOV-REL-TYPE-COMMAND-ANSWER',
            'IOV-REL-TYPE-FAULT',
            'IOV-REL-TYPE-REVISE',
            'IOV-ROUTE-DEAD-INFO',
            'IOV-SUB-TYPE-0F90',
            'IOV-SUB-TYPE-ADAS',
            'IOV-SUB-TYPE-AIR-CONDITION',
            'IOV-SUB-TYPE-ALARM-INFO',
            'IOV-SUB-TYPE-ALARM-INFO-OUTPUT',
            'IOV-SUB-TYPE-CHARGE-RECORD',
            'IOV-SUB-TYPE-DOOR-INFO',
            'IOV-SUB-TYPE-EATON-SYSTEM',
            'IOV-SUB-TYPE-ELECTRIC-MACHINERY',
            'IOV-SUB-TYPE-ELECTRICAL-ACCESSORIES',
            'IOV-SUB-TYPE-ENGINE-INFO',
            'IOV-SUB-TYPE-EVENT',
            'IOV-SUB-TYPE-EVENT-OUTPUT',
            'IOV-SUB-TYPE-FAULT',
            'IOV-SUB-TYPE-FAULT-DETAIL',
            'IOV-SUB-TYPE-FAULT-DETAIL-OUTPUT',
            'IOV-SUB-TYPE-FUEL-BATTERY',
            'IOV-SUB-TYPE-FUGONG-SYSTEM',
            'IOV-SUB-TYPE-GEMINI',
            'IOV-SUB-TYPE-GENERAL-INFO',
            'IOV-SUB-TYPE-GPS-INFO',
            'IOV-SUB-TYPE-HW-INFO',
            'IOV-SUB-TYPE-JYD-PERIOD',
            'IOV-SUB-TYPE-LIGHT-INFO',
            'IOV-SUB-TYPE-NANCHE-INFO',
            'IOV-SUB-TYPE-POWER-BATTERY',
            'IOV-SUB-TYPE-POWER-INFO',
            'IOV-SUB-TYPE-QCBATTERY-INFO',
            'IOV-SUB-TYPE-REDIS',
            'IOV-SUB-TYPE-SONGZENG-INFO',
            'IOV-SUB-TYPE-STATE-INFO',
            'IOV-SUB-TYPE-TERMINAL-INFO',
            'IOV-SUB-TYPE-TPMS-INFO',
            'IOV-SUB-TYPE-TRIP-INFO',
            'IOV-SUB-TYPE-TROLLEY-BUS',
            'IOV-SUB-TYPE-ULTRACAPACITOR-INFO',
            'IOV-SUB-TYPE-VOBCSTATUS'
        ]
    }
}
# --------------------------------------------------------------------------------------------------