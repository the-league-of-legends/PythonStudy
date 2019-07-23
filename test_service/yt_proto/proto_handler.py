__author__ = 'zouhl'

from yt_proto import vehicletrack_pb2

proto_handler = dict()
proto_handler['cling_info'] = vehicletrack_pb2.CLingInfo()
proto_handler['power_info'] = vehicletrack_pb2.PowerInfo()
proto_handler['cond_data_info'] = vehicletrack_pb2.ConditionDataInfo()
proto_handler['state_info'] = vehicletrack_pb2.StateInfo()
proto_handler['fuel_cell_info'] = vehicletrack_pb2.FuelCellInfo()
proto_handler['mvcs_info'] = vehicletrack_pb2.MvcsInfo()
proto_handler['hw_info'] = vehicletrack_pb2.HWInfo()
proto_handler['nanche_info'] = vehicletrack_pb2.NanCheInfo()
proto_handler['songzeng_info'] = vehicletrack_pb2.SongZengInfo()
proto_handler['light_info'] = vehicletrack_pb2.LightInfo()
proto_handler['jyd_tail_gas_info'] = vehicletrack_pb2.JydTailGasInfo()
proto_handler['event_info'] = vehicletrack_pb2.EventInfo()
proto_handler['door_info'] = vehicletrack_pb2.DoorInfo()
proto_handler['engine_info'] = vehicletrack_pb2.EngineExtraData()
proto_handler['automatic_drive_info'] = vehicletrack_pb2.AutomaticDriveInfo()
proto_handler['terminal_info'] = vehicletrack_pb2.TerminalInfo()
proto_handler['new_energy_dev_info'] = vehicletrack_pb2.NewEnergyDevInfo()
proto_handler['gps_info'] = vehicletrack_pb2.GPSInfo()
proto_handler['sim_flux_info'] = vehicletrack_pb2.SimFluxInfo()
proto_handler['eaton_info'] = vehicletrack_pb2.EatonInfo()


def parse_data(obj, data):
    obj.ParseFromString(data)
    parsed_data = dict()

    for item in str(obj).split('\n'):
        if not item or ': ' not in item:
            continue
        parsed_data[item.split(': ')[0]] = eval(item.split(': ')[1])
    return parsed_data


