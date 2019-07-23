from xmlrpc.server import SimpleXMLRPCServer



class RPCServer:
    def __init__(self, address):
        self._data = {}
        self._serv = SimpleXMLRPCServer(address, allow_none=True)
        # for name in self._rpc_methods_:
        #     self._serv.register_function(getattr(self, name))

    def register_function(self, function, name):
        self._serv.register_function(function, name)

    def serve_forever(self):
        self._serv.serve_forever()
