import pickle
from multiprocessing.connection import Listener
from threading import Event
from queue import Queue
from concurrent.futures import ThreadPoolExecutor


_END = '[[end]]'


def send_rpc_msg(rpc_msg_q, msg):
    rpc_msg_q.put(pickle.dumps(msg))


def rpc_server(handler, address, authkey):
    print('Service Started.')
    print('-' * 120)
    sock = Listener(address, authkey=authkey)
    pool1 = ThreadPoolExecutor(128)
    pool2 = ThreadPoolExecutor(128)
    while True:
        client = sock.accept()
        print('StartTest>>> Receive client connected.')
        connection = RPCConnection(handler, client)
        pool1.submit(connection.handle_connection)
        pool2.submit(connection.msg_sync)
        # t = Thread(target=connection.handle_connection)
        # t.daemon = True
        # t.start()
        #
        # s = Thread(target=connection.msg_sync)
        # s.daemon = True
        # s.start()
        #
        # t.join()
        # s.join()
        # print('TestTerminate[{}]>>> Client closed.'.format(_client_id))


class RPCConnection:
    def __init__(self, handler, conn):
        self.msg_q = Queue()
        self.terminate = Event()
        self.handler = handler
        self.conn = conn

    def handle_connection(self):
        try:
            while True:
                # Receive a message
                func_name, args, kwargs = pickle.loads(self.conn.recv())
                # Run the RPC and send a response
                try:
                    r = self.handler.functions[func_name](self.msg_q, self.terminate, *args, **kwargs)
                    if self.terminate.is_set():
                        break
                    self.conn.send(pickle.dumps(r))
                    self.terminate.set()
                except Exception as e:
                    if self.terminate.is_set():
                        break
                    self.conn.send(pickle.dumps(e))
                    self.terminate.set()

        except EOFError:
            self.terminate.set()

    def msg_sync(self):
        while True:
            rpc_msg = pickle.loads(self.msg_q.get())
            if rpc_msg == _END:
                break
            try:
                self.conn.send(pickle.dumps(rpc_msg))
            except:
                self.terminate.set()
                print('StopTest>>> Test terminate by client.')
                break


class RPCHandler:
    def __init__(self):
        self.functions = {}

    def register_function(self, func, func_name=None):
        if not func_name:
            func_name = func.__name__
        self.functions[func_name] = func
