import socket
import time


class AquilaTest(object):
    def __init__(self, server, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((server, port))

    def send_data(self, data):
        if isinstance(data, list):
            for buf in data:
                self.sock.send(buf)
        else:
            self.sock.sendall(data)
        time.sleep(5)

    def recv_data(self, max_size):
        data = self.sock.recv(max_size)
        return data

    def close(self):
        self.sock.close()
    # end