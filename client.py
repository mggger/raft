import msgpack
import collections
import socket
import random


class Client:
    def _request(self, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


        sock.connect(self.server_address)
        sock.send(msgpack.packb(message, use_bin_type=True))

        buff = bytes()
        while True:
            block = sock.recv(128)
            if not block:
                break
            buff += block
        resp = msgpack.unpackb(buff, encoding='utf-8')
        sock.close()
        if 'type' in resp and resp['type'] == 'redirect':
            self.server_address = tuple(resp['leader'])
            resp = self._request(message)
        return resp

    def _get_state(self):
        """Retrive remote state machine."""
        self.server_address = tuple(random.choice(self.data['cluster']))
        return self._request({'type': 'get'})

    def _append_log(self, payload):
        """Append to remote log."""
        return self._request({'type': 'append', 'data': payload})


class DistributeDict(collections.UserDict, Client):

    def __init__(self, addr, port):
        super().__init__()
        self.data = {'cluster': [(addr, port)]}
        self.refresh()

    def __getitem__(self, key):
        self.refresh()
        return self.data[key]

    def __setitem__(self, key, value):
        self._append_log({'action': 'change', 'key': key, 'value': value})

    def __delitem__(self, key):
        if key is self.data.keys():
            del self.data[key]
            self._append_log({'action': 'delete', 'key': key})

    def refresh(self):
        self.data = self._get_state()
