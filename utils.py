import collections
import msgpack
import os
import json
from msgpack.exceptions import OutOfData

MAX_MSGPACK_ARRAY_HEADER_LEN = 5


class PersistentDict(collections.UserDict):
    def __init__(self, path=None, data={}):
        if os.path.isfile(path):
            with open(path, 'r') as f:
                data = json.loads(f.read())
        self.path = path
        super().__init__(data)

    def __setitem__(self, key, value):
        self.data[self.__keytransform__(key)] = value
        self.persist()

    def __delitem__(self, key):
        del self.data[self.__keytransform__(key)]
        self.persist()

    def __keytransform__(self, key):
        return key

    def persist(self):
        with open(self.path, 'w+') as f:
            f.write(json.dumps(self.data))


def msgpack_appendable_pack(o, path):
    open(path, 'a+').close()  # touch
    with open(path, 'r+b') as f:
        packer = msgpack.Packer()
        unpacker = msgpack.Unpacker(f)

        if type(o) == list:
            try:
                previous_len = unpacker.read_array_header()
            except OutOfData:
                previous_len = 0

            header = packer.pack_array_header(previous_len + len(o))

            f.seek(0)
            f.write(header)
            f.write(bytes(1) * (MAX_MSGPACK_ARRAY_HEADER_LEN - len(header)))

            f.seek(0, 2)
            for element in o:
                f.write(packer.pack(element))
        else:
            f.write(packer.pack(o))


def msgpack_appendable_unpack(path):
    # if not list
    # return msgpack.unpackb(f.read())

    with open(path, 'rb') as f:
        packer = msgpack.Packer()
        unpacker = msgpack.Unpacker(f, encoding='utf-8')
        length = unpacker.read_array_header()

        header_length_bin = len(packer.pack_array_header(length))
        unpacker.read_bytes(MAX_MSGPACK_ARRAY_HEADER_LEN - header_length_bin)
        f.seek(MAX_MSGPACK_ARRAY_HEADER_LEN)

        return [unpacker.unpack() for _ in range(length)]
