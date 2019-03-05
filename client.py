import asyncio
import msgpack
import os


class ConfigClient(asyncio.Protocol):

    def __init__(self, message, on_con_lost, loop):
        self.message = message
        self.loop = loop
        self.on_con_lost = on_con_lost

    def connection_made(self, transport):
        transport.write(msgpack.packb(self.message, use_bin_type=True))

    def data_received(self, data):
        msg = msgpack.unpackb(data, encoding="utf-8")
        if msg['type'] == 'redirect':
            self.reply = {
                'type': 'redirect',
                'leader': tuple(msg['leader']),
                'success': False
            }

        else:
            self.reply = {
                'type': 'reply',
                'success': msg['success']
            }

    def connection_lost(self, exc):
        self.on_con_lost.set_result(self.reply)


def _make_up_msg(address: tuple, action):
    msg = {
        "type": "config",
        "address": address[0],
        "port": int(address[1]),
        "action": action
    }
    return msg


async def main():
    address = ("127.0.0.1", 5256)
    message = _make_up_msg(address, "delete")

    loop = asyncio.get_event_loop()
    on_con_lost = loop.create_future()

    transport, protocol = await loop.create_connection(lambda: ConfigClient(message, on_con_lost, loop), "127.0.0.1",
                                                       int(os.getenv("id", 0)))

    reply = await on_con_lost
    transport.close()

    if reply['type'] == "redirect":
        print("redirect to leader")
        address, port = reply['leader']

        re_futrue = loop.create_future()
        transport, protocol = await loop.create_connection(lambda: ConfigClient(message, re_futrue, loop), address,
                                                           int(port))

        reply = await re_futrue
        transport.close()

    print("config_update: ", reply['success'])


asyncio.run(main())
