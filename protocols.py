import asyncio
import logging
import msgpack
import os
from states import Follower
from config import config
from utils import extended_msgpack_serializer

logger = logging.getLogger(__name__)


class Raft:
    def __init__(self):
        os.makedirs(config.storage, exist_ok=True)
        self.state = Follower(raft=self)

    def change_state(self, new_state):
        self.state.teardown()
        logging.info('State change ' + new_state.__name__)
        self.state = new_state(old_state=self.state)

    def data_received_peer(self, sender, message):
        self.state.data_received_peer(sender, message)

    def data_received_client(self, transport, message):
        self.state.data_received_client(transport, message)

    def send(self, transport, message):
        transport.sendto(msgpack.packb(message, use_bin_type=True, default=extended_msgpack_serializer))

    def send_peer(self, recipient, message):
        if recipient != self.state.volatile['address']:
            self.peer_transport.sendto(
                msgpack.packb(message, use_bin_type=True), tuple(recipient)
            )

    def broadcast_peers(self, message):
        for recipient in self.state.volatile['cluster']:
            self.send_peer(recipient, message)


class PeerProtocol(asyncio.Protocol):
    """ udp protocol for communicating with peers  """

    def __init__(self, raft, first_message=None):
        self.raft = raft
        self.first_message = first_message

    def connection_made(self, transport):
        self.transport = transport

        if self.first_message:
            transport.sendto(msgpack.packb(self.first_message, use_bin_type=True))

    def datagram_received(self, data, sender):
        message = msgpack.unpackb(data, encoding="utf-8")
        self.raft.data_received_peer(sender, message)

    def error_received(self, ex):
        logging.error(f"Error: {ex}")


class ClientProtocol(asyncio.Protocol):

    def __init__(self, raft):
        self.raft = raft

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        message = msgpack.unpackb(data, encoding="utf-8")
        self.raft.data_received_client(self, message)

    def connection_lost(self, exc):
        logger.info(f'close connection with client %s:%s', *self.transport.get_extra_info('peername'))

    def send(self, message):
        self.transport.write(msgpack.packb(message, use_bin_type=True, default=extended_msgpack_serializer))
        self.transport.close()
