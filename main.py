import asyncio
from protocols import Raft, PeerProtocol
from config import config
from logging.config import dictConfig


def start_logger():
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'prod': {'format': '{asctime}: {levelname}: {message}',
                     'style': '{'},
        },
        'handlers': {
            'console': {'class': 'logging.StreamHandler',
                        'formatter': 'prod',
                        'level': 'INFO'}
        },
        'loggers': {
            '': {'handlers': ['console'],
                 'level': 'INFO'}
        }
    }

    dictConfig(logging_config)


def main():
    start_logger()

    loop = asyncio.get_event_loop()
    raft = Raft()

    coro = loop.create_datagram_endpoint(lambda: PeerProtocol(raft),
                                         local_addr=tuple(config.address))
    transport, _ = loop.run_until_complete(coro)
    raft.peer_transport = transport
    loop.run_forever()


if __name__ == '__main__':
    main()
