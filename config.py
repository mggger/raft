import os

class Config:
    def __init__(self, config={}):
        if config is None:
            self.__dict__ = {}

        elif config:
            self.__dict__ = config


config = Config(
    {"address": ("127.0.0.1", int(os.getenv("id"))),
     "storage": "persist_" + os.getenv("id"),
     "cluster": [("127.0.0.1", 5254), ("127.0.0.1", 5255), ("127.0.0.1", 5256)]}
)