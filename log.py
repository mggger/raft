import os
import collections
import logging
from config import config
import utils

logger = logging.getLogger(__name__)


class Log(collections.UserList):

    def __init__(self, erase_log=False):
        super().__init__()

        self.path = os.path.join(config.storage, 'log')

        logger.info("Initializing log")

        if erase_log and os.path.isfile(self.path):
            os.remove(self.path)
            logging.info("Delete persisted data")

        elif os.path.isfile(self.path):
            self.data = utils.msgpack_appendable_unpack(self.path)
            logging.info("Using persisted data")

    def append_entries(self, entries, start):

        if len(self.data) >= start:
            self.replace(self.data[:start] + entries)

        else:
            self.data += entries
            utils.msgpack_appendable_pack(entries, self.path)

    def replace(self, new_data):
        if os.path.isfile(self.path):
            os.remove(self.path)

        self.data = new_data
        utils.msgpack_appendable_pack(self.data, self.path)


class DictStateMachine(collections.UserDict):
    def __init__(self, data={}, lastApplied=0):
        super().__init__(data)
        self.lastApplied = lastApplied

    def apply(self, items, end):
        items = items[self.lastApplied + 1: end + 1]
        for item in items:
            self.lastApplied += 1
            item = item['data']
            if item['action'] == 'change':
                self.data[item['key']] = item['value']
            elif item['action'] == 'delete':
                del self.data[item['key']]


class LogManager:
    def __init__(self, machine=DictStateMachine):
        self.log = Log()
        self.state_machine = machine()

        self.commitIndex = len(self.log)
        self.state_machine.apply(self, self.commitIndex)

    def __getitem__(self, index):
        if type(index) is slice:
            start = index.start if index.start else None
            stop = index.stop if index.stop else None
            return self.log[start:stop:index.step]
        elif type(index) is int:
            return self.log[index]

    @property
    def index(self):
        return len(self.log)

    def term(self, index=None):
        if index is None:
            return self.term(self.index)

        elif index == -1:
            return 0

        elif not len(self.log) or index <= self.compacted.index:
            return self.compacted.term
        else:
            return self[index]['term']

    def append_entries(self, entries, prevLogIndex):
        self.log.append_entries(entries, prevLogIndex)
        if entries:
            logger.info(f"Appending new log: {self.log.data}")

    def commit(self, leaderCommit):
        if leaderCommit <= self.commitIndex:
            return

        self.commitIndex = min(leaderCommit, self.index)
        logger.info(f"Advancing commit to {self.commitIndex}")

        self.state_machine.apply(self, self.commitIndex)
        logger.info(f" State machine {self.state_machine.data}")
