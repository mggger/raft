import os
import collections
import logging
from config import config
import utils
import msgpack
import asyncio

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


class Compactor():
    def __init__(self, count=0, term=None, data={}):
        self.count = count
        self.term = term
        self.data = data
        self.path = os.path.join(config.storage, 'compact')

        logger.info("Initializing compactor")
        if count or term or data:
            self.persist()
            logger.info("Using parameters")

        elif os.path.isfile(self.path):
            with open(self.path, 'rb') as f:
                self.__dict__.update(msgpack.unpack(f, encoding='uft-8'))
                logger.info("Using persisted data")

    @property
    def index(self):
        return self.count - 1

    def persist(self):
        with open(self.path, 'wb') as f:
            raw = {'count': self.count, 'term': self.term, 'data': self.data}
            msgpack.pack(raw, f, use_bin_type=True)


class LogManager:
    def __init__(self,
                 compact_count=0,
                 compact_term=None,
                 compact_data={},
                 machine=DictStateMachine):

        self.log = Log()
        self.compacted = Compactor(compact_count, compact_term, compact_data)
        self.state_machine = machine(data=self.compacted.data,
                                     lastApplied=self.compacted.index)

        self.commitIndex = self.compacted.index + len(self.log)
        self.state_machine.apply(self, self.commitIndex)

    def __getitem__(self, index):
        if type(index) is slice:
            start = index.start - self.compacted.count if index.start else None
            stop = index.stop - self.compacted.count if index.stop else None
            return self.log[start:stop:index.step]
        elif type(index) is int:
            return self.log[index - self.compacted.count]

    @property
    def index(self):
        return len(self.log) + self.compacted.index

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
        self.log.append_entries(entries, prevLogIndex - self.compacted.index)
        if entries:
            logger.info(f"Appending new log: {self.log.data}")

    def commit(self, leaderCommit):
        if leaderCommit <= self.commitIndex:
            return

        self.commitIndex = min(leaderCommit, self.index)
        logger.info(f"Advancing commit to {self.commitIndex}")

        self.state_machine.apply(self, self.commitIndex)
        logger.info(f" State machine {self.state_machine.data}")

        self.compaction_timer_touch()

    def compact(self):
        del self.compaction_timer
        if self.commitIndex - self.compacted.count < 60:
            return

        logger.info("Compaction started")

        not_compacted_log = self[self.state_machine.lastApplied + 1:]
        self.compacted.data = self.state_machine.data.copy()
        self.compacted.term = self.term(self.state_machine.lastApplied)
        self.compacted.count = self.state_machine.lastApplied + 1
        self.compacted.persist()
        self.log.replace(not_compacted_log)

        logger.info("Compacted: %s", self.compacted.data)
        logger.info("Log: %s", self.log.data)

    def compaction_timer_touch(self):
        if not hasattr(self, 'compaction_timer'):
            loop = asyncio.get_event_loop()
            self.compaction_timer = loop.call_later(0.01, self.compact)
