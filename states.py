import asyncio
import logging
from random import randrange
import os
from utils import PersistentDict
from log import LogManager
from config import config
import statistics

logger = logging.getLogger(__name__)


class State:
    def __init__(self, old_state=None, raft=None):
        if old_state:
            self.raft = old_state.raft
            self.persist = old_state.persist
            self.volatile = old_state.volatile
            self.log = old_state.log

        else:
            self.raft = raft
            self.persist = PersistentDict(os.path.join(config.storage, 'state'),
                                          {'voteFor': None, 'currentTerm': 0})

            self.volatile = {'laederId': None, 'cluster': config.cluster,
                             'address': config.address}

            self.log = LogManager()
            self._update_cluster()

    def data_received_peer(self, peer, msg):

        logger.info('Received %s from %s', msg['type'], peer)

        if self.persist['currentTerm'] < msg['term'] and peer in self.volatile['cluster']:
            self.persist['currentTerm'] = msg['term']
            if not type(self) is Follower:
                logging.info('Remote term is higher converting to Follower')
                self.raft.change_state(Follower)
                self.raft.state.data_received_peer(peer, msg)
                return

        method = getattr(self, 'on_peer_' + msg['type'], None)

        if method:
            method(peer, msg)
        else:
            logging.info(f'[heartbeat]  Unrecognized message from {peer}: {msg}')

    def data_received_client(self, protocol, msg):
        method = getattr(self, 'on_client_' + msg['type'], None)
        if method:
            method(protocol, msg)
        else:
            logger.info("Unrecognized message from %s: %s",
                        protocol.transport.get_extra_info('peername'), msg)

    def on_client_append(self, protocol, msg):
        """ 指定client发送消息给leader """
        msg = {'type': 'redirect',
               'leader': self.volatile['leaderId'] if 'leaderId' in self.volatile.keys() else None}
        protocol.send(msg)
        logger.info("Redirect msg to %s, from: %s", self.volatile['leaderId'], self.volatile['address'])

    def on_client_config(self, protocol, msg):
        return self.on_client_append(protocol, msg)

    def on_client_get(self, protocol, msg):
        return self.on_client_append(protocol, msg)

    def _update_cluster(self, entries=None):
        """ Interface for updating config """
        if 'cluster' in self.log.compacted.data:
            self.volatile['cluster'] = self.log.compacted.data['cluster']

        for entry in (self.log if entries is None else entries):
            if entry['data']['key'] == 'cluster':
                self.volatile['cluster'] = entry['data']['value']

        self.volatile['cluster'] = tuple(map(tuple, self.volatile['cluster']))


class Follower(State):
    def __init__(self, old_state=None, raft=None):
        super().__init__(old_state, raft)
        self.persist['voteFor'] = None
        self.restart_election_timer()

    def teardown(self):
        self.election_timer.cancel()

    def restart_election_timer(self):
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()

        timeout = randrange(1, 4) * 10 ** 0
        loop = asyncio.get_event_loop()
        self.election_timer = loop.call_later(timeout, self.raft.change_state, Candidate)
        logging.info(f"Election timer restarted: {timeout}")

    def on_peer_request_vote(self, peer, msg):

        term_is_current = msg['term'] >= self.persist['currentTerm']
        can_vote = self.persist['voteFor'] in [tuple(msg['candidateId']), None]

        index_is_current = (msg['lastLogTerm'] > self.log.term() or
                            (msg['lastLogTerm'] == self.log.term() and msg['lastLogIndex'] >= self.log.index))

        granted = term_is_current and can_vote and index_is_current

        if granted:
            self.persist['voteFor'] = msg['candidateId']
            self.restart_election_timer()

        logging.info(f"Voting for {peer}. Term: {term_is_current}, vote: {can_vote}, index: {index_is_current}")

        response = {
            'type': 'response_vote',
            'voteGranted': granted,
            'term': self.persist['currentTerm']
        }

        self.raft.send_peer(peer, response)

    def on_peer_append_entries(self, peer, msg):
        term_is_current = msg['term'] >= self.persist['currentTerm']
        prev_log_term_match = msg['prevLogTerm'] is None or \
                              self.log.term(msg['prevLogIndex']) == msg['prevLogTerm']
        success = term_is_current and prev_log_term_match

        if term_is_current:
            self.restart_election_timer()

        if 'compact_data' in msg:
            self.log = LogManager(compact_count=msg['compact_count'],
                                  compact_term=msg['compact_term'],
                                  compact_data=msg['compact_data'])

            self.volatile['leaderId'] = msg['leaderId']
            logger.info('Initialized Log with compact data from leader')

        elif success:
            self.log.append_entries(msg['entries'], msg['prevLogIndex'])
            self.log.commit(msg['leaderCommit'])
            self.volatile['leaderId'] = msg['leaderId']

            logging.info(f'Log index is now {self.log.index}')

        else:
            logging.warning("Could not append entries. cause: %s", 'wrong term' \
                if not term_is_current else 'prev log term mismatch')

        self._update_cluster()

        resp = {
            'type': 'response_append',
            'success': success,
            'term': self.persist['currentTerm'],
            'matchIndex': self.log.index
        }

        self.raft.send_peer(peer, resp)


class Candidate(Follower):

    def __init__(self, old_state=None, raft=None):
        super().__init__(old_state, raft)

        self.persist['currentTerm'] += 1
        self.vote_count = 0

        logging.info("new election Term: %s", self.persist['currentTerm'])
        self.send_vote_requests()

        def vote_self():
            self.persist['voteFor'] = self.volatile['address']
            self.on_peer_response_vote(
                self.volatile['address'], {'voteGranted': True})

        loop = asyncio.get_event_loop()
        loop.call_soon(vote_self)

    def send_vote_requests(self):
        logging.info('Broadcasting request_vote')

        msg = {
            'type': 'request_vote',
            'term': self.persist['currentTerm'],
            'candidateId': self.volatile['address'],
            'lastLogIndex': self.log.index,
            'lastLogTerm': self.log.term()}
        self.raft.broadcast_peers(msg)

    def on_peer_append_entries(self, peer, msg):
        logging.info("Converting to Follower")

        self.raft.change_state(Follower)
        self.raft.state.on_peer_append_entries(peer, msg)

    def on_peer_response_vote(self, peer, msg):
        self.vote_count += msg['voteGranted']
        logging.info(f'Vote count: {self.vote_count}')
        if self.vote_count > len(self.volatile['cluster']) / 2:
            self.raft.change_state(Leader)


class Leader(State):

    def __init__(self, old_state=None, raft=None):
        super().__init__(old_state, raft)
        logging.info("Leader of term: %s", self.persist['currentTerm'])

        self.volatile['leaderId'] = self.volatile['address']
        self.matchIndex = {p: 0 for p in self.volatile['cluster']}

        self.nextIndex = {p: self.log.commitIndex + 1 for p in self.matchIndex}
        self.waiting_clients = {}

        self.send_append_entries()

        if 'cluster' not in self.log.state_machine:
            self.log.append_entries(
                [{
                    'term': self.persist['currentTerm'],
                    'data': {'key': 'cluster',
                             'value': tuple(self.volatile['cluster']),
                             'action': 'change'
                             }
                }], self.log.index)

            self.log.commit(self.log.index)

    def teardown(self):
        self.append_timer.cancel()

        if hasattr(self, 'config_timer'):
            self.config_timer.cancel()

    def send_append_entries(self):
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue

            msg = {'type': 'append_entries',
                   'term': self.persist['currentTerm'],
                   'leaderCommit': self.log.commitIndex,
                   'leaderId': self.volatile['address'],
                   'prevLogIndex': self.nextIndex[peer] - 1,
                   'entries': self.log[self.nextIndex[peer]: self.nextIndex[peer] + 100]}

            msg.update({'prevLogTerm': self.log.term(msg['prevLogIndex'])})

            if self.nextIndex[peer] <= self.log.compacted.index:
                msg.update({"compact_data": self.log.compacted.data})
                msg.update({"compact_term": self.log.compacted.term})
                msg.update({"compact_count": self.log.compacted.count})

            logging.info('Sending %s entries to %s. Start index %s', len(msg['entries']), peer,
                         self.nextIndex[peer])

            self.raft.send_peer(peer, msg)

        timeout = randrange(1, 4) * 10 ** -1
        loop = asyncio.get_event_loop()

        self.append_timer = loop.call_later(timeout, self.send_append_entries)

    def on_peer_response_append(self, peer, msg):
        if msg['success']:
            self.matchIndex[peer] = msg['matchIndex']
            self.nextIndex[peer] = msg['matchIndex'] + 1

            self.matchIndex[self.volatile['address']] = self.log.index
            self.nextIndex[self.volatile['address']] = self.log.index + 1

            index = statistics.median_low(self.matchIndex.values())
            self.log.commit(index)
            self.send_client_append_response()
        else:
            self.nextIndex[peer] = max(0, self.nextIndex[peer] - 1)

    def on_client_config(self, protocol, msg):
        """ Push new cluster config. When uncommitted cluster changes
       are already present, retries until they are committed
       before proceding."""

        pending_configs = tuple(filter(lambda x: x['data']['key'] == 'cluster',
                                       self.log[self.log.commitIndex + 1:]
                                       ))

        if pending_configs:
            timeout = randrange(1, 4) * 10 ** -1
            loop = asyncio.get_event_loop()
            self.config_timer = loop.call_later(timeout, self.on_client_append, msg)
            return

        success = True

        cluster = set(self.volatile['cluster'])
        peer = (msg['address'], int(msg['port']))

        if msg['action'] == 'add' and peer not in cluster:
            logger.info('adding node %s', peer)
            cluster.add(peer)
            self.nextIndex[peer] = 0
            self.matchIndex[peer] = 0

        elif msg['action'] == 'delete' and peer in cluster:
            logger.info(f'Removing node {peer}')
            cluster.remove(peer)

            del self.nextIndex[peer]
            del self.matchIndex[peer]

        else:
            success = False

        if success:
            self.log.append_entries(
                [{'term': self.persist['currentTerm'],
                  'data': {'key': 'cluster', 'value': tuple(cluster), 'action': 'change'}
                  }],
                self.log.index
            )
            self.volatile['cluster'] = cluster
        protocol.send({'type': 'result', 'success': success})

    def on_client_append(self, protocol, msg):
        entry = {'term': self.persist['currentTerm'], 'data': msg['data']}

        if msg['data']['key'] == 'cluster':
            protocol.send({'type': 'result', 'success': False})
            return

        self.log.append_entries([entry], self.log.index)
        if self.log.index in self.waiting_clients:
            self.waiting_clients[self.log.index].append(protocol)
        else:
            self.waiting_clients[self.log.index] = [protocol]

        self.on_peer_response_append(
            self.volatile['address'], {'success': True, 'matchIndex': self.log.commitIndex}
        )

    def on_client_get(self, protocol, msg):
        state_machine = self.log.state_machine.data.copy()
        protocol.send(state_machine)

    def send_client_append_response(self):

        to_delete = []
        for client_index, clients in self.waiting_clients.items():

            if client_index <= self.log.commitIndex:
                for client in clients:
                    client.send({'type': 'result', 'success': True})
                    logger.info("Sent successful response to client")

                to_delete.append(client_index)

        for index in to_delete:
            del self.waiting_clients[index]
