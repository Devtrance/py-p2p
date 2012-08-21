import json
import hashlib
import random
import threading
import uuid
import time

import udp
import timer
import event

class Broadcaster(object):
    '''
    Implements an overlay network designed to minimize the number
    of hops in a network broadcast.

    Except "designed" doesn't really convey the amount of flailing
    going on, here.
    '''
    def __init__(self, bootstrap=(), port=6966, heartbeat=30, k=4):
        # XXX clean up self.possibles and self.seen regularly (every heartbeat?)
        self.peers = {}
        self.possibles = set()
        self.id = uuid.uuid4().hex
        self.udp = udp.UDP(port)
        self.seen = set()
        self.udp.handlers += self.handle_msg
        self.k = k
        self.lock = threading.RLock()
        self.timer = timer.Timer(heartbeat)
        self.timer += self.ping_all
        self.timer += self.find_friends
        self.timer += self.dump_baggage
        self.timer.start()
        self.event = event.Event()
        if bootstrap:
            self.bootstrap(bootstrap)

    def mkmsg(self, stamp=None):
        msg = {}
        msg['type'] = 'noop'
        msg['id'] = self.id
        if stamp:
            msg['stamp'] = stamp
        else:
            msg['stamp'] = uuid.uuid4().int
        return msg

    def bootstrap(self, addr):
        msg = self.mkmsg()
        msg['type'] = 'newguy'
        self._send(msg, addr)

    def send(data):
        msg = self.mkmsg()
        msg['data'] = data
        msg['type'] = 'data'
        self.broadcast(msg)

    def handle_msg(self, msg, addr):
        '''
        msg is a json-encoded message, and addr is an (ip, port) tuple
        '''
        with self.lock:
            self.possibles.add(addr)
            msg = json.loads(msg)
            if msg['stamp'] in self.seen:
                return
            self.seen.add(msg['stamp'])
            #print "%s >> %s: %s"%(msg['id'], self.id, msg['type'])
            if msg['type'] == 'bounce':
                self.broadcast(msg)
            if msg['type'] == 'ping':
                if not addr in self.peers:
                    pass
                pd = self.peers.get(addr, {})
                pd['id'] = msg['id']
                self.peers[addr] = pd
                self.pong(addr)
            elif msg['type'] == 'bye':
                if addr in self.peers:
                    del self.peers[addr]
                if len(self.peers) == 0:
                    # we have a right to be bitchy
                    self.bootstrap(addr)
            elif msg['type'] == 'pong':
                if not addr in self.peers:
                    if len(self.peers) >= self.k:
                        kick = random.choice(self.peers.keys())
                        del self.peers[kick]
                        self.bye(kick)
                pd = self.peers.get(addr, {})
                pd['exp'] = 0
                pd['id'] = msg['id']
                self.peers[addr] = pd
            elif msg['type'] == 'hi':
                if len(self.peers) < self.k:
                    snd = msg.get('src', None) or addr
                    snd = snd[0], snd[1]
                    self.peers[snd] = dict(id=msg['id'], exp=0)
                    self.pong(snd)
            elif msg['type'] == 'newguy':
                newmsg = self.mkmsg(msg['stamp'])
                newmsg['type'] = 'newguy'
                newmsg['id'] = msg['id']
                src = msg.get('src', None) or addr
                src = src[0], src[1]
                newmsg['src'] = src
                self.broadcast(newmsg)
                self.hi(src)
            elif msg['type'] == 'needfriend':
                newmsg = self.mkmsg(msg['stamp'])
                newmsg['type'] = 'needfriend'
                newmsg['id'] = msg['id']
                src = msg.get('src', None) or addr
                src = src[0], src[1]
                newmsg['src'] = src
                self.broadcast(newmsg)
                if len(self.peers) < self.k and not src in self.peers:
                    self.hi(src)
            elif msg['type'] == 'data':
                self.broadcast(msg)
                data = msg.get('data', None)
                if data:
                    self.event.fire(msg['data'])

    def hi(self, addr):
        msg = self.mkmsg()
        msg['type'] = 'hi'
        self._send(msg, addr)

    def bye(self, addr):
        msg = self.mkmsg()
        msg['type'] = 'bye'
        self._send(msg, addr)

    def needfriend(self, addr):
        msg = self.mkmsg()
        msg['type'] = 'needfriend'
        self._send(msg, addr)

    def broadcast(self, msg):
        with self.lock:
            for peer in self.peers:
                self._send(msg, peer)

    def dump_baggage(self):
        with self.lock:
            if len(self.peers) > self.k:
                kick = random.choice(self.peers.keys())
                del self.peers[kick]
                self.bye(kick)

    def find_friends(self):
        with self.lock: 
            if len(self.peers) < self.k and len(self.peers) > 0:
                print "I (%s) need a friend (%d)"%(self.id, len(self.peers))
                msg = self.mkmsg()
                msg['type'] = 'needfriend'
                self.broadcast(msg)
            if len(self.peers) == 0:
                for addr in self.possibles:
                    self.bootstrap(addr)

    def ping_all(self):
        with self.lock:
            todel = []
            for peer, stats in self.peers.iteritems():
                count = stats.get('exp', 0)
                if count > 5:
                    todel.append(peer)
                    continue
                stats['exp'] = count + 1
                self.ping(peer)
            for t in todel:
                del self.peers[t]

    def ping(self, addr):
        msg = self.mkmsg()
        msg['type'] = 'ping'
        self._send(msg, addr)

    def pong(self, addr):
        msg = self.mkmsg()
        msg['type'] = 'pong'
        self._send(msg, addr)

    def _send(self, msg, addr):
        self.seen.add(msg['stamp'])
        msg = json.dumps(msg)
        self.udp.send(msg, addr)

class DHT(object):
    '''
    Implements Kademlia (when it works, which isn't now)
    http://en.wikipedia.org/wiki/Kademlia
    http://xlattice.sourceforge.net/components/protocol/kademlia/specs.html
    '''
    def __init__(self, idseed, bootstrap=(), port=6965):
        self.buckets = {}
        self.id = hashlib.sha1(idseed).digest()
        self.udp = udp.UDP(port)
        self.udp.handlers += self.handle_msg
        if bootstrap:
            self.bootstrap(bootstrap)

    def xor(id1, id2):
        k = 0
        m = len(id1)
        for x in xrange(m):
            k += (ord(id1[x])^ord(id2[x])) * 256**((m - 1) - x)
        return k

    def sha2num(sha):
        k = 0
        m = len(sha)
        for x in xrange(m):
            k += (ord(sha[x]) * 256**((m - 1) - x))
        return k

    bucks = [2**x for x in xrange(160)]
    def bucketid(num):
        for x in xrange(len(self.bucks)):
            if n < self.bucks[x]:
                return x - 1
        return x

    def heartbeat(self):
        msg = json.dumps({'heartbeat': True})
        dlst = []
        for addr in self.peers:
            self.udp.send(msg, addr)
            self.peers[addr] += 1
            if self.peers[addr] > 5:
                dlst.append(addr)
        for d in dlst:
            del self.peers[addr]

    def handle_msg(self, data, addr):
        msg = json.loads(data)
        if msg.get('heartbeat', False):
            self.handle_heartbeat(addr)
        if msg.get('state', False):
            self.handle_state(msg['state'], addr)

    def handle_heartbeat(self, addr):
        self.peers[addr] = 0

    def handle_remove(self, addr):
        if self.peers.has_key(addr):
            del self.peers[addr]

    def handle_state(self, state):
        print "anyone for state?"
