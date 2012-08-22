import json
import hashlib
import random
import threading
import uuid
import time

import udp
import timer
import event
import mrq

class Broadcaster(object):
    '''
    Implements an overlay network designed to minimize the number
    of hops in a network broadcast.

    Except "designed" doesn't really convey the amount of flailing
    going on, here.
    '''
    def __init__(self, bootstrap=(), port=6966, heartbeat=30, k=20):
        self.peers = {}
        self.possibles = mrq.MRQ(50)
        self.id = uuid.uuid4().hex
        self.udp = udp.UDP(port)
        self.seen = mrq.MRQ(2500)
        self.udp.handlers += self.handle_msg
        self.k = k
        self.lock = threading.RLock()
        self.timer = timer.Timer(heartbeat)
        self.timer += self.ping_all
        self.timer += self.find_friends
        self.timer += self.dump_baggage
        self.event = event.Event()
        self.boot = bootstrap

    def start(self):
        self.udp.start()
        if self.boot:
            self.bootstrap(self.boot)
        self.timer.start()

    def stop(self):
        with self.lock:
            for p in self.peers:
                self.bye(p)
            self.timer.disable()
            self.udp.shutdown()

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

    def send(self, data):
        msg = self.mkmsg()
        msg['data'] = data
        msg['type'] = 'data'
        self.broadcast(msg)

    def send_one(self, data):
        msg = self.mkmsg()
        msg['data'] = data
        msg['type'] = 'oncedata'
        dst = random.choice(self.peers.keys())
        self._send(msg, dst)

    def handle_msg(self, msg, addr):
        '''
        msg is a json-encoded message, and addr is an (ip, port) tuple
        '''
        with self.lock:
            self.possibles += addr
            msg = json.loads(msg)
            if msg['stamp'] in self.seen:
                return
            self.seen += msg['stamp']
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
            elif msg['type'] == 'oncedata':
                data = msg.get('data', None)
                if data:
                    def reply(data):
                        msg = self.mkmsg()
                        msg['type'] = 'oncedata'
                        msg['data'] = data
                        self._send(msg, addr)
                    self.event.fire(data, reply=reply)

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
        self.seen += msg['stamp']
        msg = json.dumps(msg)
        self.udp.send(msg, addr)
