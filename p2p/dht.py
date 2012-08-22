import json
import hashlib
import random
import threading
import uuid
import time

import udp
import timer
import event

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
