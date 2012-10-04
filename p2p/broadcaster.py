import json
import hashlib
import random
import threading
import uuid
import time
import base64

import udp
import timer
import event
import mrq
import itc

class Broadcaster(object):
    '''
    Implements an overlay network designed to minimize the number
    of hops in a network broadcast.

    Except "designed" doesn't really convey the amount of flailing
    going on, here.
    '''
    def __init__(self, bootstrap=(), port=6966, heartbeat=30, joincb=None):
        self.peers = {}
        self.id = uuid.uuid4().hex
        self.value = (0, 0)
        self.lace_max = (0, 0)
        self.udp = udp.UDP(port)
        self.seen = mrq.MRQ(2500)
        self.possibles = mrq.MRQ(50)
        self.udp.handlers += self.handle_msg
        self.lock = threading.RLock()
        self.timer = timer.Timer(heartbeat)
        self.handlers = event.Event()
        self.boot = bootstrap
        self.joincb = joincb
        self.clock = None
        # maekawa state variables
        self.acqcb = None
        self.grant = None
        self.maeq = []
        self.mutexed = False
        self.fails = []
        self.yields = []
        self.grants = []

    def base(self):
        self.value = self.lace_max = (1, 1)
        self.clock = itc.Stamp()

    def dumpstamp(self, stamp):
        return base64.b64encode(stamp.dump())

    def loadstamp(self, stampstr):
        return itc.Stamp.load(base64.b64decode(stampstr))

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

    def mkmsg(self, msgtype='noop'):
        msg = {}
        msg['type'] = msgtype
        msg['id'] = (self.id, self.value)
        msg['stamp'] = uuid.uuid4().int
        if self.clock:
            msg['clock'] = self.dumpstamp(self.clock.peek())
        return msg

    def send(self, data):
        msg = self.mkmsg()
        msg['data'] = data
        msg['type'] = 'data'
        self.broadcast(msg)

    def send_one(self, data):
        msg = self.mkmsg()
        msg['data'] = data
        msg['type'] = 'oncedata'
        dst = None
        with self.lock:
            if len(self.peers) > 0:
                dst = random.choice(self.peers.keys())
        if dst:
            self.sendmsg(msg, dst)

    def bootstrap(self, plist):
        msg = self.mkmsg()
        msg['type'] = 'hello'
        for p in plist:
            self.sendmsg(msg, p)

    def handle_msg(self, msg, addr):
        '''
        msg is a json-encoded message, and addr is an (ip, port) tuple
        '''
        # XXX do something fancy with getaddr here
        msglist = {
            'hello': self.handle_msg_hello,
            'newlm': self.handle_msg_newlm,
            'newpeer': self.handle_msg_newpeer,
            'needpeer': self.handle_msg_needpeer,
            'recon': self.handle_msg_recon,
            'maekawa': self.handle_msg_maekawa,
        }
        with self.lock:
            msg = json.loads(msg)
            src = msg.get('src', None) or addr
            src = src[0], src[1]
            if not msg.get('src', None):
                msg['src'] = src
            else:
                self.possibles += src
            self.possibles += addr
            if msg['stamp'] in self.seen:
                return
            self.seen += msg['stamp']
            addr = tuple(msg['src'])
            def reply(data):
                msg = self.mkmsg()
                msg['type'] = 'oncedata'
                msg['data'] = data
                self.sendmsg(msg, src)
            fun = msglist.get(msg['type'], lambda a, b, c: True)
            fun(msg, addr, reply)

    def get_next_addr(self, addr):
        '''
        So the way we fill out the lace is:

            1 2 5
            3 4 7
            6 8 9

        which pattern is (1, 1), [we are 1-indexed, here] (2, 1),
        (1, 2), (2, 2), (3, 1), (1, 3), (3, 2), (2, 3), (3, 3), etc.

        The rule for generating this pattern is, for any tuple (x, y):

        (x, y) -> (x+1, 1) when x = y
        (x, y) -> (y, x) when x > y
        (x, y) -> (y, x+1) when x < y
        '''
        x = addr[0]
        y = addr[1]
        if x == y:
            return (x + 1, 1)
        elif x > y:
            return (y, x)
        elif x < y:
            return (y, x+1)
        else:
            # son, you've got issues
            return (0, 0)

    def ispeer(self, value):
        if value[0] == self.value[0] or value[1] == self.value[1]:
            return True
        return False

    def acquire(self, acqcb=None):
        self.acqcb = acqcb
        msg = self.mkmsg('maekawa')
        msg['maekawa'] = 'request'
        self.broadcast(msg)

    def release(self):
        with self.lock:
            if not self.mutexed:
                return
            self.mutexed = False
            msg = self.mkmsg('maekawa')
            msg['maekawa'] = 'release'
            self.broadcast(msg)

    class mutob(object):
        def __init__(self, bc):
            self.bc = bc
            self.ev = threading.Event()

        def __enter__(self):
            self.bc.acquire(self.ev.set)
            a = self.ev.wait(1)
            if not a:
                print "fail on", self.bc.udp.sock.getsockname()[1]
                raise RuntimeError

        def __exit__(self, type, value, traceback):
            self.bc.release()

    def mutex(self):
        return self.mutob(self)

    def handle_msg_maekawa(self, msg, addr, reply):
        '''
        Implements Maekawa's distributed mutex.
        '''
        if not addr in self.peers:
            # who the hell is this?
            print "bad msg", msg['id'][1]
            return
        if not msg.has_key('maekawa'):
            print "what the fuck"
            print msg
            return
        nmsg = self.mkmsg('maekawa')
        if msg['maekawa'] == 'request':
            if not self.grant:
                nmsg['maekawa'] = 'grant'
                self.grant = addr
            else:
                self.maeq.append(addr)
                if self.peers[self.grant][0] > msg['id'][0]:
                    # current grant has greater "priority"
                    nmsg['maekawa'] = 'fail'
                else:
                    nmsg['maekawa'] = 'inquire'
        elif msg['maekawa'] == 'grant':
            self.grants.append(addr)
            if len(self.grants) == len(self.peers):
                self.mutexed = True
                self.fails = []
                self.yields = []
                self.grants = []
                if self.acqcb:
                    self.acqcb()
                self.acqcb = None
            return
        elif msg['maekawa'] == 'inquire':
            if len(self.fails) > 0 or len(self.yields) > 0:
                nmsg['maekawa'] = 'yield'
            else:
                return # no answer?
        elif msg['maekawa'] == 'yield':
            self.maeq.append(addr)
            addr = self.maeq.pop()
        elif msg['maekawa'] == 'release':
            if addr != self.grant:
                print "what"
                return
            self.grant = None
            if addr in self.maeq:
                self.maeq.remove(addr)
            if self.maeq:
                addr = self.maeq.pop()
                self.grant = addr
                nmsg['maekawa'] = 'grant'
            else:
                return
        elif msg['maekawa'] == 'fail':
            self.fails.append(addr)
            return
        self.sendmsg(nmsg, addr)

    def handle_msg_newlm(self, msg, addr, reply):
        '''
        Handle 'newlm' message, bumping the lace_max
        '''
        self.broadcast(msg)
        self.lace_max = tuple(msg['newlm'])

    def handle_msg_recon(self, msg, addr, reply):
        '''
	    The 'reconnect' message; just start all over and grab new
    	state.
        '''
        self.value = self.lace_max = (0, 0)
        self.stamp = None
        self.bootstrap([addr])

    def handle_msg_needpeer(self, msg, addr, reply):
        '''
	    Handle the 'needpeer' message.  addr is seeking peers.
        '''
        self.broadcast(msg)
        if not self.ispeer(msg['id'][1]):
            # not a concern of ours
            return
        if tuple(msg['id'][1]) == self.value:
            # someone got handed our id
            nmsg = self.mkmsg()
            nmsg['type'] = 'recon'
            self.sendmsg(nmsg, addr)
            return
        self.peers[addr] = tuple(msg['id'])
        nmsg = self.mkmsg()
        nmsg['type'] = 'newpeer'
        nmsg['newlm'] = self.lace_max
        self.sendmsg(nmsg, addr)

    def handle_msg_newpeer(self, msg, addr, reply):
        '''
        addr is introducing itself as a new peer
        '''
        if not self.ispeer(msg['id'][1]):
            # something's broke
            return
        self.peers[addr] = tuple(msg['id'])
        self.lace_max = tuple(msg['newlm'])
        # clean house
        rem = []
        for a in self.peers:
            if not self.ispeer(self.peers[a][1]):
                rem.append(a)
        for r in rem:
            del self.peers[r]

    def handle_msg_hello(self, msg, addr, reply):
        '''
	    Handle the 'hello' message.  Either we are already in the
    	lace, in which case we are being greeted by a new member,
	    or we are the new member getting a greeting in reply.
        '''
        if self.value == (0, 0):
            # we are new
            self.value = tuple(msg['value'])
            if self.value == (0, 0):
                # dafuq
                return
            self.clock = self.loadstamp(msg['itc'])
            # add whoever we're talking to as a temporary peer
            self.peers[addr] = tuple(msg['id'])
            nmsg = self.mkmsg()
            nmsg['type'] = 'needpeer'
            self.broadcast(nmsg)
        else:
            # we are being greeted
            nmsg = self.mkmsg()
            nmsg['type'] = 'hello'
            nlm = self.get_next_addr(self.lace_max)
            nmsg['value'] = nlm
            a, b = self.clock.fork()
            self.clock = a
            nmsg['itc'] = self.dumpstamp(b)
            self.sendmsg(nmsg, addr)
            # bump lace_max site-wide
            # XXX this is broken right now
            self.lace_max = nlm
            nmsg = self.mkmsg()
            nmsg['type'] = 'newlm'
            nmsg['newlm'] = self.lace_max
            self.broadcast(nmsg)            

    def broadcast(self, msg):
        with self.lock:
            for peer in self.peers:
                self.sendmsg(msg, peer)

    def sendmsg(self, msg, addr):
        self.seen += msg['stamp']
        msg = json.dumps(msg)
        self.udp.send(msg, addr)
