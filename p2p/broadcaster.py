import json
import hashlib
import random
import threading
import uuid
import time
import base64
import heapq

import udp
import tcp
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
        self.uuid = uuid.uuid4().int
        self.value = (0, 0)
        self.lace_max = (0, 0)
        self.udp = udp.UDP(port)
        self.tcp = tcp.TCP(port)
        self.seen = mrq.MRQ(2500)
        self.udp.handlers += self.handle_udp_msg
        self.tcp.handlers += self.handle_tcp_msg
        self.tcp.connected += self.new_tcp_conn
        self.tcpconns = {}
        self.lock = threading.RLock()
        self.plock = threading.RLock()
        self.timer = timer.Timer(heartbeat)
        self.handlers = event.Event()
        self.boot = bootstrap
        self.joincb = joincb
        self.clock = None
        # maekawa state variables
        self.testid = 0
        self.acqcb = None     # the callback invoked when the mutex is acquired
        self.grant = None     # the id of the peer we have given our "grant" toekn
        self.maeq = []        # a queue of requests
        self.mutexed = False  # whether we have the mutex
        self.fails = set()    # a set of peers who have rejected our request
        self.yields = set()   # a set of peers we have yielded our request to
        self.grants = set()   # the peers we have grant tokens from
        self.grantseq = None  # the sequence number for the peer we have given our grant token
        self.reqseq = None    # the sequence number for our grant request
        self.inquires = set() # a set of peers who have sent us 'inquire' messages, in case we decide to yield

    def base(self):
        self.value = self.lace_max = (1, 1)
        self.clock = itc.Stamp()

    def addpeer(self, pid, contact):
        pl = self.peers.get(pid, {'contact': None, 'value': (0, 0)})
        pl['contact'] = contact
        self.peers[pid] = pl

    def dumpstamp(self, stamp):
        return base64.b64encode(stamp.dump())

    def loadstamp(self, stampstr):
        return itc.Stamp.load(base64.b64decode(stampstr))

    def start(self):
        self.udp.start()
        self.tcp.start()
        if self.boot:
            self.bootstrap(self.boot)
        self.timer.start()

    def stop(self):
        with self.lock:
            for p in self.peers.values():
                self.bye(p)
            self.timer.disable()
            self.udp.shutdown()
            self.tcp.shutdown()

    def mkmsg(self, msgtype='noop'):
        msg = {}
        msg['type'] = msgtype
        msg['id'] = (self.uuid, self.value)
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
            self.sendmsg_raw(msg, p)

    def new_tcp_conn(self, conn, addr):
        self.tcpconns[addr] = conn

    def handle_udp_msg(self, msg, addr):
        self.handle_msg(msg, addr, 'udp')

    def handle_tcp_msg(self, msg, conn):
        self.handle_msg(msg, conn.getsockname(), 'tcp')

    def handle_msg(self, msg, addr, proto):
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
            'wanttcp': self.handle_msg_wanttcp,
            'havetcp': self.handle_msg_havetcp,
            'bumptid': self.handle_msg_bumptid,
        }
        with self.lock:
            msg = json.loads(msg)
            src = msg.get('src', None) or addr
            src = src[0], src[1]
            if not msg.get('src', None):
                msg['src'] = src
            if msg['stamp'] in self.seen:
                return
            self.seen += msg['stamp']
            addr = tuple(msg['src'])
            def reply(data):
                msg = self.mkmsg()
                msg['type'] = 'oncedata'
                msg['data'] = data
                if proto == 'udp':
                    self.sendmsg_raw_udp(msg, src)
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
        with self.lock:
            self.acqcb = acqcb
            self.seeking = True
            msg = self.mkmsg('maekawa')
            msg['maekawa'] = 'request'
            msg['seq'] = uuid.uuid4().int
            self.broadcast(msg)

    def release(self):
        with self.lock:
            if not self.mutexed:
                return
            self.mutexed = False
            msg = self.mkmsg('maekawa')
            msg['maekawa'] = 'release'
            msg['seq'] = self.reqseq
            self.broadcast(msg)

    class mutob(object):
        def __init__(self, bc):
            self.bc = bc
            self.ev = threading.Event()

        def __enter__(self):
            self.bc.acquire(self.ev.set)
            a = self.ev.wait(1)
            if not a:
                print "fail on", self.bc.uuid % 997
                print len(self.bc.fails), len(self.bc.yields), len(self.bc.grants), self.bc.mutexed
                raise RuntimeError

        def __exit__(self, type, value, traceback):
            self.bc.release()

    def mutex(self):
        return self.mutob(self)

    def bumptid(self):
        self.testid += 1
        m = self.mkmsg('bumptid')
        m['testid'] = self.testid
        self.broadcast(m)

    def handle_msg_wanttcp(self, msg, addr, reply):
        port = msg['port']
        ip = addr[0]
        conn = self.tcp.connect((ip, port))
        self.addpeer(msg['id'][0], conn)
        self.peers[msg['id'][0]]['value'] = msg['id'][1]
        nmsg = self.mkmsg('havetcp')
        nmsg['port'] = conn.getsockname()[1]
        self.sendmsg(nmsg, msg['id'][0])

    def handle_msg_havetcp(self, msg, addr, reply):
        self.addpeer(msg['id'][0], self.tcpconns[(addr[0], msg['port'])])
        self.peers[msg['id'][0]]['value'] = msg['id'][1]

    def handle_msg_bumptid(self, msg, addr, reply):
        self.broadcast(msg)
        self.testid = msg['testid']

    def handle_msg_maekawa(self, msg, addr, reply):
        '''
        Implements Maekawa's distributed mutex.
        '''
        msgid = msg['id'][0]
        if not msgid in self.peers and not msgid == self.uuid:
            # who the hell is this?
            return
        if not msg.has_key('maekawa'):
            return
        nmsg = self.mkmsg('maekawa')
        if msg['maekawa'] == 'request':
            if not self.grant:
                nmsg['maekawa'] = 'grant'
                self.grant = msgid
                self.grantseq = msg['seq']
            else:
                heapq.heappush(self.maeq, (msgid, msg['seq']))
                if self.grant <= msg['id'][0]:
                    # current grant has greater "priority", lower is better
                    nmsg['maekawa'] = 'fail'
                    nmsg['seq'] = msg['seq']
                else:
                    msgid = self.grant
                    nmsg['maekawa'] = 'inquire'
                    nmsg['seq'] = self.grantseq
        elif msg['maekawa'] == 'grant':
            self.grants.add(msgid)
            if msgid in self.fails:
                self.fails.remove(msgid)
            if msgid in self.yields:
                self.yields.remove(msgid)
            if len(self.grants) == len(self.peers) + 1:
                self.mutexed = True
                self.fails = set()
                self.yields = set()
                self.grants = set()
                self.inquires = set()
                self.seeking = False
                if self.acqcb:
                    self.acqcb()
                self.acqcb = None
            return
        elif msg['maekawa'] == 'inquire' and msg['seq'] == self.reqseq:
            if self.fails > 0 or self.yields > 0:
                nmsg['maekawa'] = 'yield'
                nmsg['seq'] = self.reqseq
                self.yields.add(msgid)
                self.grants.remove(msgid) # let's see if this constraint holds...
            else:
                self.inquires.add((msgid, msg['seq']))
                return # no answer?
        elif msg['maekawa'] == 'yield' and msg['seq'] == self.grantseq:
            newmsgid, newseq = heapq.heappop(self.maeq)
            heapq.heappush(self.maeq, (msgid, msg['seq']))
            msgid = newmsgid
            nmsg['maekawa'] = 'grant'
            nmsg['seq'] = newseq
            self.grant = msgid
            self.grantseq = newseq
        elif msg['maekawa'] == 'release':
            if msgid != self.grant:
                return
            self.grant = None
            self.grantseq = None
            if not self.maeq:
                return
            msgid, newseq = heapq.heappop(self.maeq)
            self.grant = msgid
            self.grantseq = newseq
            nmsg['maekawa'] = 'grant'
            nmsg['seq'] = newseq
        elif msg['maekawa'] == 'fail' and msg['seq'] == self.reqseq:
            self.fails.add(msgid)
            for i, s in self.inquires:
                tmsg = self.mkmsg('maekawa')
                tmsg['maekawa'] = 'yield'
                tmsg['seq'] = i
                self.sendmsg(tmsg, i)
                self.yields.add(i)
                self.grants.remove(i)
            self.inquires = []
            return
        else:
            # dead messages
            return
        self.sendmsg(nmsg, msgid)

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
        pid = msg['id'][0]
        self.addpeer(pid, addr)
        self.peers[msg['id'][0]]['value'] = msg['id'][1]
        nmsg = self.mkmsg()
        nmsg['type'] = 'newpeer'
        nmsg['newlm'] = self.lace_max
        self.sendmsg(nmsg, pid)

    def handle_msg_newpeer(self, msg, addr, reply):
        '''
        addr is introducing itself as a new peer
        '''
        if not self.ispeer(msg['id'][1]):
            # something's broke
            return
        self.addpeer(msg['id'][0], addr)
        self.peers[msg['id'][0]]['value'] = msg['id'][1]
        self.lace_max = tuple(msg['newlm'])
        # clean house
        rem = []
        for a in self.peers:
            if not self.ispeer(self.peers[a]['value']):
                rem.append(a)
        for r in rem:
            del self.peers[r]
        pid = msg['id'][0]
        nmsg = self.mkmsg('wanttcp')
        nmsg['port'] = self.tcp.port
        self.sendmsg(nmsg, pid)

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
            self.addpeer(msg['id'][0], addr)
            self.peers[msg['id'][0]]['value'] = msg['id'][1]
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
            self.sendmsg_raw(nmsg, addr)
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
            self.sendmsg(msg, self.uuid) # sigh

    def sendmsg(self, msg, peerid):
        if self.uuid == peerid:
            if msg['type'] == 'maekawa':
                msg['stamp'] = uuid.uuid4().int # newstamp
                self.handle_msg(json.dumps(msg), (0, 0), None)
            return
        self.seen += msg['stamp']
        msg = json.dumps(msg)
        ct = self.peers[peerid]['contact']
        if type(ct) == tuple:
            self.udp.send(msg, ct)
        else:
            self.tcp.send(msg, ct)

    def sendmsg_raw(self, msg, addr):
        self.seen += msg['stamp']
        msg = json.dumps(msg)
        self.udp.send(msg, addr)
