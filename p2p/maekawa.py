import uuid
import heapq
import threading

class MaekawaNode(object):
    def __init__(self, parent):
        self.lock = threading.RLock() # probably an unneeded mistake
        self.parent = parent  # this is so goddamn backwards
        self.acqcb = None     # the callback invoked when the mutex is acquired
        self.grant = None     # the id of the peer we have given our "grant" toekn
        self.maeq = []        # a queue of requests, in the form of (sequence number, node id)
        self.mutexed = False  # whether we have the mutex
        self.fails = set()    # a set of peers who have rejected our request
        self.grants = set()   # the peers we have grant tokens from
        self.grantseq = None  # the sequence number for the peer we have given our grant token
        self.reqseq = 0       # the sequence number for our grant request
        self.inquires = set() # a set of peers who have sent us 'inquire' messages, in case we decide to yield
        self.inquired = False # whether we have an outstanding 'inquire' message with our current grant
        self.grantset = set() # the set of peers we need to get grant tickets from
        self.started = False

    def acquire(self, acqcb=None):
        with self.lock:
            if self.started:
                return
            if self.mutexed:
                raise RuntimeError("bizzare truth values")
            self.acqcb = acqcb
            self.inquires = set()
            self.fails = set()
            self.grants = set()
            msg = self.parent.mkmsg('maekawa')
            msg['maekawa'] = 'request'
            msg['seq'] = self.reqseq
            with self.parent.lock:
                self.grantset = set(self.parent.peers.keys() + [self.parent.uuid])
                self.parent.broadcast(msg)

    def release(self):
        with self.lock:
            if self.mutexed == False:
                return
            self.mutexed = False
            self.acqcb = None
            self.reqseq += 1 # ARE YOU FUCKING KIDDING ME MOVING THIS HERE FIXED ALL THE BUGS WHAT THE SHITDICK
            msg = self.parent.mkmsg('maekawa')
            msg['maekawa'] = 'release'
            msg['seq'] = self.reqseq
            self.parent.broadcast(msg)
            self.started = False

    @staticmethod
    def should_yield(msgid, msgseq, newid, newseq):
        '''
        The question, specifically, is "should msgid, msgseq yield to newid, newseq?"

	    The rule is, lower sequence IDs get precidence, and when
    	seqids are tied, lower msgids win.
        '''
        if msgseq < newseq:
            return False
        if newseq < msgseq:
            return True
        if msgid < newid:
            return False
        if newid < msgid:
            return True
        return False

    def handle_msg(self, msg):
        if not msg['maekawa']:
            print "bad message"
            return
        try:
            handler = getattr(self, "handle_msg_%s"%msg['maekawa'])
        except AttributeError:
            print "no such handler"
            return
        msgid = msg['id'][0]
#        f = " ".join(['recv', "%03d"%(msgid%997), ">>", "%03d"%(self.parent.uuid%997), msg['maekawa']])
#        print f
        nmsg = self.parent.mkmsg('maekawa')
        with self.lock:
            ans = handler(msg, msgid, nmsg)
        if ans:
#            f = " ".join(['send', "%03d"%(self.parent.uuid%997), ">>", "%03d"%(ans[1]%997), ans[0]['maekawa']])
#            print f
            self.parent.sendmsg(ans[0], ans[1])

    def sendfail(self, nodeid, reqseq):
        fmsg = self.parent.mkmsg('maekawa')
        fmsg['maekawa'] = 'fail'
        fmsg['seq'] = reqseq
#        f = " ".join(['send', "%03d"%(self.parent.uuid%997), ">>", "%03d"%(nodeid%997), fmsg['maekawa']])
#        print f
        self.parent.sendmsg(fmsg, nodeid)

    def handle_msg_request(self, msg, msgid, nmsg):
        '''
        Received a 'request' message.

	    If self.grant is None (we have not granted anyone yet), set
    	self.grant and send 'grant' message.

	    If we have an outstanding grant from an ID with msgid that
    	is lower, numerically, than the requesting msgid, then queue
	    the request and send a 'fail' message back to the requesting
    	msgid.

	    If we have an outstanding grant and the requesting msgid
    	is lower than the grant id, send an 'inquire' message to
    	the node that has our grant.
        '''
        if self.grant is None:
            # we have nothing outstanding -- grant the request
            self.grant = msgid
            self.grantseq = msg['seq']
            nmsg['seq'] = msg['seq']
            nmsg['maekawa'] = 'grant'
            self.inquired = False
            return nmsg, msgid
        # enqueue the message, since we can't grant it now
        heapq.heappush(self.maeq, (msg['seq'], msgid))
        if not self.should_yield(self.grant, self.grantseq, msgid, msg['seq']):
            # the current grant has a higher priority; fail the request
            nmsg['seq'] = msg['seq']
            nmsg['maekawa'] = 'fail'
            return nmsg, msgid
        if not self.inquired:
            # send an inquire, since we haven't yet
            nmsg['maekawa'] = 'inquire'
            nmsg['seq'] = self.grantseq
            self.inquired = True
            return nmsg, self.grant
        # nothing to do, fall off the end

    def handle_msg_grant(self, msg, msgid, nmsg):
        if msg['seq'] != self.reqseq:
            return
        self.grants.add(msgid)
        if self.grants == self.grantset:
            self.mutexed = True
            if self.acqcb:
                self.acqcb()
                self.acqcb = None

    def handle_msg_inquire(self, msg, msgid, nmsg):
        '''
	    The 'inquire' message.  If we have any failures, we reply
    	with a 'yield'.  Otherwise just store the request in case
    	we get a failure at some other point.
        '''
        if self.mutexed or not msg['seq'] == self.reqseq:
            # this actually happens a lot
            return
        if len(self.fails) > 0:
            nmsg['maekawa'] = 'yield'
            nmsg['seq'] = self.reqseq
            return nmsg, msgid
        else:
            self.inquires.add(msgid)

    def handle_msg_fail(self, msg, msgid, nmsg):
        if not msg['seq'] == self.reqseq:
            return
        # send out yields for any cached inquires
        for i in self.inquires:
            t = self.parent.mkmsg('maekawa')
            t['maekawa'] = 'yield'
            t['seq'] = self.reqseq
            self.parent.sendmsg(t, i)
        self.inquires = set() # empty the set
        self.fails.add(msgid)

    def handle_msg_yield(self, msg, msgid, nmsg):
	    # the node holding our grant has yielded it, so give it to
	    # whoever's next on the queue
        if msg['seq'] != self.grantseq or msgid != self.grant:
            return
        heapq.heappush(self.maeq, (self.grantseq, self.grant))
        seq, mid = heapq.heappop(self.maeq) # we won't get the same request back, 'cause heap
        self.grant = mid
        self.grantseq = seq
        nmsg['seq'] = seq
        nmsg['maekawa'] = 'grant'
        self.inquired = False
        return nmsg, mid

    def handle_msg_release(self, msg, msgid, nmsg):
        # unlock and send a grant to the next guy, if any
        self.grant = None
        self.grantseq = None
        if len(self.maeq) > 0:
            seq, mid = heapq.heappop(self.maeq)
            self.grant = mid
            self.grantseq = seq
            nmsg['seq'] = seq
            nmsg['maekawa'] = 'grant'
            self.inquired = False
            # send a 'fail' message to every other node in the queue
            # this isn't in the paper, but I'm pretty sure that it's necessary for the following scenario:
            #   node A sends a request to node B, and B sends a grant to node A
            #   node C sends a request to B and C
            #   C gives a grant to itself, and node B compares the requests and sends an inquire to A
            #   A declines to yield and eventually gains the lock
            #   node B sends requests to itself and to C; B's request supersedes C's
            #   A releases its lock, and B immediately grants its own request
            #   C, meanwhile, sends itself an inquire
            #   B and C deadlock, because C doesn't know B failed C's request.
            for fseq, fid in self.maeq:
                self.sendfail(fid, fseq)
            return nmsg, mid

