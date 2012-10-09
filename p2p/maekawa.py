import uuid
import heapq

class MaekawaNode(object):
    def __init__(self, parent):
        self.parent = parent  # this is so goddamn backwards
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

    def acquire(self, acqcb=None):
        self.acqcb = acqcb
        msg = self.parent.mkmsg('maekawa')
        msg['maekawa'] = 'request'
        msg['seq'] = self.reqseq = uuid.uuid4().int
        self.parent.broadcast(msg)

    def release(self):
        if not self.mutexed:
            return
        self.mutexed = False
        msg = self.parent.mkmsg('maekawa')
        msg['maekawa'] = 'release'
        msg['seq'] = self.reqseq
        self.parent.broadcast(msg)

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
        f = " ".join(['recv', "%03d"%(msgid%997), ">>", "%03d"%(self.parent.uuid%997), msg['maekawa']])
        print f
        nmsg = self.parent.mkmsg('maekawa')
        ans = handler(msg, msgid, nmsg)
        if ans:
            f = " ".join(['send', "%03d"%(self.parent.uuid%997), ">>", "%03d"%(ans[1]%997), ans[0]['maekawa']])
            print f
            self.parent.sendmsg(ans[0], ans[1])

    def handle_msg_request(self, msg, msgid, nmsg):
        if not self.grant:
            nmsg['maekawa'] = 'grant'
            self.grant = msgid
            self.grantseq = msg['seq']
        else:
            heapq.heappush(self.maeq, (msgid, msg['seq']))
            if self.grant <= msgid:
                # current grant has greater "priority", lower is better
                nmsg['maekawa'] = 'fail'
                nmsg['seq'] = msg['seq']
            else:
                msgid = self.grant
                nmsg['maekawa'] = 'inquire'
                nmsg['seq'] = self.grantseq
        return nmsg, msgid

    def handle_msg_grant(self, msg, msgid, nmsg):
        self.grants.add(msgid)
        if msgid in self.fails:
            self.fails.remove(msgid)
        if msgid in self.yields:
            self.yields.remove(msgid)
        if len(self.grants) == len(self.parent.peers) + 1:
            self.mutexed = True
            self.fails = set()
            self.yields = set()
            self.grants = set()
            self.inquires = set()
            if self.acqcb:
                self.acqcb()
            self.acqcb = None

    def handle_msg_inquire(self, msg, msgid, nmsg):
        if msg['seq'] != self.reqseq:
            return
        if len(self.fails) > 0 or len(self.yields) > 0:
            nmsg['maekawa'] = 'yield'
            nmsg['seq'] = self.reqseq
            self.yields.add(msgid)
            self.grants.remove(msgid)
        else:
            self.inquires.add((msgid, self.grantseq))
            return # no answer?
        return nmsg, msgid

    def handle_msg_yield(self, msg, msgid, nmsg):
        if msg['seq'] != self.grantseq:
            return
        newmsgid, newseq = heapq.heappop(self.maeq)
        heapq.heappush(self.maeq, (msgid, msg['seq']))
        msgid = newmsgid
        nmsg['maekawa'] = 'grant'
        nmsg['seq'] = newseq
        self.grant = msgid
        self.grantseq = newseq
        return nmsg, msgid

    def handle_msg_release(self, msg, msgid, nmsg):
        if msgid != self.grant or msg['seq'] != self.grantseq:
            print "%03d I don't believe you"%(self.parent.uuid%997)
            return
        self.grant = None
        self.grantseq = None
        if len(self.maeq) == 0:
            print "%03d noq"%(self.parent.uuid%997)
            return
        msgid, newseq = heapq.heappop(self.maeq)
        self.grant = msgid
        self.grantseq = newseq
        nmsg['maekawa'] = 'grant'
        nmsg['seq'] = newseq
        return nmsg, msgid

    def handle_msg_fail(self, msg, msgid, nmsg):
        if msg['seq'] != self.reqseq:
            return
        self.fails.add(msgid)
        for i, s in self.inquires:
            tmsg = self.parent.mkmsg('maekawa')
            tmsg['maekawa'] = 'yield'
            tmsg['seq'] = s
            f = " ".join(['send', "%03d"%(self.parent.uuid%997), ">>", "%03d"%(i%997), tmsg['maekawa']])
            print f
            self.parent.sendmsg(tmsg, i)
            self.yields.add(i)
            if i in self.grants:
                self.grants.remove(i)
        self.inquires = set()
