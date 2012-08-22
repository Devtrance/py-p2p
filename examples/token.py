#!/usr/bin/env python

import time
import code
import sys
sys.path.insert(0, "..")
from p2p import Broadcaster # the broadcaster overlay

token = None

# in order to connect to the p2p network, we first need the address
# of a server that's already in that network.  it's possible to
# start a new network and then join a network later, but for now we'll
# pretend that a network is already running.  to start a new network,
# just pass nothing or an empty tuple, (), to Broadcaster
if len(sys.argv) > 1:
    bootstrap_server = (sys.argv[1], 6966)
else:
    bootstrap_server = ()
b = Broadcaster(bootstrap_server)
b.start()

# now we define some reactors.  each reactor will be called every
# time we receive a data packet.  since we're just passing a token
# we don't need to create to much.  we need to be able to say, (a)
# hi, I'm new here, what's the current token, and (b) everybody update
# your tokens to X.

# first we'll react to (b)
def react_to_update(data, **kwargs): # need **kwargs to hide keyword arguments we don't care about
    # data was encoded as a json string on the wire; this is done
    # automatically by p2p
    if not data.get("update", None):
        return # not something we want to react to
    t = data.get("token", None)
    if t:
        global token
        token = t

# next, we'll react to someone requesting (a).  since getting the
# current token is not something you want to bother the whole network
# about, a new peer will just ask one of its immediate neighbors for
# a copy of their state.  in order to respond to such a request, the
# neighbor has to be able to send a unicast message back to the source.
# we do that in p2p by passing a `reply` object to the reactor.
def react_to_newguy(data, reply=None, **kwargs):
    if not data.get("hi", None):
        return
    if not reply: # didn't get a reply object; can't do much about that
        return
    d = {} # remember, p2p will jsonize this for us
    d['update'] = True
    d['token'] = token
    reply(d) # send the token back to the requester

# register the reactors
b.event += react_to_update
b.event += react_to_newguy

# give everyone a second to find some peers, if they haven't yet
if len(b.peers) == 0:
    time.sleep(1)

# now ask for the current token, since we don't have a token at all
d = dict(hi=True)
b.send_one(d) # use send_one() to bug a neighbor

token # new value!

# ...time passes...

# update the token and send it to everyone
#token = 4
#d = dict(update=True, token=token)
#b.send(d) # send() sends it to EVERYBODY

code.interact(local=locals())
