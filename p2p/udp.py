import socket
import errno
import thread
import hashlib

# for splintering packets
import threading
import uuid
import struct
import math

from event import Event

class UDP(object):
    def __init__(self, port):
        self.port = port
        self.handlers = Event()
        self.splinters = {}
        self.lock = threading.RLock()

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", self.port))
        thread.start_new_thread(self.recv, ())

    def recv(self):
        while True:
            try:
                data, addr = self.sock.recvfrom(65535) # max udp size
            except socket.error as e:
                if e[0] == errno.EBADF:
                    return
                continue
            thread.start_new_thread(self.handle_packet, (data, addr))

    def handle_packet(self, data, addr):
        if data[:20] != hashlib.sha1(data[20:]).digest():
            return # bad packet, ignore it
        data = data[20:]
        if data.startswith("splinter"):
            try:
                self.reform(data[8:], addr)
                return
            except:
                return
        self.handlers.fire(data, addr)

    def reform(self, data, addr):
        '''
        If the message size is too big for a single UDP datagram, try
        chopping it up.

        A normal message is [20B hash][data], but when splintered, we'll be:
        [20B hash]"splinter"[16B splinter group id][2B splinter count][2B splinter id][data]

        Everytime we receive a splinter, add it to the splinter list,
        and if we can recover a whole packet then fire it off.

        By the time we get to this method, the hash has been verified
        and removed.
        '''
        uuid = data[0:16] # this kills the namespace
        with self.lock:
            hsh = self.splinters.get(uuid, dict(count=struct.unpack("!H", data[16:18])[0], extent={}))
            count = hsh['count']
            packetid = struct.unpack("!H", data[18:20])[0]
            hsh['extent'][packetid] = data[20:]
            if len(hsh['extent']) == count:
                # have a whole packet now
                packet = "".join([hsh['extent'][x] for x in xrange(count)])
                thread.start_new_thread(self.handlers.fire, (packet, addr))
                del self.splinters[uuid]
            else:
                self.splinters[uuid] = hsh                

    def splinter(self, msg, dst):
        total = int(math.ceil(len(msg) / 20000.))
        msguuid = uuid.uuid4().bytes
        for count in xrange(total):
            tmp = "splinter" + msguuid + struct.pack("!HH", total, count) + msg[20000*count:20000*(count+1)]
            self.send(tmp, dst)            

    def send(self, msg, dst):
        if len(msg) > 40000: # arbitrary
            self.splinter(msg, dst)
            return
        msg = hashlib.sha1(msg).digest() + msg
        try:
            self.sock.sendto(msg, dst)
        except socket.error as e:
            if e[0] == errno.EPIPE:
                self.shutdown()
                self.start()

    def shutdown(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except: # nobody cares
            pass
