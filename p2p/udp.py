import socket
import thread
import hashlib

from event import Event

class UDP(object):
    def __init__(self, port):
        self.port = port
        self.handlers = Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", port))
        thread.start_new_thread(self.recv, ())

    def recv(self):
        while True:
            data, addr = self.sock.recvfrom(65535) # max udp size
            if data[:20] != hashlib.sha1(data[20:]).digest():
                continue # bad packet, ignore it
            data = data[20:]
            self.handlers.fire(data, addr)

    def send(self, msg, dst):
        msg = hashlib.sha1(msg).digest() + msg
        self.sock.sendto(msg, dst)
