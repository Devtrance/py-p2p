import socket
import errno
import thread
import hashlib

from event import Event

class UDP(object):
    def __init__(self, port):
        self.port = port
        self.handlers = Event()

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
            if data[:20] != hashlib.sha1(data[20:]).digest():
                continue # bad packet, ignore it
            data = data[20:]
            thread.start_new_thread(self.handlers.fire, (data, addr))

    def send(self, msg, dst):
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
