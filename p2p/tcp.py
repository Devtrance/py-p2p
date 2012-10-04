import socket
import errno
import thread
import struct

from event import Event

class TCP(object):
    def __init__(self, port):
        self.port = port
        self.handlers = Event()
        self.connected = Event()

    def start(self):
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.srv.bind(("", self.port))
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                # something is already there; just bind to anything for now
                self.srv.bind(("", 0))
            else:
                raise e
        self.port = self.srv.getsockname()[1]
        thread.start_new_thread(self.accept, ())

    def connect(self, addr):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(addr)
        self.connected.fire(conn, addr)
        thread.start_new_thread(self.handle_conn, (conn, addr))
        return conn

    def accept(self):
        self.srv.listen(5)
        while True:
            conn, addr = self.srv.accept()
            thread.start_new_thread(self.handle_conn, (conn, addr))

    def handle_conn(self, conn, addr):
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.connected.fire(conn, addr)
        buff = ""
        while True:
            msg, buff = self.read_msg(conn, buff)
            self.handlers.fire(msg, conn)

    def read_msg(self, conn, buff=""):
        while len(buff) < 4:
            buff += conn.recv(1024)
        msgsize = struct.unpack("!I", buff[0:4])[0]
        buff = buff[4:]
        while len(buff) < msgsize:
            buff += conn.recv(1024)
        msg = buff[:msgsize]
        buff = buff[msgsize:]
        return msg, buff

    def send(self, msg, conn):
	    # pepper me with exceptions, for when TCP falls on its
    	# stupid face
        msgsize = struct.pack("!I", len(msg))
        conn.send(msgsize)
        conn.send(msg)

    def shutdown(self):
        try:
            self.srv.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except: # nobody cares
            pass
