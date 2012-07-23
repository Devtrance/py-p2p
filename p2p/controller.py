import json

import udp
import timer

class Controller(object):
    def __init__(self, initial_servers={}, port=6965):
        self.peers = initial_servers
        self.udp = udp.UDP(port)
        self.udp.handlers += self.heartbeat_recv
        self.timer = timer.Timer(5)
        self.timer += self.heartbeat
        self.timer.start()

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

    def heartbeat_recv(self, data, addr):
        msg = json.loads(data)
        if msg.get('heartbeat', False):
            self.peers[addr] = 0
