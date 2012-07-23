from threading import Timer as ThreadTimer

class Timer(object):
    def __init__(self, interval=1):
        self.interval = interval
        self.funcs = []
        self.t = None

    def __iadd__(self, new):
        if type(new) == type(()):
            if len(new) == 3:
                self.funcs.append(new)
            elif len(new) == 2:
                self.funcs.append((new[0], new[1], {}))
            elif len(new) == 1:
                self.funcs.append((new[0], (), {}))
        else:
            self.funcs.append((new, (), {}))
        return self

    def start(self):
        def tick():
            for f, a, k in self.funcs:
                f(*a, **k)
            self.t = ThreadTimer(self.interval, tick)
            self.t.daemon = True
            self.t.start()
        tick()

    def disable(self):
        if self.t:
            self.t.cancel()
