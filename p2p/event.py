import thread
import traceback

class Event(object):
    def __init__(self):
        self.hooks = []

    def __iadd__(self, new):
        self.hooks.append(new)
        return self

    def __isub__(self, gone):
        self.hooks.remove(gone)
        return self

    def threadfire(self, *args, **kwargs):
        for hook in self.hooks:
            try:
                thread.start_new_thread(hook, args, kwargs)
            except Exception as e:
                print traceback.format_exc()
                continue

    def fire(self, *args, **kwargs):
        for hook in self.hooks:
            try:
                hook(*args, **kwargs)
            except Exception as e:
                print traceback.format_exc()
                continue

    def clear(self):
        self.hooks = []
