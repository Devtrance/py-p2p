class Event(object):
    def __init__(self):
        self.hooks = []

    def __iadd__(self, new):
        self.hooks.append(new)
        return self

    def __isub__(self, gone):
        self.hooks.remove(gone)
        return self

    def fire(self, *args, **kwargs):
        for hook in self.hooks:
            hook(*args, **kwargs)

    def clear(self):
        self.hooks = []
