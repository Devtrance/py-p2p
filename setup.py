#!/usr/bin/env python

from distutils.core import setup
from p2p import __version__

setup(name='py-p2p',
      version=__version__,
      description="A light peer-to-peer framework.",
      author="Toby Burress",
      author_email="kurin@delete.org",
      packages=['p2p'])
