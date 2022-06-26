import asyncio
import json
import time
from subprocess import run

import pystache
from conclave.config import CodeGenConfig
from conclave.dispatch import Dispatcher
from conclave.net import SalmonPeer


class MotionDispatcher(Dispatcher):

    def __init__(self, peer: SalmonPeer, config: CodeGenConfig):

        print("MotionDispatcher peer=", peer)
        self.peer = peer   
        self.config = config
        self.loop = peer.loop
        self.to_wait_on = {}  # {peer: asyncio.Future(), ...} for peer in job.parties
        self.early = set()

    def _setup(self, job):
        """
        Wait until submit party is ready.
        """

        cmd = "{}/bash.sh".format(job.code_dir)

        print("{}: setup MOTION job. ".format(job.name))

        try:
            run(["/bin/bash", cmd, "build"], cwd=job.code_dir)
        except Exception as e:
            print(e)

    def _execute(self, job):
        """
        Execute MOTION job.
        """

        cmd = "{}/bash.sh".format(job.code_dir)

        print("{}: execute MOTION job. "
              .format(job.name))

        try:
            run(["/bin/bash", cmd], cwd=job.code_dir)
        except Exception as e:
            print(e)

        for other_pid in job.input_parties:
            if other_pid not in self.early and other_pid != self.peer.pid:
                self.to_wait_on[other_pid] = asyncio.Future()
            if other_pid != self.peer.pid:
                self.peer.send_done_msg(other_pid, job.name)
        future = self.to_wait_on.values()
        self.loop.run_until_complete(asyncio.gather(*future))

        time.sleep(3)

    def dispatch(self, job):

        print("Dispatching. pid=%s, job=" % self.peer.pid, job)

        # register self as current dispatcher with peer
        self.peer.register_dispatcher(self)

        #self.peer.dispatcher = None
        self.to_wait_on = {}
        self.early = set()
        self._setup(job)

        self._execute(job)

        self.peer.dispatcher = None
        self.to_wait_on = {}
        self.early = set()

    def receive_msg(self, msg):
        """ Receive message from other party in computation. """

        done_peer = msg.pid
        if done_peer in self.to_wait_on:
            self.to_wait_on[done_peer].set_result(True)
        else:
            self.early.add(done_peer)
            print("early message", msg)
