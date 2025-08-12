## Copyright (c) 2022, Lancaster University
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions
## are met:
##
## 1. Redistributions of source code must retain the above copyright
##    notice, this list of conditions and the following disclaimer.
##
## 2. Redistributions in binary form must reproduce the above
##    copyright notice, this list of conditions and the following
##    disclaimer in the documentation and/or other materials provided
##    with the distribution.
##
## 3. Neither the name of the copyright holder nor the names of its
##    contributors may be used to endorse or promote products derived
##    from this software without specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
## FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
## COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
## INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
## (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
## SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
## HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
## STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
## OF THE POSSIBILITY OF SUCH DAMAGE.

from socketserver import DatagramRequestHandler
import time
import functools
import queue
import threading

class UDPQueuer:
    def __init__(self, dest=None):
        self._dest = dest
        if self._dest is not None:
            self._q = queue.Queue()
            self._hdlr = functools.partial(self.Handler, self)
            self._thrd = threading.Thread(target=self._serve_forever)
            pass
        pass

    def handler(self):
        return self._hdlr

    def start(self):
        if self._dest is None:
            return
        self._thrd.start()
        pass

    def halt(self):
        if self._dest is None:
            return
        self._q.put('end')
        self._q.join()
        pass

    def _serve_forever(self):
        while True:
            rec = self._q.get()
            try:
                if isinstance(rec, str):
                    break
                self._dest(rec['ts'], rec['peer'], rec['payload'])
            finally:
                self._q.task_done()
                pass
            continue
        pass

    def _push(self, ts, peer, payload):
        rec = {
            'peer': peer,
            'payload': payload,
            'ts': ts,
        }
        return self._q.put(rec)

    class Handler(DatagramRequestHandler):
        def __init__(self, rcvr, *args, **kwargs):
            self._rcvr = rcvr
            super().__init__(*args, **kwargs)
            pass

        def handle(self):
            rec = {
                'peer': self.client_address,
                'payload': self.request[0],
                'ts': time.time(),
            }
            self._rcvr._push(time.time(), self.client_address, self.request[0])
            pass

        pass

    pass
