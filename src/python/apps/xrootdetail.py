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

import logging
import traceback
import socketserver
import functools
import struct

class Detailer:
    def __init__(self):
        ## We map from client host/port.
        self.peers = { }
        pass

    class Peer:
        def __init__(self, outer, stod):
            self.outer = outer
            self.stod = stod
            self.pseq = None
            self.cache = { }
            pass

        def record(self, code, pseq, data):
            if self.pseq is not None:
                if pseq - self.pseq < 0:
                    ## This is too old!
                    return
                if pseq in self.cache:
                    ## This is a duplicate!
                    return
                pass

            ## Store the message for processing.
            self.cache[pseq] = (code, data)

            ## Process all messages before any gaps.
            while self.pseq in self.cache:
                code, data = self.cache[self.pseq]
                self.process(code, data)
                self.pseq += 1
                self.pseq %= 256
                continue
            return

        def process(self, code, data):
            ## TODO
            return

        pass

    def record(self, addr, data):
        ## TODO: Check if addr[0] is in permitted set.

        ## Valid messages have an 8-byte header.
        if len(data) < 8:
            return

        ## Decode the header.
        code = data[0:1].decode('ascii')
        if code not in '=dfgiprtux':
            return
        pseq = int(data[1])
        plen = struct.unpack('>H', data[2:4])[0]
        stod = struct.unpack('>I', data[4:8])[0]

        ## Valid messages have the specified length.
        if len(data) != plen:
            logging.info('Length mismatch: %x:%x' % (plen, len(data)))
            return

        ## Locate the peer record.
        peer = self.peers.get(addr)
        if peer is None or stod > peer.stod:
            peer = self.Peer(self, stod)
            self.peers[addr] = peer
            pass

        ## Ignore out-of-date messages.
        if stod < peer.stod:
            return

        ## Submit the message to be incorporated into the peer record.
        logging.info('From %s:%d' % addr)
        peer.record(code, pseq, data[8:])
        return

    class Handler(socketserver.DatagramRequestHandler):
        def __init__(self, outer, *args, **kwargs):
            self.outer = outer
            super().__init__(*args, **kwargs)
            pass

        def handle(self):
            self.outer.record(self.client_address, self.request[0])
            pass
        pass

    def handler(self):
        return functools.partial(self.Handler, self)

    pass

if __name__ == '__main__':
    detailer = Detailer()
    bindaddr = ('', 9486)
    try:
        with socketserver.UDPServer(bindaddr, detailer.handler()) as server:
            logging.info('Started')
            server.serve_forever()
            pass
    except KeyboardInterrupt as e:
        pass
    logging.info('Stopping')
    pass
