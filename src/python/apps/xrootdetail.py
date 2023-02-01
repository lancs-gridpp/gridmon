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
import re
import urllib
import time
import sys
from pprint import pprint
from getopt import gnu_getopt

_userid_fmt = re.compile(r'^([^/]+)/([^.]+)\.([^:]+):([^@]+)@(.*)')
_uriarg_fmt = re.compile(r'&([^=]+)=([^&]+)')

def _parse_monmapinfo(text):
    lines = text.splitlines()
    prot, user, pid, sid, host = _userid_fmt.match(lines[0]).groups()
    args = { }
    for it in _uriarg_fmt.finditer(lines[1]):
        name = it.group(1)
        value = it.group(2)
        args[name] = value
        continue
    return {
        'prot': prot,
        'user': user,
        'pid': pid,
        'sid': sid,
        'host': host,
        'args': args,
    }
    

class Detailer:
    def __init__(self):
        ## We map from client host/port to Peer.
        self.peers = { }

        ## When we get an identity, we map it to the client host/port
        ## here.  If the old value is different, we purge the old
        ## value from self.peers.
        self.names = { }

        self.timeout = 2
        pass

    class Peer:
        def __init__(self, outer, stod, addr):
            self.outer = outer
            self.stod = stod
            self.addr = addr
            self.pseq = None
            self.cache = { }
            self.expiries = { }
            self.last_time = time.time()
            pass

        def clear(now):
            ## TODO: Clear out expired stuff.
            pass

        def record(self, now, pseq, code, data):
            expiry = now + self.outer.timeout

            if self.pseq is None:
                self.pseq = pseq
            else:
                ## Flush out really old stuff.  The opposite half of
                ## the sequence number range should be cleared.
                for i in range(0, 128):
                    cand = (i + 64) % 256
                    if cand in self.cache:
                        code, data = self.cache[cand]
                        del self.cache[cand]
                        del self.expiries[cand]
                        if code is not None:
                            self.process(cand, code, data)
                            pass
                        if cand == self.pseq:
                            self.pseq += 1
                            pass
                        pass
                    continue
                pass

            ## Store the message for processing.
            self.cache[pseq] = (code, data)

            ## Set expiries.
            for i in range(0, (256 + pseq - self.pseq) % 256):
                cand = (self.pseq + i) % 256
                if cand not in self.expiries:
                    self.expiries[cand] = expiry
                    pass
                continue

            ## Process all messages before any gaps.
            while self.pseq in self.cache or self.self.pseq in self.expiries:
                if self.pseq in self.cache:
                    code, data = self.cache[self.pseq]
                    del self.cache[self.pseq]
                    self.process(self.pseq, code, data)
                else:
                    got = self.expiries[self.pseq]
                    if now < got:
                        break
                    pass
                del self.expiries[self.pseq]
                self.pseq += 1
                self.pseq %= 256
                continue
            return

        def process(self, pseq, code, data):
            logging.info('processing %s:%d@%d' % (self.addr + (pseq,)))
            if code in '=dipux':
                return self.process_mapping(code, data)
            ## TODO
            return

        def process_mapping(self, code, data):
            dictid = struct.unpack('>I', data[0:4])[0]
            info = _parse_monmapinfo(data[4:].decode('us-ascii'))
            print('Mapping "%s": %d=' % (code, dictid))
            pprint(info)
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

        ## Locate the peer record.  Replace with a new one if the
        ## start time has increased.
        peer = self.peers.get(addr)
        if peer is None or stod > peer.stod:
            peer = self.Peer(self, stod, addr)
            self.peers[addr] = peer
            pass

        ## Ignore messages from old instances.
        if stod < peer.stod:
            return

        ## Submit the message to be incorporated into the peer record.
        logging.info('From %s:%d' % addr)
        now = time.time()
        peer.record(now, pseq, code, data[8:])

        if now - self.last_time > self.expiry:
            for addr, peer in self.peers.items():
                peer.clear(now)
                continue
            pass
        self.last_time = now
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
    udp_host = ''
    udp_port = 9486
    silent = False
    log_params = {
        'format': '%(asctime)s %(message)s',
        'datefmt': '%Y-%d-%mT%H:%M:%S',
    }
    opts, args = gnu_getopt(sys.argv[1:], "zl:U:u:",
                            [ 'log=', 'log-file=' ])
    for opt, val in opts:
        if opt == '-U':
            udp_host = val
        elif opt == '-u':
            udp_port = int(val)
        elif opt == '-z':
            silent = True
        elif opt == '--log':
            log_params['level'] = getattr(logging, val.upper(), None)
            if not isinstance(log_params['level'], int):
                sys.stderr.write('bad log level [%s]\n' % val)
                sys.exit(1)
                pass
            pass
        elif opt == '--log-file':
            log_params['filename'] = val
            pass
        continue

    if silent:
        with open('/dev/null', 'w') as devnull:
            fd = devnull.fileno()
            os.dup2(fd, sys.stdout.fileno())
            os.dup2(fd, sys.stderr.fileno())
            pass
        pass

    logging.basicConfig(**log_params)

    bindaddr = (udp_host, udp_port)
    detailer = Detailer()
    try:
        with socketserver.UDPServer(bindaddr, detailer.handler()) as server:
            logging.info('Started')
            server.serve_forever()
            pass
    except KeyboardInterrupt as e:
        pass
    logging.info('Stopping')
    pass
