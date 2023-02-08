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
from frozendict import frozendict
from pprint import pprint
from getopt import gnu_getopt

_userid_fmt = re.compile(r'^([^/]+)/([^.]+)\.([^:]+):([^@]+)@(.*)')
_uriarg_fmt = re.compile(r'&([^=]+)=([^&]*)')
_spaces = re.compile(r'\s+')

## Parse a monitor mapping.  The argument consists of lines of text.
## The first line has a fixed format.  For other lines, if a line
## begins with '&', it looks like URI-encoded form arguments.
## Otherwise, at most one line is a file path.
def _parse_monmapinfo(text):
    lines = text.splitlines()
    prot, user, pid, sid, host = _userid_fmt.match(lines[0]).groups()
    result = {
        'prot': prot,
        'user': user,
        'pid': pid,
        'sid': sid,
        'host': host,
    }
    for line in lines[1:]:
        if len(line) == 0 or line[0] != '&':
            result['path'] = line
            continue
        args = { }
        for it in _uriarg_fmt.finditer(line):
            name = it.group(1)
            value = it.group(2)
            ## TODO: Should probably URI-decode both name and value.
            args[name] = value
            continue
        if 'o' in args and 'r' in args and 'g' in args:
            oa = _spaces.split(args['o'])
            ra = _spaces.split(args['r'])
            ga = _spaces.split(args['g'])
            if len(oa) == len(ra) and len(oa) == len(ga):
                orga = []
                for i in range(0, len(oa)):
                    ov = None if oa[i] == 'NULL' else oa[i]
                    rv = None if ra[i] == 'NULL' else ra[i]
                    gv = None if ga[i] == 'NULL' else ga[i]
                    orge = {
                        'g': gv,
                        'r': rv,
                        'o': ov,
                    }
                    orga.append(frozendict(orge))
                    continue
                args['or'] = tuple(orga)
                del args['o']
                del args['r']
                del args['g']
                pass
            pass
        result['args'] = args
        continue
    return frozendict(result)



class Peer:
    def __init__(self, detailer, stod, addr):
        self.detailer = detailer
        self.stod = stod
        self.addr = addr
        self.host = None

        ## Keep a cache of unparsed messages, indexed by sequence
        ## number.  'pseq' is the next expected sequence number (8
        ## bits).  Each entry in 'cache' is a tuple(code, data, ts) if
        ## the message is present.  'code' is the message type, a
        ## single character.  'data' is the message excluding the
        ## header (which has already been parsed).  'ts' is the
        ## timestamp when the message arrived.  If the message is not
        ## present, the value is just a number (not a tuple), giving
        ## the expiry time.
        self.pseq = None
        self.plim = None
        self.pmax = 200
        self.psz = 256
        self.cache = [ None ] * self.psz

        ## Learn identities supplied by the server, as specified by
        ## monitor mapping messages.  Index is the dictid.  Values are
        ## a map with 'expiry' timestamp (which could be updated),
        ## 'code' (the type of the message that the mapping came from)
        ## and 'info' (data parsed from the mapping message).
        ## self.id_clear(ts) flushes out anything older than the
        ## specified timestamp.
        self.ids = { }
        pass

    def offset(self, idx):
        return (idx + self.psz - self.pseq) % self.psz

    def advance(self, base, ln):
        assert ln >= -self.psz
        return (base + ln + self.psz) % self.psz

    ## Flush out identities with old expiry times.
    def id_clear(self, now):
        for k in [ k for k, v in self.ids.items() if now > v["expiry"] ]:
            self.ids.pop(k, None)
            continue
        pass

    ## We are asked if we know who our peer is yet.  If not, all
    ## peers' output will be suspended.
    def is_identified(self):
        return self.host is not None

    ## We are told when everyone can output again.
    def continue_output(self):
        ## TODO
        pass

    ## We are told when we all have to stop, usually because a new
    ## peer has appeared, but has not yet identified itself.
    def suspend_output(self):
        ## TODO
        pass

    def discard():
        ## TODO: Maybe flush out any old data?
        pass

    def seq_clear(self, now):
        logging.debug('%s:%d pseq=%d plim=%d (pre-clear)' %
                      (self.addr + (self.pseq, self.plim)))
        assert self.offset(self.plim) <= self.pmax

        ## Clear out expired stuff.
        while self.pseq != self.plim:
            ce = self.cache[self.pseq]
            if ce is None:
                break
            elif type(ce) is tuple:
                code, data, ts = ce
                self.decode(ts, self.pseq, code, data)
            elif ce is not None:
                if ce > now:
                    break
                pass
            self.cache[self.pseq] = None
            self.pseq = self.advance(self.pseq, 1)
            continue
        pass

    ## Accept a packet for decoding.  If the sequence number is not
    ## the one expected, cache it in anticipation of earlier ones
    ## arriving out of order.
    def record(self, now, pseq, code, data):
        ## If we have to wait for other packets, at what point do we
        ## give up?
        expiry = now + self.detailer.seq_timeout

        if self.pseq is None:
            ## This is the very first packet.  Assume we've just
            ## missed a few before.
            self.plim = self.pseq = self.advance(pseq, -10)
            logging.debug('%s:%d #%d plim=pseq=%d (initial)' %
                          (self.addr + (pseq, self.pseq)))
            pass
        assert self.offset(self.plim) <= self.pmax

        ## If we're outside the acceptable range, give up on missing
        ## packets until the range engulfs the new packet's sequence
        ## number.
        while self.offset(pseq) > self.pmax - 1:
            ## Pop out the expected packet, and decode if available.
            ce = self.cache[self.pseq]
            if type(ce) is tuple:
                code, data, ts = ce
                self.decode(ts, self.pseq, code, data)
            elif ce is not None:
                logging.warning('%s:%d #%d (abandoned on rcpt of %s)' %
                                (self.addr + (self.pseq, pseq)))
                pass
            self.cache[self.pseq] = None

            ## Advance to the next entry.  Also advance our limit if
            ## we would overtake it.
            nv = self.advance(self.pseq, 1)
            if self.plim == self.pseq:
                self.plim = nv
                pass
            self.pseq = nv
            continue
        assert self.offset(self.plim) <= self.pmax
        assert self.offset(pseq) <= self.pmax
        logging.debug('%s:%d #%d pseq=%d plim=%d (cleared until window)' %
                      (self.addr + (pseq, self.pseq, self.plim)))

        ## Store the message for decoding.
        self.cache[pseq] = (code, data, now)
        assert self.offset(self.plim) < self.pmax
        logging.debug('%s:%d #%d pseq=%d plim=%d (inserted)' %
                      (self.addr + (pseq, self.pseq, self.plim)))

        ## Set expiries on missing entries just before this one.
        while self.offset(self.plim) < self.offset(pseq):
            self.cache[self.plim] = expiry
            self.plim = self.advance(self.plim, 1)
            continue

        ## Step over the one we've just added.
        if pseq == self.plim:
            self.plim = self.advance(self.plim, 1)
            pass
        assert self.offset(self.plim) <= self.pmax
        logging.debug('%s:%d #%d pseq=%d plim=%d (expiries)' %
                      (self.addr + (pseq, self.pseq, self.plim)))

        ## Decode all messages before any gaps that haven't expired.
        self.seq_clear(now)
        assert self.offset(self.plim) <= self.pmax

        logging.debug('%s:%d #%d pseq=%d plim=%d' %
                      (self.addr + (pseq, self.pseq, self.plim)))
        n = self.offset(self.plim)
        for i in range(0, n):
            sn = self.advance(self.pseq, i)
            ce = self.cache[sn]
            if type(ce) is tuple:
                msg = '%s (%d bytes)' % (code, len(data))
            elif ce is None:
                msg = 'None'
            else:
                msg = str(ce - now) + "s left"
                pass
            logging.debug('%s:%d [%d] = %s' % (self.addr + (sn, msg)))
            continue
        return

    ## Decode the tail of a message.  Messages should be resequenced
    ## before being delivered to this method.  'now' is the timestamp
    ## of the message.  'pseq' is its sequence number, which should
    ## only be significant for logging.  'code' is the message type (a
    ## single character).  'data' is the remainder of the message; the
    ## first 8 bytes have been decoded.
    def decode(self, now, pseq, code, data):
        logging.info('%s:%d #%d ev=decoding code=%s bytes=%d' %
                     (self.addr + (pseq, code, len(data))))
        if code in '=dipux':
            return self.decode_mapping(now, code, data)

        ## TODO

        # logging.error('%s:%d seq=%d mt=%s ev=unk-code' %
        #               (self.addr + (pseq, code)))
        return

    def decode_mapping(self, now, code, data):
        dictid = struct.unpack('>I', data[0:4])[0]
        info = _parse_monmapinfo(data[4:].decode('us-ascii'))
        if code == '=':
            return self.decode_identity(info['host'],
                                         info['args']['inst'],
                                         info['args']['pgm'])
        elif code in 'px':
            print('Unused mapping mt=%s %d=' % (code, dictid))
            pprint(info)
        else:
            self.ids[dictid] = {
                "expiry": now + self.detailer.id_timeout,
                "code": code,
                "info": info,
            }
            pass
        return

    def decode_identity(self, host, inst, pgm):
        if self.host is not None:
            ## TODO: Should really check that the details haven't
            ## changed.
            return

        ## Record our new details.
        self.host = host
        self.inst = inst
        self.pgm = pgm

        ## Make sure any old records with our id are discarded.
        self.detailer.record_identity(host, inst, pgm, self)

        ## Check to see if we can start reporting again.
        self.detailer.check_identity()
        pass

    pass





class Detailer:
    def __init__(self):
        ## We map from client host/port to Peer.
        self.peers = { }

        ## When we get an identity, we map it to the client host/port
        ## here.  If the old value is different, we purge the old
        ## value from self.peers.
        self.names = { }

        ## Set the timeout for missing sequence numbers.  Remember
        ## when we last purged them.
        self.seq_timeout = 2
        self.seq_ts = time.time()

        ## Set the timeout for ids.  Remember when we last purged
        ## them.
        self.id_timeout = 5 * 60
        self.id_ts = time.time()

        self.output_enabled = True
        pass

    def check_identity(self):
        ## Go through all peers.  If any are unidentified, halt all
        ## output.
        for addr, peer in self.peers.items():
            if not peer.is_identified():
                self.output_enabled = False
                return
            continue
        self.output_enabled = True
        for addr, peer in self.peers.items():
            peer.continue_output()
            continue
        return

    def record_identity(self, host, inst, pgm, peer):
        ## Check to see if anything has changed.
        key = (host, inst, pgm)
        old_addr = self.names.get(key)
        if old_addr is not None:
            old = self.peers.get(old_addr)
            if old is not None and old == peer:
                return

        ## Replace the old entry.
        self.names[key] = peer
        self.peers.pop(old_addr, None)
        if old is not None:
            old.discard()
            pass
        pass

    def record(self, addr, data):
        now = time.time()
        try:
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

            ## Locate the peer record.  Replace with a new one if the
            ## start time has increased.
            peer = self.peers.get(addr)
            if peer is None or stod > peer.stod:
                logging.info('%s:%d ev=new-entry' % addr)
                peer = Peer(self, stod, addr)
                self.peers[addr] = peer
                self.check_identity()
            elif stod < peer.stod:
                ## Ignore messages from old instances.
                return

            ## Valid messages have the specified length.
            if len(data) != plen:
                logging.info(('%s:%d seq=%d mt=%s ev=len-mismatch ' +
                              'hdr=%s exp=%x got=%x') %
                             (addr + (pseq, code, data[0:8].hex(),
                                      plen, len(data))))
                return

            ## Submit the message to be incorporated into the peer
            ## record.
            # logging.info('%s:%d seq=%d code mt=%s' % (addr + (pseq, code)))
            # logging.info('bytes: %s' % data)
            peer.record(now, pseq, code, data[8:])
        finally:
            if now - self.seq_ts > self.seq_timeout:
                for addr, peer in self.peers.items():
                    peer.seq_clear(self.seq_ts)
                    continue
                self.seq_ts = now
                pass

            if now - self.id_ts > self.id_timeout:
                for addr, peer in self.peers.items():
                    peer.id_clear(now)
                    continue
                self.id_ts = now
                pass
            pass
        return

    class Handler(socketserver.DatagramRequestHandler):
        def __init__(self, detailer, *args, **kwargs):
            self.detailer = detailer
            super().__init__(*args, **kwargs)
            pass

        def handle(self):
            self.detailer.record(self.client_address, self.request[0])
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
            server.max_packet_size = 64 * 1024
            logging.info('Started')
            server.serve_forever()
            pass
    except KeyboardInterrupt as e:
        pass
    logging.info('Stopping')
    pass
