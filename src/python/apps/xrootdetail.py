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
                    orga.append(orge)
                    continue
                args['or'] = tuple(orga)
                del args['o']
                del args['r']
                del args['g']
                pass
            pass
        result['args'] = args
        continue
    return result

def _decode_mapping(stod, code, pseq, data):
    dictid = struct.unpack('>I', data[0:4])[0]
    info = _parse_monmapinfo(data[4:].decode('us-ascii'))
    status = 'server-id' if code == '=' else \
        'user-path-id' if code == 'd' else \
        'user-info-id' if code == 'i' else \
        'log-auth-id' if code == 'u' else \
        'user-info-id' if code == 'p' else \
        'xfer-id' if code == 'x' else None
    assert status is not None
    return {
        'stod': stod,
        'seq': pseq,
        'status': status,
        'data': {
            'info': info,
            'dictid': dictid,
        },
    }

def _decode_packet(data):
    ## Valid messages have an 8-byte header.
    if len(data) < 8:
        return {
            'stod': stod,
            'seq': pseq,
            'status': 'too-short',
            'data': {
                'unparsed': data,
            },
        }

    ## Decode the header.
    code = data[0:1].decode('ascii')
    # if code not in '=dfgiprtux':
    #     return
    pseq = int(data[1])
    plen = struct.unpack('>H', data[2:4])[0]
    stod = struct.unpack('>I', data[4:8])[0]

    if len(data) != plen:
        return {
            'stod': stod,
            'seq': pseq,
            'status': 'len-mismatch',
            'data': {
                'expected': plen,
                'unparsed': data[8:],
                'code': code,
            },
        }

    if code in '=dipux':
        return _decode_mapping(stod, code, pseq, data[8:])

    return {
        'stod': stod,
        'seq': pseq,
        'status': 'unrecognized',
        'data': {
            'unparsed': data[8:],
            'code': code,
        },
    }


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
        self.pmax = 32
        self.psz = 256
        self.cache = [ None ] * self.psz

        self.expected = None

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

    def seq_clear(self, now, stop_if_early=False, cur=None):
        logging.debug('peer=%s:%d ev=clear pseq=%d plim=%d)' %
                      (self.addr + (self.pseq, self.plim)))
        assert self.offset(self.plim) <= self.pmax

        ## Clear out expired stuff.
        try:
            while self.pseq != self.plim and self.pseq != cur:
                advance = False
                ce = self.cache[self.pseq]
                assert ce is not None
                # if ce is None:
                #     ## This shouldn't really happen.
                #     assert False
                #     # logging.debug('peer=%s:%d stop at missing %d' %
                #     #               (self.addr + (self.pseq,)))
                #     return

                try:
                    if type(ce) is tuple:
                        code, data, ts, exp = ce
                        if stop_if_early and exp > now:
                            return
                        advance = True
                        self.decode(ts, self.pseq, code, data)
                    else:
                        if ce > now:
                            # logging.debug('peer=%s:%d stop at expected %d' %
                            #               (self.addr + (self.pseq,)))
                            return
                        advance = True
                        pass
                finally:
                    if advance:
                        self.cache[self.pseq] = None
                        self.pseq = self.advance(self.pseq, 1)
                        pass
                    pass
                continue
        finally:
            # logging.debug('peer=%s:%d pseq=%d plim=%d (post-clear%s)' %
            #               (self.addr + (self.pseq, self.plim,
            #                             ' SIE' if stop_if_early else '')))
            pass
        pass

    ## Accept a packet for decoding.  If the sequence number is not
    ## the one expected, cache it in anticipation of earlier ones
    ## arriving out of order.
    def record(self, now, pseq, status, parsed):
        print('\n%d: %s' % (pseq, status))
        pprint(parsed)
        return

    ## Decode the tail of a message.  Messages should be resequenced
    ## before being delivered to this method.  'ts' is the timestamp
    ## of the message.  'pseq' is its sequence number, which should
    ## only be significant for logging.  'code' is the message type (a
    ## single character).  'data' is the remainder of the message; the
    ## first 8 bytes have been decoded.
    def decode(self, ts, pseq, code, data):
        quit = False
        if self.expected is not None and self.expected != pseq:
            logging.warning('peer=%s:%d ev=bad-seq nseq=%d exp=%d' %
                            (self.addr + (pseq, self.expected)))
            quit = True
            pass
        self.expected = self.advance(pseq, 1)
        logging.info('peer=%s:%d ev=dec nseq=%d c="%s" bytes=%d' %
                     (self.addr + (pseq, code, len(data))))
        try:

            ## TODO

            # logging.error('peer=%s:%d seq=%d mt=%s ev=unk-code' %
            #               (self.addr + (pseq, code)))
            return
        finally:
            # if quit:
            #     logging.debug('exiting')
            #     sys.exit(1)
            #     pass
            pass

    def decode_mapping(self, ts, code, data):
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
                "expiry": ts + self.detailer.id_timeout,
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

            ## Parse the packet.
            stod, pseq, status, parsed = _decode_packet(data)

            ## Locate the peer record.  Replace with a new one if the
            ## start time has increased.
            peer = self.peers.get(addr)
            if peer is None or stod > peer.stod:
                logging.info('peer=%s:%d ev=new-entry' % addr)
                peer = Peer(self, stod, addr)
                self.peers[addr] = peer
                self.check_identity()
            elif stod < peer.stod:
                ## Ignore messages from old instances.
                return

            ## Submit the message to be incorporated into the peer
            ## record.
            # logging.info('peer=%s:%d seq=%d code mt=%s' % (addr + (pseq, code)))
            # logging.info('bytes: %s' % data)
            peer.record(now, pseq, status, parsed)
        finally:
            # if now - self.seq_ts > self.seq_timeout:
            #     logging.debug('clear out')
            #     for addr, peer in self.peers.items():
            #         peer.seq_clear(self.seq_ts)
            #         continue
            #     self.seq_ts = now
            #     pass

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
