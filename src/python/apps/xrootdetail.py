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
import json
import signal
import os
from datetime import datetime
from frozendict import frozendict
from pprint import pprint
from getopt import gnu_getopt
from utils import merge
from reseq import FixedSizeResequencer as Resequencer

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

def _decode_mapping(code, buf):
    dictid = struct.unpack('>I', buf[0:4])[0]
    info = _parse_monmapinfo(buf[4:].decode('us-ascii'))
    status = 'server-id' if code == '=' else \
        'user-path-id' if code == 'd' else \
        'user-info-id' if code == 'i' else \
        'log-auth-id' if code == 'u' else \
        'file-purge-id' if code == 'p' else \
        'xfer-id' if code == 'x' else None
    assert status is not None
    return (status, { 'info': info, 'dictid': dictid })

def _decode_trace(buf):
    assert len(buf) == 16

    ## OPEN
    if buf[0] == 0x80:
        flen = struct.unpack('>Q', buf[0:8])[0] & 0xffffffffffffff
        dictid = struct.unpack('>I', buf[12:16])[0]
        return ('open', { 'len': flen, 'id': dictid })

    ## READV or READU
    if buf[0] == 0x90 or buf[0] == 0x91:
        rvid = buf[1]
        typ = 'readv' if buf[0] == 0x90 else 'readu'
        nseg = struct.unpack('>H', buf[2:4])[0]
        blen = struct.unpack('>i', buf[8:12])[0]
        dictid = struct.unpack('>I', buf[12:16])[0]
        return (typ, { 'nsegs': nseg, 'len': blen, 'id': dictid })

    ## APPID
    if buf[0] == 0xa0:
        name = buf[4:16].decode('us-ascii')
        return ('appid', { 'name': name })

    ## CLOSE
    if buf[0] == 0xc0:
        rtotsh = buf[1]
        wtotsh = buf[2]
        rtot = struct.unpack('>I', buf[4:8])[0] << rtotsh
        wtot = struct.unpack('>I', buf[8:12])[0] << wtotsh
        dictid = struct.unpack('>I', buf[12:16])[0]
        return ('close', { 'wtot': wtot, 'rtot': rtot, 'id': dictid })

    ## DISCONNECT
    if buf[0] == 0xd0:
        forced = (buf[1] & 0x01) != 0
        boundp = (buf[1] & 0x02) != 0
        dur = struct.unpack('>i', buf[8:12])[0]
        dictid = struct.unpack('>I', buf[12:16])[0]
        return ('disconnect', { 'dur': dur, 'id': dictid })

    ## WINDOW
    if buf[0] == 0xe0:
        srvid = struct.unpack('>Q', buf[0:8])[0] & 0xffffffffffff
        gapstart = struct.unpack('>I', buf[8:12])[0]
        gapend = struct.unpack('>I', buf[12:16])[0]
        return ('window', { 'g0': gapstart, 'g1': gapend })

    ## READ/WRITE REQUEST
    if buf[0] <= 0x7f:
        offset = struct.unpack('>Q', buf[0:8])[0]
        blen = struct.unpack('>i', buf[8:12])[0]
        if blen < 0:
            blen = -blen
            typ = 'write'
        else:
            typ = 'read'
        dictid = struct.unpack('>I', buf[12:16])[0]
        return (typ, { 'off': offset, 'len': blen, 'id': dictid })

    return ('unk', { 'dat': buf })

_recTval_isClose = 0
_recTval_isOpen = 1
_recTval_isTime = 2
_recTval_isXfr = 3
_recTval_isDisc = 4

def _decode_filehdr(buf):
    recType = buf[0]
    flags = buf[1]
    sz = struct.unpack('>H', buf[2:4])[0]

    ## isTime
    if recType == _recTval_isTime:
        recs = (struct.unpack('>h', buf[4:6])[0],
                struct.unpack('>h', buf[6:8])[0])
        return (recType, flags, sz, recs)

    dictid = struct.unpack('>I', buf[4:8])[0]
    return (recType, flags, sz, dictid)

def _decode_file_time(flags, extr, buf):
    beg = struct.unpack('>I', buf[0:4])[0]
    end = struct.unpack('>I', buf[4:8])[0]
    result = { 'beg': beg, 'end': end, 'nxfr': extr[0], 'nrec': extr[1] }
    if flags & 0x01:
        result['sid'] = struct.unpack('>Q', buf[8:16])[0]
        pass
    return ('time', result)

def _decode_file_disc(flags, dictid, buf):
    return ('disc', { 'user': dictid })

def _decode_file_lfn(buf):
    dictid = struct.unpack('>I', buf[0:4])[0]
    for i in range(4, len(buf)):
        if buf[i] == 0:
            break
        continue
    lfn = buf[4:i].decode('us-ascii')
    buf = buf[i:]
    return ((dictid, lfn), buf)

def _decode_file_open(flags, dictid, buf):
    fsz = struct.unpack('>Q', buf[0:8])[0]
    rw = (flags & 0x02) != 0
    result = { 'file': dictid, 'rw': rw }
    if flags & 0x01:
        result['ufn'], ignored = _decode_file_lfn(buf[8:])
        pass
    return ('open', result)

def _decode_file_ops(buf):
    nrd = struct.unpack('>I', buf[0:4])[0]
    nrdv = struct.unpack('>I', buf[4:8])[0]
    nwr = struct.unpack('>I', buf[8:12])[0]
    nseg_min = struct.unpack('>H', buf[12:14])[0]
    nseg_max = struct.unpack('>H', buf[14:16])[0]
    nseg = struct.unpack('>Q', buf[16:24])[0]
    rd_min = struct.unpack('>I', buf[24:28])[0]
    rd_max = struct.unpack('>I', buf[28:32])[0]
    rdv_min = struct.unpack('>I', buf[32:36])[0]
    rdv_max = struct.unpack('>I', buf[36:40])[0]
    wr_min = struct.unpack('>I', buf[40:44])[0]
    wr_max = struct.unpack('>I', buf[44:48])[0]
    return {
        'readv': {
            'calls': nrdv,
            'size_min': rdv_min,
            'size_max': rdv_max,
        },
        'segs': {
            'total': nseg,
            'segs_min': nseg_min,
            'segs_max': nseg_max,
        },
        'read': {
            'calls': nrd,
            'size_min': rd_min,
            'size_max': rd_max,
        },
        'write': {
            'calls': nwr,
            'size_min': wr_min,
            'size_max': wr_max,
        },
    }

def _decode_file_ssq(buf):
    rd_sq = struct.unpack('>Q', buf[0:8])[0]
    rdv_sq = struct.unpack('>Q', buf[8:16])[0]
    seg_sq = struct.unpack('>Q', buf[16:24])[0]
    wr_sq = struct.unpack('>Q', buf[24:32])[0]
    return {
        'read': { 'sqsum': rd_sq },
        'readv': { 'sqsum': rdv_sq },
        'segs': { 'sqsum': seg_sq },
        'write': { 'sqsum': wr_sq },
    }

def _decode_file_close(flags, dictid, buf):
    from utils import merge

    brd = struct.unpack('>Q', buf[0:8])[0]
    brdv = struct.unpack('>Q', buf[8:16])[0]
    bwr = struct.unpack('>Q', buf[16:24])[0]
    buf = buf[24:]
    result = {
        'file': dictid,
        'read': { 'bytes': brd },
        'readv': { 'bytes': brdv },
        'write': { 'bytes': bwr },
        'forced': (flags & 0x1) != 0,
    }

    if flags & 0x02:
        ops = _decode_file_ops(buf)
        merge(result, ops)
        buf = buf[48:]
        pass

    if flags & 0x04:
        ssq = _decode_file_ssq(buf)
        merge(result, ssq)
        pass

    return ('close', result)

def _decode_file_xfr(flags, dictid, buf):
    brd = struct.unpack('>Q', buf[0:8])[0]
    brdv = struct.unpack('>Q', buf[8:16])[0]
    bwr = struct.unpack('>Q', buf[16:24])[0]
    return ('xfr',
            { 'file': dictid, 'read': brd, 'readv': brdv, 'write': bwr })

def _decode_file_unk(typ, flags, extr, buf):
    return ('unk' + str(typ), { 'flags': flags, 'extr': extr, 'buf': buf })

def _decode_file(buf):
    result = list()
    while len(buf) >= 8:
        recType, flags, sz, extr = _decode_filehdr(buf[0:8])
        bdy = buf[8:sz]
        buf = buf[sz:]
        if recType == _recTval_isTime:
            result += (_decode_file_time(flags, extr, bdy),)
        elif recType == _recTval_isClose:
            result += (_decode_file_close(flags, extr, bdy),)
        elif recType == _recTval_isOpen:
            result += (_decode_file_open(flags, extr, bdy),)
        elif recType == _recTval_isDisc:
            result += (_decode_file_disc(flags, extr, bdy),)
        elif recType == _recTval_isXfr:
            result += (_decode_file_xfr(flags, extr, bdy),)
        else:
            result += (_decode_file_unk(recType, flags, extr, bdy),)
        continue
    return ('file', { 'entries': result, 'rem': buf })

def _decode_traces(buf):
    result = list()
    while len(buf) >= 16:
        hdr = buf[0:16]
        result += (_decode_trace(hdr),)
        buf = buf[16:]
        continue
    return ('trace', { 'info': result, 'tail': buf })

def _decode_packet(buf):
    ## Valid messages have an 8-byte header.
    if len(buf) < 8:
        return (stod, pseq, 'too-short', { 'unparsed': buf, })

    ## Decode the header.
    code = buf[0:1].decode('ascii')
    # if code not in '=dfgiprtux':
    #     return
    pseq = int(buf[1])
    plen = struct.unpack('>H', buf[2:4])[0]
    stod = struct.unpack('>I', buf[4:8])[0]

    if len(buf) != plen:
        return (stod, pseq, 'len-mismatch', {
            'expected': plen,
            'unparsed': buf[8:],
            'code': code,
        })

    if code in '=dipux':
        return (stod, pseq) + _decode_mapping(code, buf[8:])

    if code == 't':
        return (stod, pseq) + _decode_traces(buf[8:])

    if code == 'f':
        return (stod, pseq) + _decode_file(buf[8:])

    return (stod, pseq, 'unrecognized', {
        'unparsed': buf[8:],
        'code': code,
    })


class Peer:
    def __init__(self, detailer, stod, addr):
        self.detailer = detailer
        self.stod = stod
        self.addr = addr
        self.host = None
        self.inst = None
        self.pgm = None
        self.sids = { }

        ## Prepare to resequence Monitor Map messages.
        map_action = functools.partial(Peer.act_on_map, self)
        self.map_seq = Resequencer(256, 32, map_action,
                                   timeout=detailer.seq_timeout,
                                   logpfx='peer=%s:%d seq=map' % self.addr)

        ## Learn identities supplied by the server, as specified by
        ## monitor mapping messages.  Index is the dictid.  Values are
        ## a map with 'expiry' timestamp (which could be updated),
        ## 'code' (the type of the message that the mapping came from)
        ## and 'info' (data parsed from the mapping message).
        ## self.id_clear(ts) flushes out anything older than the
        ## specified timestamp.
        self.ids = { }
        pass

    def id_get(self, now, dictid):
        r = self.ids.get(dictid)
        if r is None:
            return None
        r['expiry'] = now + self.detailer.id_timeout
        return r['info']

    ## Flush out identities with old expiry times.
    def id_clear(self, now):
        for k in [ k for k, v in self.ids.items() if now > v["expiry"] ]:
            self.ids.pop(k, None)
            continue
        pass

    def log(self, lvl, msg, *args):
        msg = 'peer=%s:%d ' + msg
        args = self.addr + args
        if self.host is not None:
            msg = 'xrdid=%s@%s pgm=%s ' + msg
            args = (self.inst, self.host, self.pgm) + args
            pass
        logging.log(lvl, msg, *args)
        pass

    def critical(self, msg, *args):
        return self.log(logging.CRITICAL, msg, *args)

    def error(self, msg, *args):
        return self.log(logging.ERROR, msg, *args)

    def warning(self, msg, *args):
        return self.log(logging.WARNING, msg, *args)

    def info(self, msg, *args):
        return self.log(logging.INFO, msg, *args)

    def debug(self, msg, *args):
        return self.log(logging.DEBUG, msg, *args)

    ## We are asked if we know who our peer is yet.  If not, all
    ## peers' output will be suspended.
    def is_identified(self):
        return self.host is not None

    def set_identity(self, host, inst, pgm):
        self.host = host
        self.inst = inst
        self.pgm = pgm
        pass

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

    def schedule_record(self, ts, ev, data):
        return self.detailer.store_event(ts, self.inst, self.host, self.pgm,
                                         ev, data)

    def act_on_trace(self, ts, info):
        ## TODO
        pass

    def act_on_sid(self, sid, ts, pseq, status, data):
        logging.debug('peer=%s:%d seq=sid num=%d sid=%012x %s=%s' %
                     (self.addr + (pseq, sid, status, data)))

        if status == 'file':
            print('\nFile entries (%d):' % len(data['entries']))
            typ, hdr = data['entries'][0]
            assert typ == 'time'
            t0 = hdr['beg']
            t1 = hdr['end']
            td = t1 - t0
            nent = hdr['nrec']

            scheduled = 0
            too_old = 0
            for pos, (typ, ent) in enumerate(data['entries'][1:]):
                ts = t0 + td * (pos / nent)
                if typ == 'disc':
                    usr = self.id_get(ts, ent['user'])
                    print('%d disconnect: %s' % (pos, usr))
                    msg = {
                        'ev': 'disconnnect',
                    }
                    if usr is not None:
                        merge(msg, {
                            'prot': usr['prot'],
                            'user': usr['user'],
                            'client_name': usr['host'],
                            'client_addr': usr['args']['h'],
                            'ipv': usr['args']['I'],
                            'dn': usr['args']['m'],
                            'auth': usr['args']['p'],
                        })
                        pass
                    self.detailer.add_domain(msg, 'client_name', 'client_domain')
                    scheduled += 1
                    if self.schedule_record(ts, 'disconnect', msg):
                        too_old += 1
                        pass
                    pass
                elif typ == 'open':
                    fil = self.id_get(ts, ent['file'])
                    rw = ent['rw']
                    ufn_id, ufn_p = ent.get('ufn')
                    ufn = self.id_get(ts, ufn_id)
                    print('%d open: rw=%s file=%s ufn=%s p=%s' %
                          (pos, rw, fil, ufn, ufn_p))
                    msg = {
                        'ev': 'open',
                        'rw': rw,
                        'path': ufn_p,
                    }
                    if ufn is not None:
                        merge(msg, {
                            'prot': ufn['prot'],
                            'user': ufn['user'],
                            'client_name': ufn['host'],
                            'client_addr': ufn['args']['h'],
                            'ipv': ufn['args']['I'],
                            'dn': ufn['args']['m'],
                            'auth': ufn['args']['p'],
                        })
                        pass
                    self.detailer.add_domain(msg, 'client_name', 'client_domain')
                    scheduled += 1
                    if self.schedule_record(ts, 'open', msg):
                        too_old += 1
                        pass
                    pass
                elif typ == 'close':
                    fil = self.id_get(ts, ent.pop('file'))
                    print('%d close: file=%s stats=%s' % (pos, fil, ent))
                    msg = {
                        'ev': 'close',
                        'read_bytes': ent['read']['bytes'],
                        'readv_bytes': ent['readv']['bytes'],
                        'write_bytes': ent['write']['bytes'],
                        'forced': ent['forced'],
                    }
                    if fil is not None:
                        merge(msg, {
                            'prot': fil['prot'],
                            'user': fil['user'],
                            'client_name': fil['host'],
                            'path': fil['path'],
                        })
                        pass
                    self.detailer.add_domain(msg, 'client_name', 'client_domain')
                    scheduled += 1
                    if self.schedule_record(ts, 'close', msg):
                        too_old += 1
                        pass
                    pass
                elif typ == 'xfr':
                    fil = self.id_get(ts, ent.pop('file'))
                    print('%d xfr: file=%s stats=%s' % (pos, fil, ent))
                    pass
                elif typ == 'time':
                    print('time: detail=%s' % ent)
                    pass
                continue
            if too_old > 0:
                self.warning('ev=old-events tried=%d missed=%d',
                             scheduled, too_old)
            pass
        ## TODO
        pass

    def act_on_map(self, ts, pseq, status, data):
        logging.debug('peer=%s:%d seq=map num=%d %s=%s' %
                      (self.addr + (pseq, status, data)))

        ## A server-id mapping has a zero dictid, and just describes
        ## the peer in more detail.
        if status == 'server-id':
            dictid = data['dictid']
            info = data['info']
            self.detailer.record_identity(info['host'],
                                          info['args']['inst'],
                                          info['args']['pgm'],
                                          self)
            assert dictid == 0
            return

        ## An xfer-id mapping has a zero dictid, so whatever it is,
        ## it's not actually defining a mapping.
        if status == 'xfer-id':
            dictid = data['dictid']
            assert dictid == 0
            ## TODO
            return

        ## A file-purge-id mapping has a zero dictid, so whatever it
        ## is, it's not actually defining a mapping.
        if status == 'file-purge-id':
            dictid = data['dictid']
            assert dictid == 0
            ## TODO
            return

        ## Trace messages are not mapping messages, but they appear to
        ## belong to the same sequence.
        if status == 'trace':
            info = data['info']
            self.act_on_trace(ts, info)
            return

        ## Although there are several types of mapping, they all seem
        ## to use the same dictionary space, so one dict is enough for
        ## all types.
        info = data['info']
        dictid = data['dictid']
        self.ids[dictid] = {
            "expiry": ts + self.detailer.id_timeout,
            "type": status,
            "info": info,
        }
        return

    ## Accept a decoded packet for processing.  This usually means
    ## working out what sequence it belongs to, and submitting it for
    ## resequencing.
    def process(self, now, pseq, status, data):
        ## All *-id and trace messages belong to the same sequence.
        if status[-3:] == '-id' or status == 'trace':
            #print('#%d %s: %s' % (pseq, status, data))
            self.map_seq.submit(now, pseq, status, data)
            return

        if status == 'file':
            sid = data['entries'][0][1]['sid']
            sid_data = self.sids.get(sid)
            if sid_data is None:
                sid_action = functools.partial(Peer.act_on_sid, self, sid)
                seq = Resequencer(256, 32, sid_action,
                                  timeout=detailer.seq_timeout,
                                  logpfx='peer=%s:%d seq=map sid=%012x' %
                                  (self.addr + (sid,)))
                sid_data = { 'seq': seq }
                self.sids[sid] = sid_data
                pass
            sid_data['seq'].submit(now, pseq, status, data)
            #print('#%d %s: file: sid=%012x %s' % (pseq, status, sid, data))
            return

        if 'code' in data:
            logging.warning('%s:%d ev=unh nseq=%d status=%s code=%s' %
                            (self.addr % (pseq, status, data['code'])))
            return

        logging.warning('%s:%d ev=ign nseq=%d status=%s' %
                        (self.addr % (pseq, status)))
        print('#%d %s: ignored' % (pseq, status))
        return

    pass

import domains

class Detailer:
    def __init__(self, logname, domfile):
        if domfile is None:
            self.domains = None
        else:
            self.domains = domains.WatchingDomainDeriver(domfile)
            pass

        ## We map from client host/port to Peer.
        self.peers = { }

        ## When we get an identity, we map it to the client host/port
        ## here.  If the old value is different, we purge the old
        ## value from self.peers.
        self.names = { }

        ## Maintain a sequence of parsed and restructured events.  The
        ## key is a timestamp (integer, milliseconds), and the value
        ## is a list of tuples (instance, host, program, event,
        ## params).
        self.events = { }

        ## How far back do we keep events?
        self.horizon = 70
        self.event_limit = time.time() - self.horizon

        ## Set the timeout for missing sequence numbers.  Remember
        ## when we last purged them.
        self.seq_timeout = 2
        self.seq_ts = time.time()

        ## Set the timeout for ids.  Remember when we last purged
        ## them.
        self.id_timeout = 5 * 60
        self.id_ts = time.time()

        self.output_enabled = True

        self.log_name = logname
        self.out = open(self.log_name, "a")
        pass

    def store_event(self, ts, inst, host, pgm, ev, params):
        ts = int(ts * 1000)
        if ts < self.event_limit:
            return True
        grp = self.events.setdefault(ts, [ ])
        grp.append((inst, host, pgm, ev, params))
        return False

    def release_events(self, ts):
        ts = int(ts * 1000)
        if ts < self.event_limit:
            return
        ks = [ k for k in self.events if k < ts ]
        ks.sort()
        for k in ks:
            for inst, host, pgm, ev, params in self.events.pop(k):
                self.log(k / 1000,
                         '%s@%s %s %s %s' % (inst, host, pgm, ev, params))
                continue
            continue
        self.event_limit = ts
        pass

    ## Re-open the fake log for appending, and replace our stream's FD
    ## with the new one.  This should be called on SIGHUP to allow log
    ## rotation.
    def relog(self):
        with open(self.log_name, "a") as nf:
            fd = nf.fileno()
            os.dup2(fd, self.out.fileno())
            pass
        pass

    def log(self, ts, msg):
        tst = datetime.utcfromtimestamp(ts).isoformat('T', 'milliseconds')
        self.out.write('%s %s\n' % (tst, msg))
        pass

    def add_domain(self, data, key_out, key_in):
        if self.domains is None:
            return
        host = data.get(key_out)
        if host is None:
            return
        dom = self.domains.derive(host)
        data[key_in] = dom
        return

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
        peer.set_identity(host, inst, pgm)
        old = self.peers.pop(old_addr, None)
        if old is not None:
            old.discard()
            pass
        pass

    def record(self, addr, buf):
        now = time.time()
        try:
            ## TODO: Check if addr[0] is in permitted set.

            ## Parse the packet.
            #stod, pseq, status, parsed
            stod, pseq, status, data = _decode_packet(buf)
            logging.debug('%s:%d(%d) %d %s' % (addr + (stod, pseq, status)))

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
            # logging.info('bytes: %s' % buf[:32].hex())
            peer.process(now, pseq, status, data)
        except Exception as e:
            logging.error('failed to parse %s' % buf)
            raise e
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
            self.release_events(now - self.horizon)
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
    fake_log = '/tmp/xrootd-detail.log'
    domain_conf = None
    log_params = {
        'format': '%(asctime)s %(message)s',
        'datefmt': '%Y-%d-%mT%H:%M:%S',
    }
    opts, args = gnu_getopt(sys.argv[1:], "zl:U:u:d:o:",
                            [ 'log=', 'log-file=' ])
    for opt, val in opts:
        if opt == '-U':
            udp_host = val
        elif opt == '-u':
            udp_port = int(val)
        elif opt == '-o':
            fake_log = val
        elif opt == '-d':
            domain_conf = val
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

    detailer = Detailer(logname=fake_log, domfile=domain_conf)
    def handler(signum, frame):
        logging.root.handlers = []
        logging.basicConfig(**log_params)
        logging.info('rotation')
        detailer.relog()
        pass
    signal.signal(signal.SIGHUP, handler)

    bindaddr = (udp_host, udp_port)
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
