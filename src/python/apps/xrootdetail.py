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
        self.info('ev=new-entry')
        self.last_id = None
        pass

    def id_get(self, now, dictid):
        r = self.ids.get(dictid)
        if r is None:
            return None
        r['expiry'] = now + self.detailer.id_timeout
        return r['info']

    ## Flush out identities with old expiry times.
    def id_clear(self, now):
        ks = [ k for k, v in self.ids.items() if now > v["expiry"] ]
        for k in ks:
            self.ids.pop(k, None)
            continue
        self.info('ev=purged-dict amount=%d rem=%d', len(ks), len(self.ids))
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

    def schedule_record(self, ts, ev, data, ctxt={}):
        if self.inst is None or self.host is None or self.pgm is None:
            return False
        return self.detailer.store_event(ts, self.inst, self.host, self.pgm,
                                         ev, data, ctxt)

    def act_on_trace(self, ts, info):
        ## TODO
        pass

    def act_on_sid(self, sid, ts, pseq, status, data):
        self.debug('ev=sid num=%d sid=%012x type=%s data=%s',
                   pseq, sid, status, data)

        if status == 'file':
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
                    # print('%d disconnect: %s' % (pos, usr))
                    msg = { }
                    if usr is None:
                        self.warning('dictid=%d field=user' +
                                     ' ev=unknown-dictid' +
                                     ' rec=file-disconnect', ent['user'])
                        pass
                    else:
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
                    if fil is None:
                        self.warning('dictid=%d field=file' +
                                     ' ev=unknown-dictid' +
                                     ' rec=file-open', ent['file'])
                        pass
                    rw = ent['rw']
                    msg = { 'rw': rw }
                    ufn_s = ent.get('ufn')
                    if ufn_s is None:
                        ufn = None
                        pass
                    else:
                        ufn_id, ufn_p = ufn_s
                        ufn = self.id_get(ts, ufn_id)
                        msg['path'] = ufn_p
                        pass
                    if ufn is None:
                        if ufn_s is not None:
                            self.warning('dictid=%d field=ufn' +
                                         ' ev=unknown-dictid' +
                                         ' rec=file-open', ufn_id)
                            pass
                        pass
                    else:
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
                    filid = ent['file']
                    fil = self.id_get(ts, filid)
                    msg = {
                        'read_bytes': ent['read']['bytes'],
                        'readv_bytes': ent['readv']['bytes'],
                        'write_bytes': ent['write']['bytes'],
                        'forced': ent['forced'],
                    }
                    if fil is None:
                        self.warning('dictid=%d field=file' +
                                     ' ev=unknown-dictid' +
                                     ' rec=file-close', filid)
                        pass
                    else:
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
                    # fil = self.id_get(ts, ent.pop('file'))
                    # print('%d xfr: file=%s stats=%s' % (pos, fil, ent))
                    pass
                elif typ == 'time':
                    # print('time: detail=%s' % ent)
                    pass
                continue
            if too_old > 0:
                self.warning('ev=old-events tried=%d missed=%d',
                             scheduled, too_old)
            pass
        ## TODO
        pass

    def act_on_map(self, ts, pseq, status, data):
        self.debug('ev=map num=%d type=%s data=%s', pseq, status, data)

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

        if self.last_id is not None:
            skip = dictid - self.last_id
            if skip > 1 and skip < 0x80000000:
                self.warning('ev=skip-dicts count=%d from=%d to=%d',
                             skip - 1,
                             (self.last_id + 1) % 0x100000000,
                             (dictid + 0xffffffff) % 0x100000000)
                pass
            pass
        self.last_id = dictid
        return

    ## Accept a decoded packet for processing.  This usually means
    ## working out what sequence it belongs to, and submitting it for
    ## resequencing.
    def process(self, now, pseq, status, data):
        ## All *-id and trace messages belong to the same sequence.
        if status[-3:] == '-id' or status == 'trace':
            self.debug('sn=%d type=%s data=%s', pseq, status, data)
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
            self.warning('ev=unh nseq=%d status=%s code=%s',
                         pseq, status, data['code'])
            return

        self.warning('ev=ign nseq=%d status=%s', pseq, status)
        return

    pass

## Increment a counter.  'data' is a dict with 'value', 'zero' and
## 'last' (empty on first use).  'inc' is amount to increase by.  't0'
## is the default reset time.  't1' is now.
def _inc_counter(t0, t1, data, inc):
    if 'value' not in data:
        data['value'] = 0
        data['zero'] = t0
        pass
    old = data['value']
    data['value'] += inc
    if data['value'] >= 0x8000000000000000:
        pdiff = 0x8000000000000000 - old
        wdiff = data['value'] - old
        tdiff = t1 - data['last']
        data['zero'] = data['last'] + tdiff * (pdiff / wdiff)
        data['value'] -= 0x8000000000000000
        pass
    data['last'] = t1
    pass

import domains
import logfmt

class Detailer:
    def __init__(self, logname, rmw, domfile):
        self.rmw = rmw
        if domfile is None:
            self.domains = None
        else:
            self.domains = domains.WatchingDomainDeriver(domfile)
            pass

        self.t0 = time.time()

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
        self.event_limit = self.t0 - self.horizon * 1000

        ## Set the timeout for missing sequence numbers.
        self.seq_timeout = 2

        ## Set the timeout for ids.  Remember when we last purged
        ## them.
        self.id_timeout = 30 * 60
        self.id_ts = self.t0

        ## Remote-write new data at this interval.
        self.write_interval = 60
        self.write_ts = self.t0

        ## This holds ccumulated statistics, indexed by pgm, host,
        ## inst, client domain, stat (read, readv, write), then
        ## 'value', 'zero' (the time of the last counter reset), and
        ## 'last' (the time of the last increment).
        self.stats = { }

        self.output_enabled = True

        self.log_name = logname
        self.out = open(self.log_name, "a")
        pass

    def store_event(self, ts, inst, host, pgm, ev, params, ctxt):
        ts = int(ts * 1000)
        if ts < self.event_limit:
            return True
        grp = self.events.setdefault(ts, [ ])
        grp.append((inst, host, pgm, ev, params, ctxt))
        return False

    def release_events(self, ts):
        ts = int(ts * 1000)
        if ts < self.event_limit:
            return
        ks = [ k for k in self.events if k < ts ]
        ks.sort()
        for k in ks:
            for inst, host, pgm, ev, params, ctxt in self.events.pop(k):
                t1 = k / 1000
                self.log(t1, '%s@%s %s %s %s' %
                         (inst, host, pgm, ev, logfmt.encode(params, ctxt)))
                stats = self.stats.setdefault(pgm, { }) \
                                  .setdefault(host, { }) \
                                  .setdefault(inst, { })
                if ev == 'disconnect' and \
                   'prot' in params and \
                   'client_domain' in params and \
                   'ipv' in params and \
                   'auth' in params:
                    substats = stats.setdefault(params['prot'], { }) \
                                    .setdefault(params['client_domain'], { }) \
                                    .setdefault('ip_version', { }) \
                                    .setdefault(params['ipv'], { }) \
                                    .setdefault('auth', { }) \
                                    .setdefault(params['auth'], { })
                    dis = substats.setdefault('disconnects', { })
                    _inc_counter(self.t0, t1, dis, 1)
                    pass
                elif ev == 'open' and \
                   'prot' in params and \
                   'client_domain' in params and \
                   'ipv' in params and \
                   'auth' in params and \
                   'rw' in params:
                    substats = stats.setdefault(params['prot'], { }) \
                                    .setdefault(params['client_domain'], { }) \
                                    .setdefault('ip_version', { }) \
                                    .setdefault(params['ipv'], { }) \
                                    .setdefault('auth', { }) \
                                    .setdefault(params['auth'], { })
                    ops = substats.setdefault('opens', { })
                    rwops = substats.setdefault('rw-opens', { })
                    _inc_counter(self.t0, t1, ops, 1)
                    _inc_counter(self.t0, t1, rwops, 1 if params['rw'] else 0)
                    pass
                elif ev == 'close' and \
                   'prot' in params and \
                   'client_domain' in params:
                    substats = stats.setdefault(params['prot'], { }) \
                                    .setdefault(params['client_domain'], { })
                    cr = substats.setdefault('read', { })
                    crv = substats.setdefault('readv', { })
                    cw = substats.setdefault('write', { })
                    cl = substats.setdefault('closes', { })
                    fcl = substats.setdefault('forced-closes', { })
                    _inc_counter(self.t0, t1, cr, params['read_bytes'])
                    _inc_counter(self.t0, t1, crv, params['readv_bytes'])
                    _inc_counter(self.t0, t1, cw, params['write_bytes'])
                    _inc_counter(self.t0, t1, cl, 1)
                    _inc_counter(self.t0, t1, fcl, 1 if params['forced'] else 0)
                    pass
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

            ## Locate the peer record.  Replace with a new one if the
            ## start time has increased.
            peer = self.peers.get(addr)
            if peer is None or stod > peer.stod:
                peer = Peer(self, stod, addr)
                self.peers[addr] = peer
                self.check_identity()
            elif stod < peer.stod:
                ## Ignore messages from old instances.
                return

            ## Submit the message to be incorporated into the peer
            ## record.
            peer.process(now, pseq, status, data)
        except Exception as e:
            logging.error('failed to parse %s' % buf)
            raise e
        finally:
            if now - self.id_ts > self.id_timeout:
                for addr, peer in self.peers.items():
                    peer.id_clear(now)
                    continue
                self.id_ts = now
                pass
            self.release_events(now - self.horizon)
            if now - self.write_ts > self.write_interval:
                now_key = self.event_limit / 1000
                data = { now_key: self.stats }
                # print('stats: %s' % self.stats)
                self.rmw.install(data)
                self.write_ts = now
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

schema = [
    {
        'base': 'xrootd_data_write',
        'type': 'counter',
        'unit': 'bytes',
        'help': 'bytes received per protocol, instance, domain',
        'select': lambda e: [ (pgm, h, i, pro, d) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              for pro in e[pgm][h][i]
                              for d in e[pgm][h][i][pro]
                              if 'write' in e[pgm][h][i][pro][d] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       [t[3]][t[4]]['write']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         [t[3]][t[4]]['write']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_read',
        'type': 'counter',
        'unit': 'bytes',
        'help': 'bytes sent per protocol, instance, domain',
        'select': lambda e: [ (pgm, h, i, pro, d) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              for pro in e[pgm][h][i]
                              for d in e[pgm][h][i][pro]
                              if 'read' in e[pgm][h][i][pro][d] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       [t[3]][t[4]]['read']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         [t[3]][t[4]]['read']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_readv',
        'type': 'counter',
        'unit': 'bytes',
        'help': 'bytes sent per protocol, instance, domain',
        'select': lambda e: [ (pgm, h, i, pro, d) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              for pro in e[pgm][h][i]
                              for d in e[pgm][h][i][pro]
                              if 'readv' in e[pgm][h][i][pro][d] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       [t[3]][t[4]]['readv']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         [t[3]][t[4]]['readv']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_closes',
        'type': 'counter',
        'help': 'number of closes',
        'select': lambda e: [ (pgm, h, i, pro, d) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              for pro in e[pgm][h][i]
                              for d in e[pgm][h][i][pro]
                              if 'closes' in e[pgm][h][i][pro][d] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       [t[3]][t[4]]['closes']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         [t[3]][t[4]]['closes']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_closes_forced',
        'type': 'counter',
        'help': 'number of forced closes',
        'select': lambda e: [
            (pgm, h, i, pro, d) for pgm in e
            for h in e[pgm]
            for i in e[pgm][h]
            for pro in e[pgm][h][i]
            for d in e[pgm][h][i][pro]
            if 'forced-closes' in e[pgm][h][i][pro][d] and \
            'value' in e[pgm][h][i][pro][d]['forced-closes']
        ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       [t[3]][t[4]]['forced-closes']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         [t[3]][t[4]]['forced-closes']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_disconnects',
        'type': 'counter',
        'help': 'number of disconnnects',
        'select': lambda e: [
            (pgm, h, i, pro, d, ipv, aut) for pgm in e
            for h in e[pgm]
            for i in e[pgm][h]
            for pro in e[pgm][h][i]
            for d in e[pgm][h][i][pro]
            if 'ip_version' in e[pgm][h][i][pro][d]
            for ipv in e[pgm][h][i][pro][d]['ip_version']
            if 'auth' in e[pgm][h][i][pro][d]['ip_version'][ipv]
            for aut in e[pgm][h][i][pro][d]['ip_version'][ipv]['auth']
            if 'disconnects' in e[pgm][h][i][pro][d] \
            ['ip_version'][ipv]['auth'][aut]
        ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]][t[3]][t[4]] \
                       ['ip_version'][t[5]]['auth'][t[6]] \
                       ['disconnects']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]][t[3]][t[4]] \
                         ['ip_version'][t[5]]['auth'][t[6]] \
                         ['disconnects']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
            'ip_version': ('%s', lambda t, d: t[5]),
            'auth': ('%s', lambda t, d: t[6]),
        },
    },

    {
        'base': 'xrootd_data_opens',
        'type': 'counter',
        'help': 'number of opens',
        'select': lambda e: [
            (pgm, h, i, pro, d, ipv, aut) for pgm in e
            for h in e[pgm]
            for i in e[pgm][h]
            for pro in e[pgm][h][i]
            for d in e[pgm][h][i][pro]
            if 'ip_version' in e[pgm][h][i][pro][d]
            for ipv in e[pgm][h][i][pro][d]['ip_version']
            if 'auth' in e[pgm][h][i][pro][d]['ip_version'][ipv]
            for aut in e[pgm][h][i][pro][d]['ip_version'][ipv]['auth']
            if 'opens' in e[pgm][h][i][pro][d] \
            ['ip_version'][ipv]['auth'][aut]
        ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]][t[3]][t[4]] \
                       ['ip_version'][t[5]]['auth'][t[6]] \
                       ['opens']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]][t[3]][t[4]] \
                         ['ip_version'][t[5]]['auth'][t[6]] \
                         ['opens']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
            'ip_version': ('%s', lambda t, d: t[5]),
            'auth': ('%s', lambda t, d: t[6]),
        },
    },

    {
        'base': 'xrootd_data_opens_rw',
        'type': 'counter',
        'help': 'number of opens for read-write',
        'select': lambda e: [
            (pgm, h, i, pro, d, ipv, aut) for pgm in e
            for h in e[pgm]
            for i in e[pgm][h]
            for pro in e[pgm][h][i]
            for d in e[pgm][h][i][pro]
            if 'ip_version' in e[pgm][h][i][pro][d]
            for ipv in e[pgm][h][i][pro][d]['ip_version']
            if 'auth' in e[pgm][h][i][pro][d]['ip_version'][ipv]
            for aut in e[pgm][h][i][pro][d]['ip_version'][ipv]['auth']
            if 'rw-opens' in e[pgm][h][i][pro][d] \
            ['ip_version'][ipv]['auth'][aut]
        ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]][t[3]][t[4]] \
                       ['ip_version'][t[5]]['auth'][t[6]] \
                       ['rw-opens']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]][t[3]][t[4]] \
                         ['ip_version'][t[5]]['auth'][t[6]] \
                         ['rw-opens']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
            'ip_version': ('%s', lambda t, d: t[5]),
            'auth': ('%s', lambda t, d: t[6]),
        },
    },
]

import metrics
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

if __name__ == '__main__':
    udp_host = ''
    udp_port = 9486
    http_host = 'localhost'
    http_port = 8746
    silent = False
    fake_log = '/tmp/xrootd-detail.log'
    domain_conf = None
    endpoint = None
    pidfile = None
    log_params = {
        'format': '%(asctime)s %(levelname)s %(message)s',
        'datefmt': '%Y-%d-%mT%H:%M:%S',
    }
    opts, args = gnu_getopt(sys.argv[1:], "zl:U:u:d:o:M:t:T:",
                            [ 'log=', 'log-file=', 'pid-file=' ])
    for opt, val in opts:
        if opt == '-U':
            udp_host = val
        elif opt == '-u':
            udp_port = int(val)
        elif opt == '-M':
            endpoint = val
        elif opt == '-o':
            fake_log = val
        elif opt == '-d':
            domain_conf = val
        elif opt == '-z':
            silent = True
        elif opt == '-t':
            http_port = int(val)
        elif opt == '-T':
            http_host = val
        elif opt == '--log':
            log_params['level'] = getattr(logging, val.upper(), None)
            if not isinstance(log_params['level'], int):
                sys.stderr.write('bad log level [%s]\n' % val)
                sys.exit(1)
                pass
            pass
        elif opt == '--log-file':
            log_params['filename'] = val
        elif opt == '--pid-file':
            if not val.endswith('.pid'):
                sys.stderr.write('pid filename %s must end with .pid\n' % val)
                sys.exit(1)
                pass
            pidfile = val
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

    ## This serves no metrics, only the documentation.
    history = metrics.MetricHistory(schema, horizon=30)

    rmw = metrics.RemoteMetricsWriter(endpoint=endpoint,
                                      schema=schema,
                                      job='xrootd_detail',
                                      expiry=10*60)

    detailer = Detailer(logname=fake_log, rmw=rmw, domfile=domain_conf)
    def handler(signum, frame):
        logging.root.handlers = []
        logging.basicConfig(**log_params)
        logging.info('rotation')
        detailer.relog()
        pass
    signal.signal(signal.SIGHUP, handler)

    partial_handler = functools.partial(metrics.MetricsHTTPHandler,
                                        hist=history)
    webserver = HTTPServer((http_host, http_port), partial_handler)
    logging.info('Created HTTP server on http://%s:%d' %
                 (http_host, http_port))

    server = socketserver.UDPServer((udp_host, udp_port), detailer.handler())
    server.max_packet_size = 64 * 1024

    try:
        if pidfile is not None:
            with open(pidfile, "w") as f:
                f.write('%d\n' % os.getpid())
                pass
            pass

        ## Use a separate thread to run the server, which we can stop
        ## by calling shutdown().
        srv_thrd = threading.Thread(target=HTTPServer.serve_forever,
                                    args=(webserver,))
        srv_thrd.start()

        logging.info('Started')
        try:
            server.serve_forever()
        except KeyboardInterrupt as e:
            pass

        logging.info('Stopping')
        history.halt()
        logging.info('Halted history')
        webserver.shutdown()
        webserver.server_close()
        logging.info('Server stopped.')
    finally:
        if pidfile is not None:
            os.remove(pidfile)
            pass
        pass
    pass
