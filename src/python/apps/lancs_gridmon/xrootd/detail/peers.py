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

import functools
from lancs_gridmon.sequencing import FixedSizeResequencer as Resequencer

class Peer:
    def __init__(self, stod, addr, recip, idrec,
                 id_timeout=60*120, seq_timeout=2, domains=None):
        self._recip = recip
        self._idrec = idrec
        self._seq_to = seq_timeout
        self._id_to = id_timeout
        self._domains = domains
        self.stod = stod
        self.addr = addr
        self.host = None
        self.inst = None
        self.pgm = None
        self._sids = { }

        ## Prepare to resequence Monitor Map messages.
        self._map_seq = Resequencer(256, 32, self.__act_on_map,
                                    timeout=self._seq_to,
                                    logpfx='peer=%s:%d seq=map' % self.addr)

        ## Learn identities supplied by the server, as specified by
        ## monitor mapping messages.  Index is the dictid.  Values are
        ## a map with 'expiry' timestamp (which could be updated),
        ## 'code' (the type of the message that the mapping came from)
        ## and 'info' (data parsed from the mapping message).
        ## self.id_clear(ts) flushes out anything older than the
        ## specified timestamp.
        self.ids = { }
        self.__info('ev=new-entry')
        self.last_id = None
        pass

    def __add_domain(self, data, key_out, key_in):
        if self._domains is None:
            return
        host = data.get(key_out)
        if host is None:
            return
        dom = self._domains.derive(host)
        data[key_in] = dom
        return

    def __id_get(self, now, dictid):
        r = self.ids.get(dictid)
        if r is None:
            return None
        r['expiry'] = now + self._id_to
        return r['info']

    ## Flush out identities with old expiry times.
    def id_clear(self, now):
        ks = [ k for k, v in self.ids.items() if now > v["expiry"] ]
        for k in ks:
            self.ids.pop(k, None)
            continue
        self.__info('ev=purged-dict amount=%d rem=%d', len(ks), len(self.ids))
        pass

    def __log(self, lvl, msg, *args):
        msg = 'peer=%s:%d ' + msg
        args = self.addr + args
        if self.host is not None:
            msg = 'xrdid=%s@%s pgm=%s ' + msg
            args = (self.inst, self.host, self.pgm) + args
            pass
        logging.log(lvl, msg, *args)
        pass

    def __critical(self, msg, *args):
        return self.__log(logging.CRITICAL, msg, *args)

    def __error(self, msg, *args):
        return self.__log(logging.ERROR, msg, *args)

    def __warning(self, msg, *args):
        return self.__log(logging.WARNING, msg, *args)

    def __info(self, msg, *args):
        return self.__log(logging.INFO, msg, *args)

    def __debug(self, msg, *args):
        return self.__log(logging.DEBUG, msg, *args)

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

    def __schedule_record(self, ts, ev, data, ctxt={}):
        if self.inst is None or self.host is None or self.pgm is None:
            return False
        return self._recip(ts, self.inst, self.host, self.pgm, ev, data, ctxt)

    def act_on_trace(self, ts, info):
        ## TODO
        pass

    def __act_on_sid(self, sid, ts, pseq, status, data):
        self.__debug('ev=sid num=%d sid=%012x type=%s data=%s',
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
                    usr = self.__id_get(ts, ent['user'])
                    # print('%d disconnect: %s' % (pos, usr))
                    msg = { }
                    if usr is None:
                        self.__warning('dictid=%d field=user' +
                                       ' ev=unknown-dictid' +
                                       ' rec=file-disconnect', ent['user'])
                        self.__schedule_record(ts, 'unk-dict', {
                            'rec': 'file-disconnect',
                            'field': 'user',
                        }, ctxt=None)
                        pass
                    else:
                        merge_trees(msg, {
                            'prot': usr['prot'],
                            'user': usr['user'],
                            'client_name': usr['host'],
                            'client_addr': usr['args']['h'],
                            'ipv': usr['args']['I'],
                            'dn': usr['args']['m'],
                            'auth': usr['args']['p'],
                        })
                        pass
                    self.__add_domain(msg, 'client_name', 'client_domain')
                    scheduled += 1
                    if self.__schedule_record(ts, 'disconnect', msg):
                        too_old += 1
                        pass
                    pass
                elif typ == 'open':
                    fil = self.__id_get(ts, ent['file'])
                    if fil is None:
                        self.__warning('dictid=%d field=file' +
                                       ' ev=unknown-dictid' +
                                       ' rec=file-open', ent['file'])
                        self.__schedule_record(ts, 'unk-dict', {
                            'rec': 'file-open',
                            'field': 'file',
                        }, ctxt=None)
                        pass
                    rw = ent['rw']
                    msg = { 'rw': rw }
                    ufn_s = ent.get('ufn')
                    if ufn_s is None:
                        ufn = None
                        pass
                    else:
                        ufn_id, ufn_p = ufn_s
                        ufn = self.__id_get(ts, ufn_id)
                        msg['path'] = ufn_p
                        pass
                    if ufn is None:
                        if ufn_s is not None:
                            self.__warning('dictid=%d field=ufn' +
                                           ' ev=unknown-dictid' +
                                           ' rec=file-open', ufn_id)
                            self.__schedule_record(ts, 'unk-dict', {
                                'rec': 'file-open',
                                'field': 'ufn',
                            }, ctxt=None)
                            pass
                        pass
                    else:
                        merge_trees(msg, {
                            'prot': ufn['prot'],
                            'user': ufn['user'],
                            'client_name': ufn['host'],
                            'client_addr': ufn['args']['h'],
                            'ipv': ufn['args']['I'],
                            'dn': ufn['args']['m'],
                            'auth': ufn['args']['p'],
                        })
                        pass
                    self.__add_domain(msg, 'client_name', 'client_domain')
                    scheduled += 1
                    if self.__schedule_record(ts, 'open', msg):
                        too_old += 1
                        pass
                    pass
                elif typ == 'close':
                    filid = ent['file']
                    fil = self.__id_get(ts, filid)
                    msg = {
                        'read_bytes': ent['read']['bytes'],
                        'readv_bytes': ent['readv']['bytes'],
                        'write_bytes': ent['write']['bytes'],
                        'forced': ent['forced'],
                    }
                    if fil is None:
                        self.__warning('dictid=%d field=file' +
                                       ' ev=unknown-dictid' +
                                       ' rec=file-close', filid)
                        self.__schedule_record(ts, 'unk-dict', {
                            'rec': 'file-close',
                            'field': 'file',
                        }, ctxt=None)
                        pass
                    else:
                        merge_trees(msg, {
                            'prot': fil['prot'],
                            'user': fil['user'],
                            'client_name': fil['host'],
                            'path': fil['path'],
                        })
                        pass
                    self.__add_domain(msg, 'client_name', 'client_domain')
                    scheduled += 1
                    if self.__schedule_record(ts, 'close', msg):
                        too_old += 1
                        pass
                    pass
                elif typ == 'xfr':
                    # fil = self.__id_get(ts, ent.pop('file'))
                    # print('%d xfr: file=%s stats=%s' % (pos, fil, ent))
                    pass
                elif typ == 'time':
                    # print('time: detail=%s' % ent)
                    pass
                continue
            if too_old > 0:
                self.__warning('ev=old-events tried=%d missed=%d',
                               scheduled, too_old)
                pass
            pass
        ## TODO
        pass

    def __act_on_map(self, ts, pseq, status, data):
        self.__debug('ev=map num=%d type=%s data=%s', pseq, status, data)

        ## A server-id mapping has a zero dictid, and just describes
        ## the peer in more detail.
        if status == 'server-id':
            dictid = data['dictid']
            info = data['info']
            self._idrec(info['host'], info['args']['inst'],
                        info['args']['pgm'], self)
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
            "expiry": ts + self._id_to,
            "type": status,
            "info": info,
        }

        if self.last_id is not None:
            skip = dictid - self.last_id
            if skip > 1 and skip < 0x80000000:
                id_from = (self.last_id + 1) % 0x100000000
                id_to = (dictid + 0xffffffff) % 0x100000000
                self.__warning('ev=skip-dicts count=%d from=%d to=%d',
                               skip - 1, id_from, id_to)
                self.__schedule_record(ts, 'skip-dict', {
                    'from': id_from,
                    'to': id_to,
                    'n': skip - 1,
                }, ctxt=None)
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
            self.__debug('sn=%d type=%s data=%s', pseq, status, data)
            self._map_seq.submit(now, pseq, status, data)
            return

        if status == 'file':
            sid = data['entries'][0][1]['sid']
            sid_data = self._sids.get(sid)
            if sid_data is None:
                sid_action = functools.partial(self.__act_on_sid, sid)
                seq = Resequencer(256, 32, sid_action,
                                  timeout=self._seq_to,
                                  logpfx='peer=%s:%d seq=map sid=%012x' %
                                  (self.addr + (sid,)))
                sid_data = { 'seq': seq }
                self._sids[sid] = sid_data
                pass
            sid_data['seq'].submit(now, pseq, status, data)
            #print('#%d %s: file: sid=%012x %s' % (pseq, status, sid, data))
            return

        if 'code' in data:
            self.__warning('ev=unh nseq=%d status=%s code=%s',
                           pseq, status, data['code'])
            return

        self.__warning('ev=ign nseq=%d status=%s', pseq, status)
        return

    pass
