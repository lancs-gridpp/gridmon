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
    def __init__(self, stod, addr, user,
                 id_timeout=60*120, seq_timeout=2, domains=None):
        self._user = user
        self._seq_to = seq_timeout
        self._id_to = id_timeout
        self._domains = domains
        self.stod = stod
        self.addr = addr
        self._host = None
        self._inst = None
        self._pgm = None
        self._map_reseqs = dict() ## indexed by sid
        self._file_reseqs = dict() ## indexed by sid
        self._gstream_reseqs = dict() ## indexed by sid

        ## Learn identities supplied by the server, as specified by
        ## monitor mapping messages.  Index is the dictid.  Values are
        ## a map with 'expiry' timestamp (which could be updated),
        ## 'code' (the type of the message that the mapping came from)
        ## and 'info' (data parsed from the mapping message).
        ## self.id_clear(ts) flushes out anything older than the
        ## specified timestamp.
        self._ids = dict()
        self.__info('ev=new-entry')
        self._last_id = None
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
        r = self._ids.get(dictid)
        if r is None:
            return None
        r['expiry'] = now + self._id_to
        return r['info']

    ## Flush out identities with old expiry times.
    def id_clear(self, now):
        ks = [ k for k, v in self._ids.items() if now > v["expiry"] ]
        for k in ks:
            self._ids.pop(k, None)
            continue
        self.__info('ev=purged-dict amount=%d rem=%d', len(ks), len(self._ids))
        pass

    def __log(self, lvl, msg, *args):
        msg = 'peer=%s:%d ' + msg
        args = self.addr + args
        if self._host is not None:
            msg = 'xrdid=%s@%s pgm=%s ' + msg
            args = (self._inst, self._host, self._pgm) + args
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
        return self._host is not None

    def set_identity(self, host, inst, pgm):
        self._host = host
        self._inst = inst
        self._pgm = pgm
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

    def __replace_dictid(self, now, obj, k, rec):
        did = obj.get(k + '_dictid', None)
        if did is None:
            return
        r = self.__id_get(now, did)
        if r is None:
            self.__warning('dictid=%d field=%s' +
                           ' ev=unknown-dictid' +
                           ' rec=%s', did, k, rec)
            self.__schedule_record(ts, 'unk-dict', {
                'rec': rec,
                'field': k,
            }, ctxt=None)
            return
        obj[k] = r
        return r

    def __call_user(self, fn, ts, *args, **kwargs):
        """Call a user function if present, passing the supplied
        timestamp, the program*, host* and instance*, and any
        additional arguments.  *If these extra fields are missing, no
        call is made.

        """

        ## Send nothing if we can't provide all the arguments.
        if self._host is None:
            return
        assert self._pgm is not None
        assert self._inst is not None

        ## Send nothing if the function is not present.
        attr = getattr(self._user, fn, None)
        if attr is None or not callable(attr):
            return

        attr(ts, self._pgm, self._host, self._inst, *args, *kwargs)
        pass

    def _get_map_resequencer(self, sid):
        seq = self._map_reseqs.get(sid)
        if seq is not None:
            return seq
        seq = Resequencer(256, 32,
                          functools.partial(self.__mapping_sequenced, sid),
                          timeout=self._seq_to,
                          logpfx='peer=%s:%d seq=map sid=%012x' %
                          (self.addr + (sid,)))
        self._map_reseqs[sid] = seq
        return seq

    def _get_file_resequencer(self, sid):
        seq = self._file_reseqs.get(sid)
        if seq is not None:
            return seq
        seq = Resequencer(256, 32,
                          functools.partial(self.__file_event_sequenced, sid),
                          timeout=self._seq_to,
                          logpfx='peer=%s:%d seq=file sid=%012x' %
                          (self.addr + (sid,)))
        self._file_reseqs[sid] = seq
        return seq

    def _get_gstream_resequencer(self, sid):
        seq = self._gstream_reseqs.get(sid)
        if seq is not None:
            return seq
        seq = Resequencer(256, 32,
                          functools.partial(self.__gstream_event_sequenced, sid),
                          timeout=self._seq_to,
                          logpfx='peer=%s:%d seq=gstream sid=%012x' %
                          (self.addr + (sid,)))
        self._gstream_reseqs[sid] = seq
        return seq

    ## Accept a decoded packet for processing.  This usually means
    ## working out what sequence it belongs to, and submitting it for
    ## resequencing.
    def process(self, now, pseq, msg):
        if 'mapping' in msg or 'traces' in msg:
            ## All mapping and trace messages belong to the same
            ## sequence.  Submitting to the resequencer results in a
            ## potentially deferred call to
            ## self.__mapping_sequenced(sid, now, pseq, msg).  We need
            ## to pass the whole message to that that function can
            ## handle the two broad classes of event distincty.
            self.__debug('sn=%d type=map-trace msg=%s', pseq, msg)
            sid = msg['traces'][0]['sid'] if 'traces' in msg \
                else msg['mapping']['info']['sid']
            self.__get_map_resequencer(sid).submit(now, pseq, msg)
            return

        if 'file' in msg:
            ## The first entry must be a timing mark, and includes the
            ## sid.  Redundantly, other timing marks in the same
            ## sequence will repeat the sid.  TODO: Log warnings,
            ## don't fail assertions.
            assert len(msg['file']) > 0, "no entries"
            assert 'time' in msg['file'][0], "first entry is not timing mark"
            sid = msg['file'][0]['time']['sid']

            ## 'file' messages (from the f-stream) need their own
            ## resequencing.  Submitting to the resequencer results in
            ## a potentially deferred call to
            ## self.__file_event_sequenced(sid, now, pseq,
            ## msg['file']).
            self._get_file_resequencer(sid).submit(now, pseq, msg['file'])
            return

        if 'gstream' in msg:
            ## 'gstream' messages need their own resequencing.
            ## Submitting to the resequencer results in a potentially
            ## deferred call to self.__gstream_event_sequenced(sid,
            ## now, pseq, msg['stream']).
            sid = msg['gstream']['sid']
            self._get_gstream_resequencer(sid).submit(now, pseq, msg['stream'])
            return

        if 'code' in msg:
            self.__warning('ev=unh nseq=%d msg=%s', pseq, msg)
            return

        self.__warning('ev=ign nseq=%d msg=%s', pseq, msg)
        return

    def __schedule_record(self, ts, ev, data, ctxt={}):
        if self._inst is None or self._host is None or self._pgm is None:
            return False
        return self._user.store_event(ts, self._pgm, self._host, self._inst,
                                      ev, data, ctxt)

    ## Calls to this are set up in self.process (the 'mapping'
    ## branch).
    def __mapping_sequenced(self, sid, ts, pseq, msg):
        self.__debug('ev=map num=%d msg=%s', pseq, msg)

        ## Trace messages are not mapping messages, but they appear to
        ## belong to the same sequence.
        if 'traces' in msg:
            self.__call_user('traces', ts, msg['traces'])
            return

        assert 'mapping' in msg
        mpg = msg['mapping']
        kind = mpg['kind']
        info = mpg['info']
        dictid = mpg['dictid']

        ## A server-id mapping has a zero dictid, and just describes
        ## the peer in more detail.
        if kind == 'server':
            assert dictid == 0
            self._user.record_identity(self,
                                       info['userid']['host'],
                                       info['pgm'], info['inst'])
            return

        ## An xfer-id mapping has a zero dictid, so whatever it is,
        ## it's not actually defining a mapping.
        if kind == 'xfer':
            assert dictid == 0
            self.__call_user('file_transferred', ts, info)
            return

        ## A file-purge-id mapping has a zero dictid, so whatever it
        ## is, it's not actually defining a mapping.
        if kind == 'file-purge':
            assert dictid == 0
            self.__call_user('file_purged', ts, info)
            return

        ## Although there are several types of mapping, they all seem
        ## to use the same dictionary space, so one dict is enough for
        ## all types.  We record the mapping kind, but only for
        ## diagnostics; we assume references are correctly used.
        self._ids[dictid] = {
            "expiry": ts + self._id_to,
            "type": kind,
            "info": info,
        }

        ## Keep track of dictids that don't get reported.  It could be
        ## down to network loss, but it might also be that some
        ## dictids are not reported because they're only referenced in
        ## messages that are not sent due to configuration.
        if self._last_id is not None:
            skip = dictid - self._last_id
            if skip > 1 and skip < 0x80000000:
                id_from = (self._last_id + 1) % 0x100000000
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
        self._last_id = dictid
        return

    ## Calls to this are set up in self.process (the 'file' branch).
    def __file_event_sequenced(self, sid, ts, pseq, ents):
        self.__debug('ev=sid num=%d sid=%012x type=%s ents=%s',
                     pseq, sid, status, ents)

        ## The first entry is always a timing mark.
        hdr = ents[0]
        assert 'time' in hdr
        t0 = hdr['tbeg']
        t1 = hdr['tend']
        td = t1 - t0
        nent = hdr['ntot']

        scheduled = 0
        too_old = 0
        for pos, ent in enumerate(ents[1:]):
            ## Interpolate the time of each event.
            ts = t0 + td * ((pos - 1) / nent)

            if 'disc' in ent:
                msg = dict()
                usr = self.__replace_dictid(ts, ent['disc'],
                                            'user', 'file-disconnect')
                if usr is not None:
                    merge_trees(msg, {
                        'prot': usr['prot'],
                        'user': usr['user'],
                        'client_name': usr['host'],
                        'client_addr': usr['args']['host_addr'],
                        'ipv': usr['args']['ip_version'],
                        'dn': usr['args']['dn'],
                        'auth': usr['args']['proto'],
                    })
                    pass
                self.__add_domain(msg, 'client_name', 'client_domain')
                scheduled += 1
                if self.__schedule_record(ts, 'disconnect', msg):
                    too_old += 1
                    pass
                pass
            elif 'open' in ent:
                fil = self.__replace_dictid(ts, ent['open'], 'file', 'file-open')
                ufn_p = ent['open'].get('lfn')
                ufn = self.__replace_dictid(ts, ent['open'], 'user', 'file-open')
                ufn_id = ent['open'].get('user_dictid')
                rw = ent['open']['mode']
                msg = { 'rw': rw }
                if ufn_p is not None:
                    msg['path'] = ufn_p
                    pass
                if ufn is not None:
                    merge_trees(msg, {
                        'prot': ufn['prot'],
                        'user': ufn['user'],
                        'client_name': ufn['host'],
                        'client_addr': ufn['args']['host_addr'],
                        'ipv': ufn['args']['ip_version'],
                        'dn': ufn['args']['dn'],
                        'auth': ufn['args']['proto'],
                    })
                    pass
                self.__add_domain(msg, 'client_name', 'client_domain')
                scheduled += 1
                if self.__schedule_record(ts, 'open', msg):
                    too_old += 1
                    pass
                pass
            elif 'close' in ent:
                fil = self.__replace_dictid(ts, ent['close'], 'file', 'file-close')
                msg = {
                    'read_bytes': ent['close']['read_bytes'],
                    'readv_bytes': ent['close']['readv_bytes'],
                    'write_bytes': ent['close']['write_bytes'],
                    'forced': ent['close']['forced'],
                }
                if fil is not None:
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
            elif 'xfr' in ent:
                # fil = self.__id_get(ts, ent.pop('file'))
                # print('%d xfr: file=%s stats=%s' % (pos, fil, ent))
                pass
            elif 'time' in ent:
                # print('time: detail=%s' % ent)
                pass
            continue
        if too_old > 0:
            self.__warning('ev=old-events tried=%d missed=%d',
                           scheduled, too_old)
            pass

        pass

    ## Calls to this are set up in self.process (the 'gstream'
    ## branch).
    def __gstream_event_sequenced(self, sid, ts, pseq):
        ## TODO
        pass

    pass
