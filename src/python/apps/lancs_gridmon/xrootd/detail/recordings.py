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
import os
from datetime import datetime
import lancs_gridmon.logfmt as logfmt

class Recorder:
    def __init__(self, t0, logname, rmw):
        self._writer = rmw
        self._t0 = t0

        ## Maintain a sequence of parsed and restructured events.  The
        ## key is a timestamp (integer, milliseconds), and the value
        ## is a list of tuples (instance, host, program, event,
        ## params).
        self._events = { }

        ## How far back do we keep events?  Units are seconds.
        self._horizon = 70
        self._event_limit = self._t0 - self._horizon

        ## Remote-write new data at this interval.  Units are seconds.
        self._write_interval = 60
        self._write_ts = self._t0

        ## This holds ccumulated statistics, indexed by pgm, host,
        ## inst, client domain, stat (read, readv, write), then
        ## 'value', 'zero' (the time of the last counter reset), and
        ## 'last' (the time of the last increment).
        self._stats = { }

        self._log_name = logname
        self._out = open(self._log_name, "a")
        pass

    ## Re-open the fake log for appending, and replace our stream's FD
    ## with the new one.  This should be called on SIGHUP to allow log
    ## rotation.
    def relog(self):
        with open(self._log_name, "a") as nf:
            fd = nf.fileno()
            os.dup2(fd, self._out.fileno())
            pass
        pass

    def log(self, ts, msg):
        tst = datetime.utcfromtimestamp(ts).isoformat('T', 'milliseconds')
        self._out.write('%s %s\n' % (tst, msg))
        pass

    def store_event(self, pgm, host, inst, ts, ev, params, ctxt):
        if ts < self._event_limit:
            logging.warning('discarding expired event@%.3f<%.3f %s:%s@%s %s %s' % \
                      (ts, self._event_limit, pgm, inst, host, ev, params))
            return True
        ts_ms = int(ts * 1000)
        grp = self._events.setdefault(ts_ms, [ ])
        grp.append((inst, host, pgm, ev, params, ctxt))
        logging.debug('installing event@%.3f %s:%s@%s %s %s' % \
                      (ts, pgm, inst, host, ev, params))
        return False

    ## Increment a counter.  'data' is a dict with 'value', 'zero' and
    ## 'last' (empty on first use).  'inc' is amount to increase by.  't0'
    ## is the default reset time.  't1' is now.  Units are seconds.
    def __inc(self, t1, data, inc):
        if 'value' not in data:
            data['value'] = 0
            data['zero'] = self._t0
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

    def advance(self, now):
        self.__release_events(now - self._horizon)
        if now - self._write_ts > self._write_interval:
            now_key = self._event_limit
            data = { now_key: self._stats }
            # print('stats: %s' % self._stats)
            self._writer.install(data)
            assert now >= self._write_ts
            self._write_ts = now
            pass
        pass

    def __release_events(self, ts):
        if ts < self._event_limit:
            return
        ts = int(ts * 1000) ## ts is in milliseconds.
        ks = [ k for k in self._events if k < ts ]
        ks.sort()
        for k in ks:
            for inst, host, pgm, ev, params, ctxt in self._events.pop(k):
                t1 = k / 1000
                if ctxt is not None:
                    self.log(t1, '%s@%s %s %s %s' %
                             (inst, host, pgm, ev, logfmt.encode(params, ctxt)))
                    pass
                stats = self._stats.setdefault(pgm, { }) \
                                   .setdefault(host, { }) \
                                   .setdefault(inst, { })
                if ev == 'skip-dict' and 'n' in params:
                    substats = stats.setdefault('dicts', { })
                    sm = substats.setdefault('skip', { })
                    self.__inc(t1, sm, params['n'])
                    pass
                elif ev == 'unk-dict' and \
                     'rec' in params and \
                     'field' in params:
                    substats = stats.setdefault('dicts', { }) \
                                    .setdefault('unk', { }) \
                                    .setdefault(params['rec'], { }) \
                                    .setdefault(params['field'], { })
                    self.__inc(t1, substats, 1)
                    pass
                elif ev == 'disconnect' and \
                   'prot' in params and \
                   'client_domain' in params and \
                   'ipv' in params and \
                   'auth' in params:
                    substats = stats.setdefault('prot', { }) \
                                    .setdefault(params['prot'], { }) \
                                    .setdefault(params['client_domain'], { }) \
                                    .setdefault('ip_version', { }) \
                                    .setdefault(params['ipv'], { }) \
                                    .setdefault('auth', { }) \
                                    .setdefault(params['auth'], { })
                    dis = substats.setdefault('disconnects', { })
                    self.__inc(t1, dis, 1)
                    pass
                elif ev == 'open' and \
                   'prot' in params and \
                   'client_domain' in params and \
                   'ipv' in params and \
                   'auth' in params and \
                   'rw' in params:
                    substats = stats.setdefault('prot', { }) \
                                    .setdefault(params['prot'], { }) \
                                    .setdefault(params['client_domain'], { }) \
                                    .setdefault('ip_version', { }) \
                                    .setdefault(params['ipv'], { }) \
                                    .setdefault('auth', { }) \
                                    .setdefault(params['auth'], { })
                    ops = substats.setdefault('opens', { })
                    rwops = substats.setdefault('rw-opens', { })
                    self.__inc(t1, ops, 1)
                    self.__inc(t1, rwops, 1 if params['rw'] else 0)
                    pass
                elif ev == 'close' and \
                   'prot' in params and \
                   'client_domain' in params:
                    substats = stats.setdefault('prot', { }) \
                                    .setdefault(params['prot'], { }) \
                                    .setdefault(params['client_domain'], { })
                    cr = substats.setdefault('read', { })
                    crv = substats.setdefault('readv', { })
                    cw = substats.setdefault('write', { })
                    cl = substats.setdefault('closes', { })
                    fcl = substats.setdefault('forced-closes', { })
                    self.__inc(t1, cr, params['read_bytes'])
                    self.__inc(t1, crv, params['readv_bytes'])
                    self.__inc(t1, cw, params['write_bytes'])
                    self.__inc(t1, cl, 1)
                    self.__inc(t1, fcl, 1 if params['forced'] else 0)
                    pass
                continue
            continue
        self._event_limit = ts / 1000
        pass

    pass
