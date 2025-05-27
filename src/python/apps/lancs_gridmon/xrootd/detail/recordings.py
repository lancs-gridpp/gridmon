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

import copy
import logging
import os
import math
from datetime import datetime
import lancs_gridmon.logfmt as logfmt

class Recorder:
    def __init__(self, t0, logname, rmw, wr_ival=60, horizon=5*60, epoch=0):
        self._writer = rmw
        self._t0 = int(t0 * 1000)
        self._epoch = epoch

        ## Maintain a sequence of parsed and restructured events.  The
        ## key is a timestamp (integer, milliseconds), and the value
        ## is a list of tuples (instance, host, program, event,
        ## params).
        self._events = { }

        ## How far back do we keep events?  Units are seconds.
        self._horiz_ival = horizon

        ## We don't retain events earlier than the horizon (in ms).
        ## It's initial value is our reset time.  This value is
        ## updated by self.__release_events.
        self._horiz = int(self._t0 * 1000)

        ## Remote-write new data at this interval.  Units are
        ## milliseconds.
        self._wr_ival = wr_ival = int(wr_ival * 1000)

        ## Set the time (in ms) of the first write.  Round it forwards
        ## to a multiple of the interval.
        self._wr_ts = int(math.ceil(self._t0 / wr_ival) * wr_ival)

        ## This holds accumulated statistics, indexed by pgm, host,
        ## inst, client domain, stat (read, readv, write), then
        ## 'value', 'zero' (the time of the last counter reset), and
        ## 'last' (the time of the last increment).
        self._stats = { }

        ## We generate a 'fake' log based on various events.
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
        ## The timestamp of the event is in seconds.  We use an
        ## integer number of milliseconds internally.
        ts_ms = int(ts * 1000)

        ## Events earlier than the horizon are discarded.
        if ts_ms < self._horiz:
            logging.warning(('expired event@%.3f<%.3f (%.3f behind)' +
                             ' %s:%s@%s %s %s') % \
                            (ts_ms / 1000 - self._epoch,
                             self._horiz / 1000 - self._epoch,
                             (self._horiz - ts_ms) / 1000,
                             pgm, inst, host, ev, params))
            return True

        ## Ensure we have an entry at the specified time, and append
        ## this event to it.
        grp = self._events.setdefault(ts_ms, list())
        grp.append((inst, host, pgm, ev, params, ctxt))
        logging.debug('installing event@%.3f %s:%s@%s %s %s' % \
                      (ts / 1000 - self._epoch, pgm, inst, host, ev, params))
        return False

    ## Increment an integer counter.  'data' is a dict with 'value',
    ## 'zero' and 'last' (empty on first use).  'inc' is amount to
    ## increase by.  't0' is the default reset time.  't1' is now.
    ## Units are seconds.
    def __inc(self, t1, data, inc):
        if 'value' not in data:
            data['value'] = 0
            data['zero'] = self._t0
            pass
        old = data['value']
        data['value'] += inc

        ## Detect 64-bit wrap-around.  Interpolate where the reset
        ## occurred.
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
        ## The time 'now' (in seconds) has been reached.  Data earlier
        ## than a period just before then ('back', in milliseconds)
        ## can be consumed.  We can't go backwards.
        back = int((now - self._horiz_ival) * 1000)
        if back <= self._horiz:
            return
        logging.debug('%.3f advancing from %.3f (+%.3fs)' % \
                      (back / 1000 - self._epoch,
                       self._horiz / 1000 - self._epoch,
                       (back - self._horiz) / 1000))

        ## Build up a message for a remote write.
        data = dict()

        while True:
            ## The horizon must be behind the next write epoch, but
            ## not by more then the write interval.  Advance the write
            ## epoch until it is just ahead of the current horizon,
            ## populating the message with a copy of the metrics each
            ## time.
            while self._horiz >= self._wr_ts:
                logging.debug('point %.3f (wr-ep=%.3f)' % \
                              (self._horiz / 1000 - self._epoch,
                               self._wr_ts / 1000 - self._epoch))
                data[self._horiz / 1000] = copy.deepcopy(self._stats)
                self._wr_ts += self._wr_ival
                continue
            assert self._wr_ts > self._horiz

            ## We can't aggregrate beyond the new horizon.  If the
            ## next write epoch is beyond the new horizon, just
            ## aggregate until then and stop.
            if self._wr_ts > back:
                self.__release_events(back)
                logging.debug('released to %.3f (final)' % \
                              (back / 1000 - self._epoch))
                break

            ## Aggregate metrics up to the next write epoch, and go
            ## round again to ensure that the write epoch is again
            ## ahead of the horizon.
            self.__release_events(self._wr_ts)
            logging.debug('released to %.3f' % \
                          (self._horiz / 1000 - self._epoch))
            assert self._wr_ts == self._horiz
            continue

        ## Send the message, if it's not empty.
        self._writer.install(data)
        return

    def __release_events(self, ts):
        ## This function updates the limit, and we can only move
        ## forward.
        assert ts >= self._horiz

        ## Identify and sequence (millisecond) timestamps in our event
        ## record until our new limit.
        ks = [ k for k in self._events if k < ts ]
        ks.sort()

        ## Aggregate the events up until the new limit.
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

        ## Set the new limit to indicate what we've already
        ## aggregated.
        self._horiz = ts
        pass

    pass
