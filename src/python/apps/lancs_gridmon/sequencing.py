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

class FixedSizeResequencer:
    def __init__(self, scope, window, action, timeout=3, drop=None, lost=None,
                 init_expect=10, logpfx='seq'):
        if scope < 1:
            raise IndexError('scope %d must be positive' % scope)
        if window < 1:
            raise IndexError('window %d must be positive' % window)
        if window >= scope:
            raise IndexError('window %d must >= scope %d' % (window, scope))
        if init_expect > window:
            raise IndexError('init_expect %d must <= window %d' %
                             (init_expect, window))
        self._pmax = window
        self._psz = scope
        self._timeout = timeout
        self._action = action
        self._drop = drop
        self._lost = lost
        self._logpfx = logpfx
        self._init_expect = init_expect

        self._base = None
        self._lim = None
        self._cache = [ None ] * self._psz
        pass

    ## How far ahead of _pseq is idx in the cycle?
    def __offset(self, idx):
        return (idx + self._psz - self._base) % self._psz

    ## What is base + ln within the cycle?  base is assumed to be
    ## within the cycle.  ln must be >= _psz.
    def __advance(self, base, ln):
        assert ln >= -self._psz
        return (base + ln + self._psz) % self._psz

    ## Return true if the supplied data was neither used nor logged.
    def submit(self, now, pseq, *args, **kwargs):
        if pseq < 0 or pseq >= self._psz:
            raise IndexError('pseq %d not in [0, %d)' % (pseq, self._psz))
        if self._base is None:
            ## This is the very first entry.  Assume we've just missed
            ## a few before.
            self._lim = self._base = self.__advance(pseq, -self._init_expect)
            # logging.debug('%s ev=init base=%d lim=%d nseq=%d' %
            #               (self._logpfx, self._base, self._lim, pseq))
            pass
        assert self.__offset(self._lim) <= self._pmax

        ## Clear out expired expectations, and process any stored
        ## entries if they should be processed by now.  Stop if we
        ## encounter a missing entry that hasn't expired.  Stop if we
        ## meet the slot for our new entry; even if the entry has
        ## expired, let's shove it in.
        #self.__clear(now, stop_if_early=True, cur=pseq)
        self.__clear(now, stop_if_early=True)

        if self.__offset(pseq) >= self._pmax:
            ## There's still time to wait for missing packets.  This
            ## new one is suspiciously early, so drop it.
            # logging.warning('%s ev=drop base=%d end=%d nseq=%d' %
            #                 (self._logpfx, self._base,
            #                  self.__advance(self._base, self._pmax),
            #                  pseq))
            if self._drop is not None:
                self._drop(now, pseq, *args, **kwargs);
                return False
            return True
        assert self.__offset(pseq) < self._pmax

        ## Set expiries on missing entries just before this one.
        expiry = now + self._timeout
        while self.__offset(self._lim) < self.__offset(pseq):
            self._cache[self._lim] = expiry
            # logging.debug('%s ev=to base=%d lim=%d exp=%d' %
            #               (self._logpfx, self._base, self._lim, expiry - now))
            self._lim = self.__advance(self._lim, 1)
            continue
        assert self.__offset(self._lim) >= self.__offset(pseq)

        ## Store the entry for later processing.
        assert self.__offset(self._lim) <= self._pmax
        assert self.__offset(pseq) < self._pmax
        old = self._cache[pseq]
        self._cache[pseq] = (now, expiry, args, kwargs)
        if type(old) is tuple:
            onow, oexpiry, oargs, okwargs = old
            # logging.warning('%s ev=replace nseq=%d' % (self._logpfx, pseq))
            pass

        if pseq == self._lim:
            ## Step over the one we've just added.
            self._lim = self.__advance(self._lim, 1)
            assert self._cache[self._lim] is None
            pass

        # logging.debug('%s ev=exps base=%d lim=%d nseq=%d' %
        #               (self._logpfx, self._base, self._lim, pseq))
        assert self.__offset(self._lim) <= self._pmax

        ## Decode all messages before any gaps that haven't expired.
        self.__clear(now)

        msg = ''
        n = self.__offset(self._lim)
        assert n <= self._pmax
        for i in range(0, n):
            sn = self.__advance(self._base, i)
            ce = self._cache[sn]
            if type(ce) is tuple:
                ts, exp, oargs, okwargs = ce
                msg += 'M'
                pass
            elif ce is None:
                msg += '!'
                pass
            else:
                msg += str(min(9, int(ce - now)))
                pass
            continue
        # logging.debug('%s ev=win base=%d lim=%d pat="%s"' %
        #               (self._logpfx, self._base, self._lim, msg))
        return False

    def __clear(self, now, stop_if_early=False, cur=None):
        # logging.debug('%s ev=clear base=%d lim=%d%s%s' %
        #               (self._logpfx, self._base, self._lim,
        #                '' if cur is None else ' nseq=%d' % cur,
        #                ' early=yes' if stop_if_early else ''))
        assert self.__offset(self._lim) <= self._pmax

        while self._base != self._lim and self._base != cur:
            adv = False
            ce = self._cache[self._base]
            assert ce is not None

            try:
                if type(ce) is tuple:
                    ts, exp, args, kwargs = ce
                    if stop_if_early and exp > now:
                        return
                    adv = True
                    self._action(ts, self._base, *args, **kwargs);
                    pass
                else:
                    if ce > now:
                        return
                    adv = True
                    if self._lost is not None:
                        self._lost(ce, self._base)
                    pass
            finally:
                if adv:
                    self._cache[self._base] = None
                    self._base = self.__advance(self._base, 1)
                    pass
                pass
            continue
        pass

    pass

if __name__ == '__main__':
    import time
    import random

    log_params = {
        'format': '%(asctime)s %(levelname)s %(message)s',
        'datefmt': '%Y-%m-%dT%H:%M:%S',
        # 'level': logging.DEBUG,
    }
    logging.basicConfig(**log_params)

    def action(ts, pseq, *args, **kwargs):
        print('%9.3f %2d OKAY args=%s kwargs=%s' % (ts, pseq, args, kwargs))
        pass

    def drop(ts, pseq, *args, **kwargs):
        print('%9.3f %2d DROP args=%s kwargs=%s' % (ts, pseq, args, kwargs))
        pass

    def lost(ts, pseq):
        print('%9.3f %2d LOST' % (ts, pseq))
        pass

    my_seq = FixedSizeResequencer(16, 8, action,
                                  drop=drop, lost=lost,
                                  init_expect=4, timeout=1.5)
    play = list()
    play += ((12, 2.0, "sib", "choot"),)
    play += ((10, 0.5, "heek", "trah"),)
    play += ((13, 1.0, "dook", "woah"),)
    play += ((15, 1.0, "dork", "wobo"),)
    play += ((14, 1.0, "naph", "tool"),)
    play += ((0, 1.0, "naph", "tool"),)
    play += ((1, 1.0, "naph", "tool"),)
    play += ((3, 1.0, "naph", "tool"),)
    play += ((4, 1.0, "naph", "tool"),)
    play += ((5, 1.0, "naph", "tool"),)
    play += ((6, 1.0, "naph", "tool"),)
    play += ((0, 1.0, "naph", "tool"),)
    play += ((7, 1.0, "naph", "tool"),)
    play += ((9, 1.0, "naph", "tool"),)
    play += ((10, 1.0, "naph", "tool"),)
    play += ((12, 1.0, "naph", "tool"),)
    play += ((13, 1.0, "naph", "tool"),)
    play += ((11, 1.0, "naph", "tool"),)
    play += ((14, 1.0, "naph", "tool"),)
    for item in play:
        print('')
        time.sleep(item[1])
        now = time.time()
        pseq = item[0]
        rem = item[2:]
        my_seq.submit(now, pseq, *rem)
        continue

    pass
