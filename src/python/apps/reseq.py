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

class FixedSizeResequencer:
    def __init__(self, scope, window, action, timeout=3, logpfx='seq'):
        self.pmax = window
        self.psz = scope
        self.timeout = timeout
        self.action = action
        self.logpfx = logpfx

        self.pseq = None
        self.plim = None
        self.cache = [ None ] * self.psz
        pass

    def _offset(self, idx):
        return (idx + self.psz - self.pseq) % self.psz

    def _advance(self, base, ln):
        assert ln >= -self.psz
        return (base + ln + self.psz) % self.psz

    def submit(self, now, pseq, *args, **kwargs):
        if self.pseq is None:
            ## This is the very first entry.  Assume we've just missed
            ## a few before.
            self.plim = self.pseq = self._advance(pseq, -10)
            pass
        assert self._offset(self.plim) <= self.pmax

        ## Clear out expired expectations, and process any stored
        ## entries if they should be processed by now.  Stop if we
        ## encounter a missing entry that hasn't expired.  Stop if we
        ## meet the slot for our new entry; even if the entry has
        ## expired, let's shove it in.
        self._clear(now, stop_if_early=True, cur=pseq)

        if self._offset(pseq) >= self.pmax:
            ## There's still time to wait for missing packets.  This
            ## new one is suspiciously early, so drop it.
            logging.warning('%s ev=drop pseq=%d end=%d nseq=%d' %
                            (self.logpfx, self.pseq,
                             self._advance(self.pseq, self.pmax),
                             pseq))
            self.action(now, pseq, drop=True, *args, **kwargs);
            return

        ## Set expiries on missing entries just before this one.
        expiry = now + self.timeout
        while self._offset(self.plim) < self._offset(pseq):
            self.cache[self.plim] = expiry
            self.plim = self._advance(self.plim, 1)
            continue

        ## Store the entry for later processing.
        assert self._offset(self.plim) < self.pmax
        assert self._offset(pseq) <= self.pmax
        old = self.cache[pseq]
        self.cache[pseq] = (now, expiry, args, kwargs)
        if type(old) is tuple:
            onow, oexpiry, oargs, okwargs = old
            logging.warning('%s ev=replace nseq=%d' % (self.logpfx, pseq))
            pass

        if pseq == self.plim:
            ## Step over the one we've just added.
            self.plim = self._advance(self.plim, 1)
            assert self.cache[self.plim] is None
            pass

        logging.debug('%s ev=exps pseq=%d plim=%d nseq=%d' %
                      (self.logpfx, self.pseq, self.plim, pseq))
        assert self._offset(self.plim) <= self.pmax

        ## Decode all messages before any gaps that haven't expired.
        self._clear(now)

        msg = ''
        n = self._offset(self.plim)
        assert n <= self.pmax
        for i in range(0, n):
            sn = self._advance(self.pseq, i)
            ce = self.cache[sn]
            if type(ce) is tuple:
                code, data, ts, exp = ce
                msg += code
                pass
            elif ce is None:
                msg += '!'
                pass
            else:
                msg += str(max(9, int(ce - now)))
                pass
            continue
        logging.debug('%s ev=win pseq=%d plim=%d pat="%s"' %
                      (self.logpfx, self.pseq, self.plim, msg))
        return

    def _clear(self, now, stop_if_early=False, cur=None):
        logging.debug('%s ev=clear pseq=%d plim=%d)' %
                      (self.logpfx, self.pseq, self.plim))
        assert self._offset(self.plim) <= self.pmax

        while self.pseq != self.plim and self.pseq != cur:
            adv = False
            ce = self.cache[self.pseq]
            assert ce is not None

            try:
                if type(ce) is tuple:
                    ts, exp, args, kwargs = ce
                    if stop_if_early and exp > now:
                        return
                    adv = True
                    self.action(ts, self.pseq, drop=False, *args, **kwargs);
                    pass
                else:
                    if ce > now:
                        return
                    adv = True
                    pass
            finally:
                if adv:
                    self.cache[self.pseq] = None
                    self.pseq = self._advance(self.pseq, 1)
                    pass
                pass
            continue
        pass

    pass
