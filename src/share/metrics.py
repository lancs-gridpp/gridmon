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

import threading
import time


class MetricHistory:
    def __init__(self, schema, horizon=60*30):
        self.timestamps = { }
        self.horizon = horizon
        self.schema = schema
        self.running = True
        self.lock = threading.Lock()
        self.entries = { }
        pass

    @staticmethod
    def __merge(a, b, pfx=()):
        for key, nv in b.items():
            ## Add a value if not already present.
            if key not in a:
                a[key] = nv
                continue

            ## Compare the old value with the new.  Apply recursively if
            ## they are both dictionaries.
            ov = a[key]
            if isinstance(ov, dict) and isinstance(nv, dict):
                MetricHistory.__merge(ov, nv, pfx + (key,))
                continue

            ## The new value and the existing value must match.
            if ov != nv:
                raise Exception('bad merge (%s over %s at %s)' %
                                (nv, ov, '.'.join(pfx + (key,))))

            continue
        pass

    def install(self, samples):
        with self.lock:
            ## Identify times which can be discarded.
            threshold = int(time.time()) - self.horizon

            ## Merge the new data with the old.
            MetricHistory.__merge(self.entries, samples)

            ## Discard old entries.
            for k in [ k for k in self.entries if k < threshold ]:
                del self.entries[k]
                continue

            nts = min(self.entries.keys())
            for ident ts in self.timestamps:
                if nts < ts:
                    self.timestamps[ident] = nts
                    pass
                continue
            pass
        pass

    def __sample(self, k, tup, mtr, fmt, func, attrs):
        entry = self.entries[k]
        value = func(tup, entry)

        ## Each element of attrs gives the attribute name, the format
        ## specifier, and a function to provide the value.  The
        ## function is provided with the tuple that identifies the
        ## metric, and the data at the given timestamp k.
        labels = []
        if attrs is None:
            attrs = { }
            pass
        for an, (afmt, af) in attrs.items():
            av = af(tup, entry)
            labels.append(('%s="' + afmt + '"') % (an, av))
            continue

        msg = ''

        if callable(fmt):
            ## Treat the extracted value as histogram data, and
            ## convert it into a dict of 'sum' (the sum of the events'
            ## values), 'count' (the number of events), 'count_name'
            ## (the name of the count metric suffix, defaulting to
            ## 'count'), and 'sum_name' (the name of the sum metric
            ## suffix, defaulting to 'sum'), with the remaining keys
            ## being int/float thresholds giving the cummulative
            ## contents of each bucket.  The first entry must be 0.
            value = fmt(value)

            ## Extract the summary data.
            gsum = value['sum']
            gcount = value['count']
            gcount_name = value.get('count_name') or 'count'
            gsum_name = value.get('sum_name') or 'sum'

            ## Extract and sort the thresholds.
            thrs = [ thr for thr in value.keys if isinstance(thr, (int,float)) ]
            thrs.sort()

            ## Generate the buckets.
            for thr in thrs:
                msg += mtr + "_bucket"
                msg += '{'
                msg += ','.join(labels + ('le="%g"' % thr,))
                msg += '}'
                msg += (' %d %.3f\n') % (value[thr], k)
                continue

            ## The +inf bucket and the count are the same.
            msg += mtr + "_bucket"
            msg += '{'
            msg += ','.join(labels + ('le="+inf"',))
            msg += '}'
            msg += (' %d %.3f\n') % (gcount, k)
            msg += mtr + "_" + gcount_name
            msg += '{'
            msg += ','.join(labels)
            msg += '}'
            msg += (' %d %.3f\n') % (gcount, k)

            msg += mtr + "_" + gsum_name
            msg += '{'
            msg += ','.join(labels)
            msg += '}'
            msg += (' %d %.3f\n') % (gsum, k)
        else:
            ## The metric is a single value, not a histogram.
            msg += mtr
            msg += '{'
            msg += ','.join(labels)
            msg += '}'
            msg += (' ' + fmt + ' %.3f\n') % (value, k)
            pass

        return msg

    def __family(self, ks, se):
        mtr = se['base']
        sel = se['select']
        bits = se['samples']
        hlp = se.get('help')
        unit = se.get('unit')
        typ = se.get('type')
        attrs = se.get('attrs')

        ## The name of a metric with a unit should end with the unit.
        if unit is not None:
            mtr += '_' + unit
            pass

        ## Within this family, build up an index by metric (identified
        ## by a tuple), and list the timestamps that contribute points
        ## to that metric.
        tses_for_tup = { }
        for k in ks:
            entry = self.entries.get(k)
            if entry is None:
                continue
            for tup in sel(entry):
                kset = tses_for_tup.setdefault(tup, set())
                kset.add(k)
                continue
            continue

        ## Start the message with metadata.
        msg = ''
        if typ is not None:
            msg += '# TYPE %s %s\n' % (mtr, typ)
            pass
        if unit is not None:
            msg += '# UNIT %s %s\n' % (mtr, unit)
            pass
        if hlp is not None:
            ## TODO: Escape the message.
            msg += '# HELP %s %s\n' % (mtr, hlp)
            pass

        for tup, kset in tses_for_tup.items():
            ## Get timestamps in order.
            kseq = list(kset)
            kseq.sort()

            ## Do each metric point.
            for k in kseq:
                ## Do each metric sample.
                for sfx, (fmt, func) in bits.items():
                    msg += self.__sample(k, tup, mtr + sfx,
                                         fmt, func, attrs)
                    continue
                continue
            continue

        return msg

    def get_message(self, ident):
        msg = ''

        with self.lock:
            ## Get the timestamp for this client.
            ts = self.timestamps.setdefault(ident, 0)

            ## Identify the most recent entries that the client has
            ## not yet seen.  These are sorted to ensure all metrics
            ## come out in the same order.
            ks = [ k for k in self.entries if k > ts ]
            ks.sort()

            ## Identify the latest time of all matching entries and
            ## the caller's timestamp.
            latest = ts if len(ks) == 0 else max(ts, ks[-1])

            ## Build up a message with any data that has arrived since
            ## ts.
            for ms in self.schema:
                msg += self.__family(ks, ms)
                continue

            ## Complete the message.
            msg += '# EOF\n'

            ## Prevent sending these metrics to the client again.
            self.timestamps[ident] = latest

            pass

        return msg

    def check(self):
        with self.lock:
            return self.running
        pass

    def halt(self):
        with self.lock:
            self.running = False
            pass
        pass

    pass
