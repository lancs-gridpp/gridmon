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

from http.server import BaseHTTPRequestHandler

def _merge(a, b, pfx=(), mismatch=0):
    for key, nv in b.items():
        ## Add a value if not already present.
        if key not in a:
            a[key] = nv
            continue

        ## Compare the old value with the new.  Apply recursively if
        ## they are both dictionaries.
        ov = a[key]
        if isinstance(ov, dict) and isinstance(nv, dict):
            _merge(ov, nv, pfx + (key,), mismatch=mismatch)
            continue

        if mismatch < 0:
            ## Use the old value.
            continue
        if mismatch > 0:
            ## Replace the old value.
            a[key] = nv
            continue

        ## The new value and the existing value must match.
        if ov != nv:
            raise Exception('bad merge (%s over %s at %s)' %
                            (nv, ov, '.'.join(pfx + (key,))))

        continue
    pass

class MetricHistory:
    """Keeps track of timestamped metrics in a thread-safe way.  Metrics
    timestamped beyond a configurable horizon are discarded.  Data can
    be represented as a text-form OpenMetrics message.  Multiple
    clients requesting the data can be tracked, so that
    retransmissions of the same metric points are minimized.

    """

    def __init__(self, schema, horizon=60*30):
        """The schema is an array of metric family descriptors.  Each is a
        dict with an entry 'base' giving the base name of the family;
        optional 'type' (e.g., 'counter', 'gauge', etc, as specified
        by OpenMetrics); optional 'unit'; optional 'help', 'select' as
        a selection function supplied with each internal entry for a given
        timestamp, and yielding a list of tuples that characterize the
        metric within the entry; 'samples' as a dict mapping from name
        suffix to a tuple of (format specifier, value function); and
        'attrs' as a dict from attribute name to value function.

        A selection function is provided with a dict d, a complete
        entry for a timestamp.  It should return a list of tuples
        describing how to get data from the entry, as well as
        attribute it.

        A value function is provided with a tuple t and a dict d.  t
        is a tuple returned by the selection function, and d is a
        complete entry for a timestamp.  In the 'samples' dict, it
        should extract from d the value indicated by t.  In the
        'attrs' dict, it will usually just return an element from t
        (to be used as an attribute value), and so d will normally be
        ignored, but could be used when defining metadata.

        The format specifier in an 'attrs' entry is used to convert
        the value returned by the value function into a string, which
        will then be (TODO: escaped and) quoted, and placed in the
        attribute set.

        The format specifier in a 'samples' entry may be a string,
        e.g., "%d", or a function.  If a function, it is passed the
        value returned by the corresponding value function, and is
        expected to return a dict with real keys specifying the bucket
        thresholds of a histogram.  In ascending order, these keys
        should yield monotonicly increasing integer values, starting
        at zero, indicating the number of events with values less than
        or equal to the corresponding key.  A 'count' entry should
        then give the total number of events, and a 'sum' entry should
        give the sum of all event values.  The total number of events
        can be greater than the last real entry, if the last bucket
        has no upper bound.

        """
        self.timestamps = { }
        self.horizon = horizon
        self.schema = schema
        self.running = True
        self.lock = threading.Lock()
        self.entries = { }
        pass

    def install(self, samples, mismatch=0):
        """Install new timestamped data, and flush out data beyond the
        horizon.  'samples' is a dict indexed by Unix timestamp, and
        will be merged with existing data.  Set 'mismatch' negative to
        silently avoid replacing values, positive to silently override
        old values, and zero (default) to raise an exception.

        """
        with self.lock:
            ## Identify times which can be discarded.
            threshold = int(time.time()) - self.horizon

            ## Merge the new data with the old.
            _merge(self.entries, samples, mismatch=mismatch)

            ## Discard old entries.
            for k in [ k for k in self.entries if k < threshold ]:
                del self.entries[k]
                continue
        pass

    def __sample(self, k, tup, mtr, fmt, func, attrs, gcount_name, gsum_name):
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
        for an, vspec in attrs.items():
            ## The first element of vspec is a format string,
            ## containing len(vspec)-1 format specifiers.  The
            ## remaining elements are functions to be supplied with
            ## details of the entry being rendered.  The functions'
            ## values are used to fulfil the format specifiers.
            lval = vspec[0] % tuple([ af(tup, entry) for af in vspec[1:] ])

            ## Create a name-value pair to form a label.  TODO: Escape
            ## the value.  TODO: Sanity-check the name.
            labels.append(('%s="%s"') % (an, lval))
            continue

        msg = ''

        if callable(fmt):
            ## Treat the extracted value as histogram data, and
            ## convert it into a dict of 'sum' (the sum of the events'
            ## values), 'count' (the number of events), with the
            ## remaining keys being int/float thresholds giving the
            ## cummulative contents of each bucket.  The first entry
            ## must be 0.
            value = fmt(value)

            ## Extract the summary data.
            gsum = value['sum']
            gcount = value['count']

            ## Extract and sort the thresholds.
            thrs = [ thr for thr in value.keys()
                     if isinstance(thr, (int,float)) ]
            thrs.sort()

            ## Generate the buckets.
            for thr in thrs:
                msg += mtr + "_bucket"
                msg += '{'
                msg += ','.join(labels + [ 'le="%g"' % thr ])
                msg += '}'
                msg += (' %d %.3f\n') % (value[thr], k)
                continue

            ## The +inf bucket and the count are the same.
            msg += mtr + "_bucket"
            msg += '{'
            msg += ','.join(labels + [ 'le="+inf"' ])
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

        gcount_name = 'gcount' if typ == 'gaugehistogram' else 'count'
        gsum_name = 'gsum' if typ == 'gaugehistogram' else 'sum'

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
                                         fmt, func, attrs,
                                         gcount_name, gsum_name)
                    continue
                continue
            continue

        return msg

    def get_message(self, ident):
        """Get the latest data for a given client, in OpenMetrics format.  A
        timestamp is recorded for each client, and only data newer
        than this timestamp is returned.  The timestamp is then
        updated to the most recent metric point just delivered,
        preventing metrics from being retransmitted.

        """
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

        return (msg, ts, latest)

    def check(self):
        """Check whether this history has been terminated."""
        with self.lock:
            return self.running
        pass

    def halt(self):
        """Terminate this history."""
        with self.lock:
            self.running = False
            pass
        pass

    pass


class MetricsHTTPHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, hist=None, prebody=None, **kwargs):
        self.hist = hist
        self.prebody = prebody
        super().__init__(*args, **kwargs)
        pass

    def do_GET(self):
        ## Identify the client by the authorization string.
        auth = self.headers.get('Authorization')
        if auth is None:
            auth = 'anonymous'
            pass

        ## Fetch the message appropriate to the client, and send it.
        print('  Forming message for %s' % auth)
        body, ts0, ts1 = self.hist.get_message(auth)

        ## Prefix the body with additional content, if a provider is
        ## specified.
        if callable(self.prebody):
            prebody = self.prebody()
            body = prebody + body
            pass

        ## Send the complete response.
        self.send_response(200)
        ct = 'application/openmetrics-text'
        ct += '; version=1.0.0; charset=utf-8'
        self.send_header('Content-Type', ct)
        self.end_headers()
        self.wfile.write(body.encode('UTF-8'))
        print('  Complete %d-%d' % (ts1, ts1 - ts0))
        pass

    pass
