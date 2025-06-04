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
import traceback
import logging
import functools
from frozendict import frozendict

from http.server import BaseHTTPRequestHandler

from lancs_gridmon.trees import merge_trees

def _safe_mod(spec, idx, snapshot):
    vals = list()
    for af in spec[1:]:
        val = af(idx, snapshot)
        if val is None:
            return None
        vals.append(val)
        continue
    return spec[0] % tuple(vals)

def _safe_func(xxx):
    """Ensures that the argument is a function taking an index, a snapshot
    and a timestamp.  If it is callable, it is assumed to take the
    correct number of arguments.  Otherwise, a lambda is returned
    returning the argument.

    """
    if not callable(xxx):
        return lambda t, d: xxx
    return xxx

def _get_sample_func(xxx):
    """Ensures that the argument is a format string followed by
    functions.  If the argument is a tuple, all but the first element
    are processed with _safe_func.  Otherwise, an int is converted to
    ('%d', ...), float to ('%.3f', ...), and anything else to ('%s',
    ...), where ... is lambda t, d: xxx.

    """
    if type(xxx) is tuple:
        return (xxx[0],) + tuple([ _safe_func(i) for i in xxx[1:] ])
    elif type(xxx) is int:
        return ('%d', _safe_func(xxx))
    elif type(xxx) is float:
        return ('%.3f', _safe_func(xxx))
    else:
        return ('%s', _safe_func(xxx))
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
            merge_trees(self.entries, samples, mismatch=mismatch)

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
            if callable(vspec):
                ## vspec is to be called with the entry details, and
                ## yields multiple key-value pairs.
                for ansfx, lval in vspec(tup, entry).items():
                    if lval is not None:
                        labels.append(('%s="%s"') % (an + ansfx, lval))
                        pass
                    continue
                continue

            vspec = _get_sample_func(vspec)
            ## The first element of vspec is a format string,
            ## containing len(vspec)-1 format specifiers.  The
            ## remaining elements are functions to be supplied with
            ## details of the entry being rendered.  The functions'
            ## values are used to fulfil the format specifiers.
            lval = _safe_mod(vspec, tup, entry)

            ## Create a name-value pair to form a label.  TODO: Escape
            ## the value.  TODO: Sanity-check the name.
            if lval is not None:
                labels.append(('%s="%s"') % (an, lval))
                pass
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
                for sfx, xxx in bits.items():
                    fmt, func = _get_sample_func(xxx)
                    msg += self.__sample(k, tup, mtr + sfx,
                                         fmt, _safe_func(func), attrs,
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

    def http_handler(self, **kwargs):
        """Get an HTTP handler that serves this history."""
        return functools.partial(MetricsHTTPHandler, hist=self, **kwargs)

    pass


class MetricsHTTPHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, hist=None, prescrape=None, prebody=None, **kwargs):
        self.hist = hist
        self.prebody = prebody
        self.prescrape = prescrape
        super().__init__(*args, **kwargs)
        pass

    def do_GET(self):
        ## Identify the client by the authorization string.
        auth = self.headers.get('Authorization')
        if auth is None:
            auth = 'anonymous'
            pass

        ## If specified, perform some rapid, current population of the
        ## history.  New data should have a very recent timestamp.
        if callable(self.prescrape):
            self.prescrape()
            pass

        ## Form the message appropriate to the client, and send it.
        logging.info('Forming metrics message for %s' % auth)
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
        logging.info('Completed metrics %d-%d' % (ts1, ts1 - ts0))
        pass

    pass

## Remote-write specification?: <https://docs.google.com/document/d/1LPhVRSFkGNSuU1fBd81ulhsCPR4hkSZyyBj1SZ8fWOM/edit#>

## Snappy: <http://google.github.io/snappy/>

class RemoteMetricsWriter:
    def __init__(self, endpoint, schema, expiry=5*60, labels=dict(), **kwargs):
        self.expiry = expiry
        self.endpoint = endpoint
        self.schema = schema
        self.labels = labels
        ## Each schema entry describes a metric family, and is a dict
        ## with the following members:
        ##
        ## 'base': the base name of the metric family.  Suffixes may
        ## be appended to this to indicate different samples within
        ## the same family, as specified by the 'samples' member
        ## below.
        ##
        ## 'select': a function taking a snapshot, and yielding a list
        ## or set of series indices for time series present in the
        ## snapshot.  Each index will usually be a tuple of dict keys
        ## allowing the snapshot to be walked to reach the value, and
        ## to annotate it with various labels.  A series index is used
        ## by functions specified in the 'samples' and 'attrs' members
        ## of the metric-family description.
        ##
        ## 'samples': a dict from name suffix to a (format, function)
        ## tuple.  The format isn't used.  The function takes a series
        ## index (as returned by 'select') and a snapshot, and yields
        ## a sample point).
        ##
        ## 'attrs': a dict from label name to a (format, function)
        ## tuple.  The function takes a series index (as returned by
        ## 'select') and a snapshot, and yields a label value to be
        ## formatted by the format string.

        if 'job' in kwargs:
            self.job = kwargs['job']
            pass

        pass

    def check(self):
        return True

    def install(self, data, mismatch=0):
        ## Data is a dict with timestamps (in seconds) as keys.
        ## Values are a usually a dict hierarchy specified by the
        ## schema.  Each of these is referred to as a snapshot below.
        if len(data) == 0:
            return True

        ## Get all the timestamps in order.
        tss = [ ts for ts in data ]
        tss.sort()

        ## This will map a frozendict of labels to a dict from
        ## timestamps to sample values.
        series = { }

        ## Consider each metric family.
        lasttime = 0
        for family in self.schema:
            basename = family['base']
            unit = family.get('unit')
            sel = family['select']
            lab = family['attrs']
            sam = family['samples']
            typ = family.get('type')
            gcount_name = 'gcount' if typ == 'gaugehistogram' else 'count'
            gsum_name = 'gsum' if typ == 'gaugehistogram' else 'sum'

            if unit is not None:
                basename += '_' + unit
                pass

            for ts in tss:
                snapshot = data[ts]

                for idx in sel(snapshot):
                    ## Get the labels shared by all samples in the
                    ## family.
                    famkey = { }
                    if hasattr(self, 'job'):
                        famkey['job'] = self.job
                        pass
                    famkey.update(self.labels)
                    for labname, labspec in lab.items():
                        if labspec is callable:
                            for labsfx, labval in labspec(idx, snapshot).items():
                                if labval is not None:
                                    famkey[labname + labsfx] = labval
                                    pass
                                continue
                            continue

                        labspec = _get_sample_func(labspec)
                        labval = _safe_mod(labspec, idx, snapshot)
                        if labval is not None:
                            famkey[labname] = labval
                            pass
                        continue

                    for sfx, xxx in sam.items():
                        samfmt, samfunc = _get_sample_func(xxx)
                        ## The sample key is the family key plus a
                        ## __name__ label.  Then freeze it so it can
                        ## be used as a dict key.
                        samkey = dict(famkey)
                        samkey['__name__'] = basename + sfx
                        samkey = frozendict(samkey)

                        ## Extract the value.
                        val = _safe_func(samfunc)(idx, snapshot)

                        if callable(samfmt):
                            ## Convert the value using the function.
                            val = samfmt(val)

                            def insert(sfx, value, *args):
                                subkey = dict(samkey)
                                subkey['__name__'] += sfx
                                for k, v in args:
                                    subkey[k] = v
                                subkey = frozendict(subkey)
                                seq = series.setdefault(subkey, [ ])
                                seq.append((ts, value))
                                pass
                            for thr, thrv in val.items():
                                if isinstance(thr, (int, float)):
                                    sublab = ('le', '%g' % thr)
                                    insert('_bucket', thrv, sublab)
                                    pass
                                continue
                            ## The +inf bucket and the count are the same.
                            insert('_bucket', val['count'], ('le', '+inf'))
                            insert('_' + gcount_name, val['count'])
                            insert('_' + gsum_name, val['sum'])
                        else:
                            ## Append the timestamp and value to the
                            ## series as a tuple.  Because we already
                            ## sorted the timestamps, each time series's
                            ## values will always be added in order.
                            seq = series.setdefault(samkey, [ ])
                            seq.append((ts, val))
                            pass
                        if ts > lasttime:
                            lasttime = ts
                            pass
                        continue
                    continue
                continue
            continue

        ## Do nothing on empty data.
        if len(series) == 0:
            return True

        ## Retries are pointless after this time.
        expiry = self.expiry + lasttime

        ## Convert the timeseries into write request.
        import lancs_gridmon.metrics.remote_write_pb2 as pb

        rw = pb.WriteRequest()
        for labs, vals in series.items():
            ts = rw.timeseries.add()

            ## Append the labels in order.
            labnames = [ name for name in labs ]
            labnames.sort()
            for labname in labnames:
                le = ts.labels.add()
                le.name = labname
                le.value = labs[labname]
                continue

            ## Append the samples.  They are already in order.
            ## Convert the timestamps in seconds to integer
            ## milliseconds.
            for stamp, value in vals:
                se = ts.samples.add()
                se.value = value
                se.timestamp = int(stamp * 1000)
                continue

            continue

        if self.endpoint is None:
            print(rw)
            return True

        ## Compress using Snappy block format.
        import snappy
        body = snappy.compress(rw.SerializeToString())

        ## POST to the endpoint, including headers, and the protobuf
        ## message in Snappy block format.
        from urllib import request
        from urllib.error import URLError, HTTPError
        import random
        while True:
            try:
                req = request.Request(self.endpoint, data=body)
                req.add_header('Content-Encoding', 'snappy')
                req.add_header('Content-Type', 'application/x-protobuf')
                req.add_header('User-Agent', 'GridMon-remote-writer')
                req.add_header('X-Prometheus-Remote-Write-Version', '0.1.0')
                rsp = request.urlopen(req)
                code = rsp.getcode()
                logging.info('target %s response %d' % (self.endpoint, code))
                if code >= 500 and code <= 599:
                    now = time.time()
                    delay = min(random.randint(240, 360), expiry - now - 1)
                    if delay < 1:
                        logging.error('target %s response %d; aborting' % \
                                      (self.endpoint, code))
                        return False
                    logging.warning('target %s response %d; retrying in %ds' % \
                                    (self.endpoint, code, delay))
                    time.sleep(delay)
                    continue
                return True
            except HTTPError as e:
                logging.error('HTTP %d (%s) from target %s; aborting' %
                              (e.code, e.reason, self.endpoint))
                return False
            except URLError as e:
                now = time.time()
                delay = min(random.randint(60, 120), expiry - now - 1)
                if delay < 1:
                    logging.error('no target %s "%s"; aborting' %
                                  (self.endpoint, e.reason))
                    return False
                logging.warning('no target %s; retrying in %ds' % \
                                (self.endpoint, delay))
                time.sleep(delay)
                continue
        pass

    pass

if __name__ == '__main__':
    import sys
    from math import fmod, sin, pi
    from pprint import pprint
    sinschema = [
        {
            'base': 'sine',
            'type': 'gauge',
            'help': 'helpless',
            'select': lambda e: [ tuple() ],
            'samples': {
                '': ('%.3f', lambda t, d: d['sine']),
            },
            'attrs': {
                'pong': ('%s-%s', lambda t, d: 'yes', lambda t, d: 'no')
            },
        }
    ]
    rmw = RemoteMetricsWriter(endpoint=sys.argv[1], schema=sinschema,
                              job='sine', expiry=120)
    period = 4 * 60 - 7 # s
    resolution = 1 / 15 # Hz
    interval = 30 # s
    tbase = t0 = time.time()

    while True:
        ## Wait a while.
        time.sleep(interval)

        ## What's the end of this reporting interval?
        t1 = time.time()

        ## How much time has passed?
        delta = t0 - tbase

        ## Find a recent timestamp to report a sample.  This will
        ## actually be slightly too early (ti < t0), but we will
        ## correct for it.
        ti = t0 - fmod(delta, 1.0 / resolution)
        while ti < t0:
            ti += 1.0 / resolution
            continue

        ## Find a recent moment when the sine wave crossed zero
        ## increasing.  This ensures we don't pass a huge value to
        ## sin().
        tb = t0 - fmod(delta, period)

        ## Populate with metric points.
        data = { }
        while ti < t1:
            data.setdefault(ti, { })['sine'] = sin((ti - tb) / period * 2 * pi)
            ti += 1.0 / resolution
            continue

        pprint(data)
        rmw.install(data)

        ## Make the next action start from where we left off.
        t0 = t1
        continue
    pass
