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
import time
from datetime import datetime
import ssl
import urllib.request
from urllib.parse import urljoin
import json
import sys
import re

_tozero = re.compile(r"[0-9]")

def _bucket(thr, cnt):
    ## Make all the digits '0', but the last '1'.
    global _tozero
    mval = _tozero.sub('0', thr)
    ival = mval[0:-1] + '1'

    ## Create a tuple of the original key, its lower threshold, its
    ## upper threshold, and count.
    lwr = float(thr)
    upr = lwr + float(ival)
    return (thr, lwr, upr, cnt)

def _histo(rbucks, mean=lambda lwr, upr: (lwr + upr) / 2.0):
    ## Work out upper thresholds of each bucket.  Also compute sums.
    global _tozero
    uppers = { }
    lowers = { }
    gsum = 0.0
    gcount = 0
    seq = [ ]
    for thr, cnt in rbucks.items():
        tup = _bucket(thr, cnt)
        seq.append(tup)
        gcount += cnt
        gsum += cnt * mean(tup[1], tup[2])
        continue

    ## Sort the keys by lower threshold.
    seq.sort(key=lambda tup: tup[1])

    ## Look for holes and overlaps.
    lupr = None
    extra = [ ]
    for thr, lwr, upr, cnt in seq:
        ## Detect and fill in a gap.
        if lupr is None or tup[1] > lupr:
            extra += ((lwr, 0),)
            pass

        ## Include ourselves.
        extra += ((upr, cnt),)

        ## Remember the previous upper threshold for comparison with
        ## the next tuple's lower.
        lupr = upr
        continue

    ## Accumulate.
    tot = 0
    acc = []
    for upr, cnt in extra:
        tot += cnt
        acc += ((upr, tot),)
        continue

    ## Convert to a dict giving the upper thresholds, and include the
    ## summary data.
    result = { upr: cnt for upr, cnt in acc }
    result['sum'] = gsum
    result['count'] = gcount
    return result

schema = [
    {
        'base': 'perfsonar_packets_lost',
        'type': 'gauge',
        'help': 'packets lost',
        'select': lambda e : [ (t,) for t in e
                               if 'measurements' in e[t]
                               and 'packet-count-lost' in e[t]['measurements'] ],
        'samples': {
            '': ('%d',
                 lambda t, d: d[t[0]]['measurements']['packet-count-lost']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_packets_sent',
        'type': 'gauge',
        'help': 'packets sent',
        'select': lambda e : [ (t,) for t in e
                               if 'measurements' in e[t]
                               and 'packet-count-sent' in e[t]['measurements'] ],
        'samples': {
            '': ('%d',
                 lambda t, d: d[t[0]]['measurements']['packet-count-sent']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_events_packets',
        'type': 'counter',
        'help': 'number of packet-loss measurements',
        'select': lambda e : [ (t,) for t in e
                               if 'counters' in e[t]
                               and 'packet-count-sent' in e[t]['counters'] ],
        'samples': {
            '_total': ('%d',
                       lambda t, d: d[t[0]]['counters']['packet-count-sent']),
            '_created': ('%.3f',
                         lambda t, d: d[t[0]]['counters']['start']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_throughput',
        'unit': 'bps',
        'type': 'gauge',
        'help': 'throughput',
        'select': lambda e : [ (t,) for t in e
                               if 'measurements' in e[t]
                               and 'throughput' in e[t]['measurements'] ],
        'samples': {
            '': ('%d',
                 lambda t, d: d[t[0]]['measurements']['throughput']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_events_throughput',
        'type': 'counter',
        'help': 'number of throughput measurements',
        'select': lambda e : [ (t,) for t in e
                               if 'counters' in e[t]
                               and 'throughput' in e[t]['counters'] ],
        'samples': {
            '_total': ('%d',
                       lambda t, d: d[t[0]]['counters']['throughput']),
            '_created': ('%.3f',
                         lambda t, d: d[t[0]]['counters']['start']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_owdelay',
        'unit': 'ms',
        'type': 'gaugehistogram',
        'help': 'one-way delay',
        'select': lambda e : [ (t,) for t in e
                               if 'measurements' in e[t]
                               and 'histogram-owdelay' in e[t]['measurements'] ],
        'samples': {
            '': (_histo,
                 lambda t, d: d[t[0]]['measurements']['histogram-owdelay']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_events_owdelay',
        'type': 'counter',
        'help': 'number of one-way delay measurements',
        'select': lambda e : [ (t,) for t in e
                               if 'counters' in e[t]
                               and 'histogram-owdelay' in e[t]['counters'] ],
        'samples': {
            '_total': ('%d',
                       lambda t, d: d[t[0]]['counters']['histogram-owdelay']),
            '_created': ('%.3f',
                         lambda t, d: d[t[0]]['counters']['start']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_ttl',
        'type': 'gaugehistogram',
        'help': 'remaining time-to-live',
        'select': lambda e : [ (t,) for t in e
                               if 'measurements' in e[t]
                               and 'histogram-ttl' in e[t]['measurements'] ],
        'samples': {
            '': (functools.partial(_histo, mean=lambda a, b: a),
                 lambda t, d: d[t[0]]['measurements']['histogram-ttl']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_events_ttl',
        'type': 'counter',
        'help': 'number of TTL measurements',
        'select': lambda e : [ (t,) for t in e
                               if 'counters' in e[t]
                               and 'histogram-ttl' in e[t]['counters'] ],
        'samples': {
            '_total': ('%d',
                       lambda t, d: d[t[0]]['counters']['histogram-ttl']),
            '_created': ('%.3f',
                         lambda t, d: d[t[0]]['counters']['start']),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'perfsonar_metadata',
        'type': 'info',
        'help': 'measurement metadata',
        'select': lambda e : [ (t,) for t in e ],
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
            'src_addr': ('%s', lambda t, d: d[t[0]]['source']),
            'dst_addr': ('%s', lambda t, d: d[t[0]]['destination']),
            'src_name': ('%s', lambda t, d: d[t[0]]['input-source']),
            'dst_name': ('%s', lambda t, d: d[t[0]]['input-destination']),
            'agent_addr': ('%s', lambda t, d: d[t[0]]['measurement-agent']),
            'peer_addr': ('%s', lambda t, d: d[t[0]]['measurement-peer']),
            'agent_name': ('%s',
                           lambda t, d: d[t[0]]['input-measurement-agent']),
            'peer_name': ('%s', lambda t, d: d[t[0]]['input-measurement-peer']),
            'tool': ('%s', lambda t, d: d[t[0]]['tool-name']),
            'subj_type': ('%s', lambda t, d: d[t[0]]['subject-type']),
            'psched_type': ('%s', lambda t, d: d[t[0]]['pscheduler-test-type']),
        },
    },

    {
        'base': 'perfsonar_ip_metadata',
        'type': 'info',
        'help': 'IP measurement metadata',
        'select': lambda e : [ (t,) for t in e
                               if 'ip-transport-protocol' in e[t]
                               and e[t]['ip-transport-protocol'] is not None ],
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'metadata_key': ('%s', lambda t, d: t[0]),
            'ip_transport_proto':
            ('%s', lambda t, d: d[t[0]]['ip-transport-protocol']),
        },
    },
]


def _merge(a, b, pfx=()):
    for key, nv in b.items():
        ## Add a value if not already present.
        if key not in a:
            a[key] = nv
            continue

        ## Compare the old value with the new.  Apply recursively if
        ## they are both dictionaries.
        ov = a[key]
        if isinstance(ov, dict) and isinstance(nv, dict):
            _merge(ov, nv, pfx + (key,))
            continue

        ## The new value and the existing value must match.
        if ov != nv:
            raise Exception('bad merge (%s over %s at %s)' %
                            (nv, ov, '.'.join(pfx + (key,))))

        continue
    pass

class PerfsonarCollector:
    known_events = set([ 'throughput', 'packet-count-lost',
                         'packet-count-sent', 'histogram-owdelay',
                         'histogram-ttl' ])

    unknown_events = set([ 'packet-retransmits-subintervals', 'failures',
                           'packet-duplicates', 'packet-reorders',
                           'packet-retransmits', 'packet-trace', 'path-mtu',
                           'pscheduler-run-href', 'throughput-subintervals',
                           'time-error-estimates', 'packet-loss-rate' ])

    def __init__(self, endpoint, lag=20):
        self.endpoint = endpoint
        self.lag = lag
        self.last = int(time.time()) - lag
        self.start = self.last
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE
        self.counters = { }
        pass

    def update(self):
        ## Determine the time range we are adding.
        curr = int(time.time()) - self.lag
        if curr <= self.last:
            return { }
        assert curr > self.last
        start = self.last + 1
        interval = "time-start=%d&time-end=%d" % (start, curr)
        print('%3ds from %10d (%s) to %10d (%s)' %
              (curr - start + 1,
               start,
               datetime.utcfromtimestamp(start).strftime('%Y-%m-%dT%H:%M:%SZ'),
               curr,
               datetime.utcfromtimestamp(curr).strftime('%Y-%m-%dT%H:%M:%SZ')))

        ## Get the summary of measurements within the interval.
        url = self.endpoint + "?" + interval
        rsp = urllib.request.urlopen(url, context=self.ctx)
        doc = json.loads(rsp.read().decode("utf-8"))

        ## Get data for mentioned events.
        data = { }
        kc = 0
        evc = 0
        for mdent in doc:
            ## Extract metadata.
            mdkey = mdent['metadata-key']
            kc += 1
            meta = { k: mdent.get(k) or None
                     for k in ('source', 'destination', 'input-source',
                               'input-destination', 'measurement-agent',
                               'tool-name', 'subject-type',
                               'pscheduler-test-type',
                               'ip-transport-protocol') }
            if meta['source'] == meta['measurement-agent']:
                meta['measurement-peer'] = meta['destination']
                meta['input-measurement-peer'] = meta['input-destination']
                meta['input-measurement-agent'] = meta['input-source']
            else:
                meta['measurement-peer'] = meta['source']
                meta['input-measurement-peer'] = meta['input-source']
                meta['input-measurement-agent'] = meta['input-destination']
                pass

            baseurl = mdent['url']
            for evt in mdent['event-types']:
                ## Skip events that haven't happened yet.
                upd = evt.get('time-updated')
                if upd is None:
                    continue

                ## The overview can contain event types that haven't
                ## been updated in ages (because a different event
                ## type for the same metadata key *has* been updated.
                ## We have to skip over these.
                if upd < start or upd > curr:
                    continue
                evc += 1

                ## Skip event types which we do not use.
                evtype = evt.get('event-type')
                if evtype not in PerfsonarCollector.known_events:
                    if evtype not in PerfsonarCollector.unknown_events:
                        print('%s %s: skipped' % (mdkey, evtype))
                        pass
                    continue

                ## Ensure a counter exists for this event type and
                ## metadata key.
                self.counters.setdefault(mdkey, { }) \
                    .setdefault(evtype, 0)

                ## Fetch the event data.
                evturl = urljoin(baseurl, evt['base-uri']) + '?' + interval
                evtrsp = urllib.request.urlopen(evturl, context=self.ctx)
                evtdoc = json.loads(evtrsp.read().decode("utf-8"))

                ## Convert the event data to a dict indexed by
                ## timestamp.
                evdic = { int(ev['ts']): ev['val'] for ev in evtdoc }

                ## Ensure we have the timestamps in order.
                evtss = [ ts for ts in evdic ]
                evtss.sort()

                for ts in evtss:
                    val = evdic[ts]

                    ## Install metadata and the value for this event.
                    tsdata = data.setdefault(ts, { })
                    evdata = tsdata.setdefault(mdkey, { })
                    _merge(evdata, meta)
                    evdata.setdefault('measurements', { })[evtype] = val

                    ## Increase the relevant event counter, and store
                    ## under this timestamp.
                    self.counters[mdkey][evtype] += 1
                    cnt = self.counters[mdkey][evtype]
                    evdata.setdefault('counters', { })['start'] = self.start
                    evdata['counters'][evtype] = cnt
                    # print('%s:%s@%10d=%d' % (mdkey, evtype, ts, cnt))
                    continue
                continue
            continue
        print("  %d keys; %d events" % (kc, evc))

        self.last = curr
        return data

    pass

if __name__ == "__main__":
    coll = PerfsonarCollector(sys.argv[1])
    time.sleep(5)
    data = coll.update()
    print(data)
    time.sleep(5)
    data = coll.update()
    print(data)
    time.sleep(5)
    data = coll.update()
    print(data)
    pass
