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

import time
import ssl
import urllib.request
from urllib.parse import urljoin
import json
import sys

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
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.last = int(time.time())
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE
        pass

    def update(self):
        ## Determine the time range we are adding.
        curr = int(time.time())
        interval = "time-start=%d&time-end=%d" % (self.last + 1, curr)

        ## Get the summary of measurements within the interval.
        url = self.endpoint + "?" + interval
        rsp = urllib.request.urlopen(url, context=self.ctx)
        doc = json.loads(rsp.read().decode("utf-8"))

        ## Get data for mentioned events.
        data = { }
        for mdent in doc:
            ## Extract metadata.
            mdkey = mdent['metadata-key']
            meta = { k: mdent.get(k) or None
                     for k in ('source', 'destination', 'input-source',
                               'input-destination', 'measurement-agent',
                               'tool-name', 'subject-type',
                               'pscheduler-test-type',
                               'ip-transport-protocol') }

            baseurl = mdent['url']
            for evt in mdent['event-types']:
                ## Skip events that haven't happened yet.
                upd = evt.get('time-updated')
                if upd is None:
                    continue

                evtype = evt.get('event-type')

                ## Fetch the event data.
                evturl = urljoin(baseurl, evt['base-uri']) + '?' + interval
                evtrsp = urllib.request.urlopen(evturl, context=self.ctx)
                evtdoc = json.loads(evtrsp.read().decode("utf-8"))

                for ev in evtdoc:
                    ts = int(ev['ts'])
                    val = ev['val']

                    ## Install metadata and the value for this event.
                    tsdata = data.setdefault(ts, { })
                    evdata = tsdata.setdefault(mdkey, { })
                    _merge(evdata, meta)
                    evdata.setdefault('measurements', { })[evtype] = val
                    continue
                continue
            continue

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