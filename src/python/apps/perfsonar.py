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
import logging
import traceback
from lancs_gridmon.trees import merge_trees

_tozero = re.compile(r"[0-9]")

def _nicetime(t):
    return datetime.utcfromtimestamp(t).strftime('%Y-%m-%dT%H:%M:%SZ')

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


def _match_host(got, sought):
    import socket, ipaddress
    gotaddr = ipaddress.ip_address(got)
    for ent in socket.getaddrinfo(sought, 0):
        if ipaddress.ip_address(ent[4][0]) == gotaddr:
            return True
        continue
    return False

class PerfsonarCollector:
    known_events = set([ 'throughput', 'packet-count-lost',
                         'packet-count-sent', 'histogram-owdelay',
                         'histogram-ttl' ])

    unknown_events = set([ 'packet-retransmits-subintervals', 'failures',
                           'packet-duplicates', 'packet-reorders',
                           'packet-retransmits', 'packet-trace', 'path-mtu',
                           'pscheduler-run-href', 'throughput-subintervals',
                           'time-error-estimates', 'packet-loss-rate' ])

    def __init__(self, endpoint, lag=20, fore=0, aft=60, forced_host=None):
        self.endpoint = endpoint
        self.lag = lag
        self.fore = fore
        self.aft = aft
        self.last = int(time.time()) - lag - aft
        self.start = self.last
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE
        self.counters = { }
        self.headers = dict()
        if forced_host is not None:
            self.headers['Host'] = forced_host
            pass
        pass

    def update(self):
        ## Determine the time range we are adding.
        curr = int(time.time()) - self.lag - self.aft
        if curr <= self.last:
            return { }
        assert curr > self.last
        start = self.last + 1
        interval = "time-start=%d&time-end=%d" % (start, curr)

        ## Consider a wider range for identifying (metadata-key,
        ## event-type) tuples that might provide data in the smaller
        ## range, even if no summary data is present.
        scan_start = start - self.fore
        scan_end = curr + self.aft
        scan = "time-start=%d&time-end=%d" % (scan_start, scan_end)

        logging.info('scan0: %10d (%s)' % (scan_start, _nicetime(scan_start)))
        logging.info('read0: %10d (%s)' % (start, _nicetime(start)))
        logging.info('read1: %10d (%s)' % (curr, _nicetime(curr)))
        logging.info('scan1: %10d (%s)' % (scan_end, _nicetime(scan_end)))

        ## Get the summary of measurements within the interval.
        url = self.endpoint + "?" + scan
        logging.debug('root scan %s' % url)
        rtreq = urllib.request.Request(url, headers=self.headers)
        rsp = urllib.request.urlopen(rtreq, context=self.ctx)
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
            if _match_host(meta['measurement-agent'], meta['input-source']):
                meta['measurement-peer'] = meta['destination']
                meta['input-measurement-peer'] = meta['input-destination']
                meta['input-measurement-agent'] = meta['input-source']
                logging.debug('%s peer is destination' % mdkey)
            elif _match_host(meta['measurement-agent'],
                             meta['input-destination']):
                meta['measurement-peer'] = meta['source']
                meta['input-measurement-peer'] = meta['input-source']
                meta['input-measurement-agent'] = meta['input-destination']
                logging.debug('%s peer is source' % mdkey)
            else:
                logging.warning('%s has no peer' % mdkey)
                continue

            baseurl = mdent['url']
            for evt in mdent['event-types']:
                ## Skip event types which we do not use.
                evtype = evt.get('event-type')
                if evtype not in PerfsonarCollector.known_events:
                    if evtype not in PerfsonarCollector.unknown_events:
                        logging.info('%s %s: skipped' % (mdkey, evtype))
                    else:
                        logging.debug('%s %s: skipped' % (mdkey, evtype))
                        pass
                    continue

                ## Ensure a counter exists for this event type and
                ## metadata key.
                self.counters.setdefault(mdkey, { }) \
                    .setdefault(evtype, 0)

                ## Fetch the event data, using the narrower interval
                ## that doesn't overlap with the previous or
                ## subsequent interval.
                evturl = urljoin(baseurl, evt['base-uri']) + '?' + interval
                logging.debug('%s get event %s' % (mdkey, evturl))
                evtreq = urllib.request.Request(evturl, headers=self.headers)
                evtrsp = urllib.request.urlopen(evtreq, context=self.ctx)
                evtdoc = json.loads(evtrsp.read().decode("utf-8"))

                ## Convert the event data to a dict indexed by
                ## timestamp.
                evdic = { int(ev['ts']): ev['val'] for ev in evtdoc }

                ## Ensure we have the timestamps in order.
                evtss = [ ts for ts in evdic ]
                evtss.sort()

                for ts in evtss:
                    val = evdic[ts]

                    logging.info('%+3d %+3d %s/%s' %
                                 (ts - scan_start,
                                  ts - start,
                                  mdkey, evtype))

                    ## Install metadata and the value for this event.
                    tsdata = data.setdefault(ts, { })
                    evdata = tsdata.setdefault(mdkey, { })
                    merge_trees(evdata, meta)
                    evdata.setdefault('measurements', { })[evtype] = val

                    ## Increase the relevant event counter, and store
                    ## under this timestamp.
                    self.counters[mdkey][evtype] += 1
                    evc += 1
                    cnt = self.counters[mdkey][evtype]
                    evdata.setdefault('counters', { })['start'] = self.start
                    evdata['counters'][evtype] = cnt
                    # print('%s:%s@%10d=%d' % (mdkey, evtype, ts, cnt))
                    continue
                continue
            continue
        logging.info("  %d keys; %d events" % (kc, evc))

        self.last = curr
        return data

    pass

if __name__ == "__main__":
    from http.server import HTTPServer
    from getopt import getopt
    import threading
    import errno
    import os
    import signal

    ## Local libraries
    import lancs_gridmon.metrics as metrics

    http_host = "localhost"
    http_port = 8732
    silent = False
    endpoint = None
    metrics_endpoint = None
    horizon = 60 * 30
    lag = 20
    fore = 0
    aft = 60
    pidfile = None
    forced_host = None
    log_params = {
        'format': '%(asctime)s %(levelname)s %(message)s',
        'datefmt': '%Y-%m-%dT%H:%M:%S',
    }
    opts, args = getopt(sys.argv[1:], "zh:t:T:E:M:S:l:f:a:H:",
                        [ 'log=', 'log-file=', 'pid-file=' ])
    for opt, val in opts:
        if opt == '-h':
            horizon = int(val) * 60
        elif opt == '-z':
            silent = True
        elif opt == '-l':
            lag = int(val)
        elif opt == '-H':
            forced_host = val
        elif opt == '-f':
            fore = int(val)
        elif opt == '-a':
            aft = int(val)
        elif opt == '-T':
            http_host = val
        elif opt == '-t':
            http_port = int(val)
        elif opt == '-E':
            endpoint = val
        elif opt == '-M':
            metrics_endpoint = val
        elif opt == '--log':
            log_params['level'] = getattr(logging, val.upper(), None)
            if not isinstance(log_params['level'], int):
                sys.stderr.write('bad log level [%s]\n' % val)
                sys.exit(1)
                pass
            pass
        elif opt == '--log-file':
            log_params['filename'] = val
        elif opt == '--pid-file':
            if not val.endswith('.pid'):
                sys.stderr.write('pid filename %s must end with .pid\n' % val)
                sys.exit(1)
                pass
            pidfile = val
        elif opt == '-S':
            endpoint = 'https://' + val + '/esmond/perfsonar/archive/'
            pass
        continue

    if silent:
        with open('/dev/null', 'w') as devnull:
            fd = devnull.fileno()
            os.dup2(fd, sys.stdout.fileno())
            os.dup2(fd, sys.stderr.fileno())
            pass
        pass

    logging.basicConfig(**log_params)
    if 'filename' in log_params:
        def handler(signum, frame):
            logging.root.handlers = []
            logging.basicConfig(**log_params)
            logging.info('rotation')
            pass
        signal.signal(signal.SIGHUP, handler)
        pass

    methist = metrics.MetricHistory(schema, horizon=horizon)
    perfcoll = PerfsonarCollector(endpoint, lag=lag, fore=fore, aft=aft,
                                  forced_host=forced_host)
    if metrics_endpoint is None:
        hist = methist
    else:
        hist = metrics.RemoteMetricsWriter(endpoint=metrics_endpoint,
                                           schema=schema,
                                           job='perfsonar',
                                           expiry=10*60)
        pass

    ## Serve the history on demand.  Even if we don't store anything
    ## in the history, the HELP, TYPE and UNIT strings are exposed,
    ## which doesn't seem to be possible with remote-write.
    partial_handler = functools.partial(metrics.MetricsHTTPHandler, hist=methist)
    try:
        webserver = HTTPServer((http_host, http_port), partial_handler)
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            sys.stderr.write('Stopping: address in use: %s:%d\n' % \
                             (http_host, http_port))
        else:
            logging.error(traceback.format_exc())
            pass
        sys.exit(1)
        pass

    try:
        if pidfile is not None:
            with open(pidfile, "w") as f:
                f.write('%d\n' % os.getpid())
                pass
            pass

        ## Use a separate thread to run the server, which we can stop by
        ## calling shutdown().
        srv_thrd = threading.Thread(target=HTTPServer.serve_forever,
                                    args=(webserver,),
                                    daemon=True)
        srv_thrd.start()

        def check_delay(hist, start):
            if not hist.check():
                return False
            now = int(time.time())
            delay = start - now
            return delay > 0

        def keep_polling(hist, coll):
            try:
                while hist.check():
                    logging.info('Getting latest data')
                    start = int(time.time()) + 30
                    new_data = coll.update()
                    hist.install(new_data)
                    logging.info('Installed')
                    while check_delay(hist, start):
                        time.sleep(1)
                        pass
                    continue
            except InterruptedError:
                pass
            except KeyboardInterrupt:
                pass
            except Exception as e:
                logging.error(traceback.format_exc())
                pass
            logging.info('Polling halted')
            pass

        try:
            keep_polling(hist, perfcoll)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(traceback.format_exc())
            sys.exit(1)
            pass

        methist.halt()
        logging.info('Halted history')
        webserver.server_close()
        logging.info('Server stopped.')
    finally:
        if pidfile is not None:
            os.remove(pidfile)
            pass
        pass
    pass
