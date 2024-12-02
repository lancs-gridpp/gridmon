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

import subprocess
import time
import datetime
import json
import re
import sys
import traceback
import logging
from urllib import request
from urllib.parse import quote_plus
from getopt import gnu_getopt
from pprint import pprint

## Local libraries
import metrics


_pgidfmt = re.compile(r'([0-9]+)\.(.+)')
_osdfmt = re.compile(r'osd\.([0-9]+)')

def get_pool_id(pgid):
    pool, subid = _pgidfmt.match(pgid).groups()
    return pool

def get_pg_comp(args=[]):
    cmd = args + [ 'ceph', 'pg', 'dump', 'pgs_brief', '-f', 'json' ]
    try:
        logging.debug('Command: %s' % cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                universal_newlines=True)
        doc = json.loads(proc.stdout.read())
        res = { 'pgs': dict(), 'osds' : dict() }
        for ent in doc.get('pg_stats', dict()):
            pgid = ent['pgid']
            pool, subid = _pgidfmt.match(pgid).groups()
            pgent = res['pgs'].setdefault(pgid, { 'pool': pool, 'osds': dict() })
            for pfx in [ 'up', 'acting' ]:
                prim = ent[pfx + '_primary']
                for osd in ent[pfx]:
                    attrs = pgent['osds'].setdefault(osd, {
                        'up': False,
                        'up_primary': False,
                        'acting': False,
                        'acting_primary': False,
                    })
                    attrs[pfx] = True
                    if osd == prim:
                        attrs[pfx + '_primary'] = True
                        pass
                    res['osds'].setdefault(osd, dict()) \
                               .setdefault(pfx, set()) \
                               .add(pgid)
                    continue
                continue
            continue
        return res
    except KeyboardInterrupt as e:
        raise e
    except FileNotFoundError:
        logging.error('Command not found: %s' % cmd)
    except:
        logging.error(traceback.format_exc())
        logging.error('Failed to execute %s' % cmd)
        pass
    return dict()

class Summary:
    def __init__(self):
        self.total = 0.0
        self.sqtotal = 0.0
        self.count = 0
        pass

    def __iadd__(self, other):
        v = float(other)
        if self.count == 0:
            self.vmin = self.vmax = v
        elif v < self.vmin:
            self.vmin = v
        elif v > self.vmax:
            self.vmax = v
            pass
        self.total += v
        self.sqtotal += v * v
        self.count += 1
        pass

    def __str__(self):
        return '<%.6f %.6f\u00b1%.6f %.6f>' % (self.min(),
                                               self.mean(),
                                               self.standard_deviation(),
                                               self.max())

    def max(self):
        return self.vmax

    def min(self):
        return self.vmin

    def mean(self):
        return self.total / self.count

    def mean_of_squares(self):
        return self.sqtotal / self.count

    def variance(self):
        mn = self.mean()
        return self.mean_of_squares() - mn * mn

    def standard_deviation(self):
        import math
        return math.sqrt(self.variance())

    pass


schema = [
    {
        'base': 'cephpg_read_latency_mean',
        'unit': 'seconds',
        'type': 'gauge',
        'help': 'mean PG read latency',
        'select': lambda e: [ (mode, pgid)
                              for mode in e['read_latency']
                              for pgid in e['read_latency'][mode] ],
        'samples': {
            '': ('%.6f', lambda t, d: d['read_latency'][t[0]][t[1]].mean()),
        },
        'attrs': {
            'mode': ('%s', lambda t, d: t[0]),
            'pgid': ('%s', lambda t, d: t[1]),
            'pool_id': ('%s', lambda t, d: get_pool_id(t[1])),
        },
    },
]


queries = {
    'read_latency': '''
ceph:disk_installation{{cluster="{cluster}"}}
* on(node, device) group_right(ceph_daemon)
(rate(node_disk_read_time_seconds_total[{sbin}])
 /
 rate(node_disk_reads_completed_total[{sbin}]))
''',

    'write_latency': '''
ceph:disk_installation{{cluster="production"}}
* on(node, device) group_right(ceph_daemon)
(rate(node_disk_write_time_seconds_total[{sbin}])
 /
 rate(node_disk_writes_completed_total[{sbin}]))
''',

    'slow_ops': '''
ceph:disk_installation{{cluster="{cluster}"}}
* on(ceph_daemon) group_right()
ceph_daemon_health_metrics{{cluster="{cluster}", type="SLOW_OPS"}}
''',
}

if __name__ == '__main__':
    # Default command-line arguments
    cluster = None
    metrics_endpoint = None
    query_endpoint = None
    query_port = 9090
    query_host = None
    sbin = '5m'
    log_params = {
        'format': '%(asctime)s %(levelname)s %(message)s',
        'datefmt': '%Y-%m-%dT%H:%M:%S',
    }

    ## Parse arguments.  Ceph command prefix is in remaining
    ## arguments.
    opts, args = gnu_getopt(sys.argv[1:], "b:M:H:P:Q:c:",
                            [ 'log=', 'log-file=' ])
    for opt, val in opts:
        if opt == '-b':
            sbin = val
        elif opt == '-c':
            cluster = val
        elif opt == '-M':
            metrics_endpoint = val
        elif opt == '-Q':
            query_endpoint = val
            query_host = None
        elif opt == '-H':
            query_host = val
            query_endpoint = None
        elif opt == '-P':
            query_port = int(val)
        elif opt == '--log':
            log_params['level'] = getattr(logging, val.upper(), None)
            if not isinstance(log_params['level'], int):
                sys.stderr.write('bad log level [%s]\n' % val)
                sys.exit(1)
                pass
            pass
        elif opt == '--log-file':
            log_params['filename'] = val
            pass
        continue

    if cluster is None:
        sys.stderr.write('must specify -c <cluster>\n')
        sys.exit(1)
        pass

    if query_endpoint is None:
        if query_host is None:
            sys.stderr.write('must specify -H <host> or -Q <endpoint>\n')
            sys.exit(1)
            pass
        query_endpoint = f'http://{query_host}:{query_port}/api/v1/query'
        pass

    logging.basicConfig(**log_params)

    logging.info('Getting PG-OSD mapping with %s' % args)
    now = time.time()
    pgmap = get_pg_comp(args)

    logging.info('Querying %s at %.3f' % (query_endpoint, now))
    results = dict()
    for qkey, qexpr in queries.items():
        ## Form the query by injecting the cluster and bin.
        query = qexpr.format(cluster=cluster, sbin=sbin)
        logging.info('Calculating %s: %s' % (qkey, query.replace('\n', ' ')))
        url = query_endpoint + '?query=' + quote_plus(query)
        url += "&time="
        url += quote_plus(datetime.datetime.utcfromtimestamp(now)\
                          .strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
        logging.debug(f'{qkey} request: {url}')

        ## Make the request and parse the JSON response.
        req = request.Request(url)
        rsp = request.urlopen(req)
        code = rsp.getcode()
        if code != 200:
            logging.error(f'{code} from {url}')
            sys.exit(1)
            pass
        msg = json.loads(rsp.read().decode())
        stat = msg.get('status')
        if stat != 'success':
            logging.error(f'status {stat} from {url}')
            sys.exit(1)
            pass

        metric = results[qkey] = { 'up': dict(), 'acting': dict() }
        for ent in msg['data']['result']:
            osd, = _osdfmt.match(ent['metric']['ceph_daemon']).groups()
            osd = int(osd)
            val = float(ent['value'][1])
            logging.debug(f'{qkey} {osd} = {val}')
            for mode, pgs in pgmap['osds'].get(osd, dict()).items():
                tab = metric[mode]
                for pgid in pgs:
                    ent = tab.get(pgid)
                    if ent is None:
                        ent = tab[pgid] = Summary()
                        pass
                    ent += val
                    continue
                continue
            continue

        continue

    rmw = metrics.RemoteMetricsWriter(endpoint=metrics_endpoint,
                                      schema=schema,
                                      job='cephpg',
                                      labels={ 'cluster': cluster },
                                      expiry=30)
    rmw.install({ now: results })
    # pprint(results)
    pass
