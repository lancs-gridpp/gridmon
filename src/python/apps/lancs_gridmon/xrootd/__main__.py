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

import os
import sys
import logging
import threading
import signal
import functools
import time
import socket
from socketserver import UDPServer
from http.server import HTTPServer

import lancs_gridmon.metrics as metrics
import lancs_gridmon.apps as apputils
from lancs_gridmon.vos import WatchingVODatabase
from lancs_gridmon.xrootd.udpqueue import UDPQueuer
from lancs_gridmon.xrootd.summary.conversion \
    import MetricConverter as XRootDSummaryConverter
from lancs_gridmon.xrootd.detail.management \
    import PeerManager as XRootDPeerManager
from lancs_gridmon.xrootd.detail.recordings \
    import Recorder as XRootDDetailRecorder
from lancs_gridmon.xrootd.filter import XRootDFilter
from lancs_gridmon.xrootd.detail import schema as xrootd_detail_schema
from lancs_gridmon.xrootd.summary import schema as xrootd_summary_schema
import lancs_gridmon.domains

def normalize_path(obj, key):
    from os.path import expanduser as expand_user_path
    if key not in obj:
        return False
    p = obj.get(key, None)
    if p is None:
        return False
    p = expand_user_path(p)
    obj[key] = p
    return True

def convert_duration(obj, key, *keys):
    from lancs_gridmon.timing import parse_duration
    pfx = [ key ] + list(keys)
    tail = pfx[-1]
    pfx = pfx[:-1]
    for p in pfx:
        if p not in obj:
            return False
        obj = obj[p]
        continue
    if tail not in obj:
        return False
    obj[tail] = parse_duration(obj[tail])
    return True

def convert_memory(obj, key, *keys):
    from lancs_gridmon.memory import parse_memory
    pfx = [ key ] + list(keys)
    tail = pfx[-1]
    pfx = pfx[:-1]
    for p in pfx:
        if p not in obj:
            return False
        obj = obj[p]
        continue
    if tail not in obj:
        return False
    obj[tail] = parse_memory(obj[tail])
    return True

def get_config(raw_args):
    config = {
        'instance': 'default',
        'source': {
            'xrootd': {
                'host': '',
                'port': 9484,
                'queue': {
                    'path': '~/.local/var/spool/xrootd-monitor/{instance}/queue',
                    'chunk_size': '1M',
                    'ram_size': '1M',
                },
            },
            'pcap': {
                'filename': None,
                'limit': None,
                'args': list(),
            },
        },
        'destination': {
            'scrape': {
                'host': 'localhost',
                'port': 8743,
            },
            'push': {
                'endpoint': None,
                'summary_job': 'xrootd',
                'detail_job': 'xrootd_detail',
                'labels': dict(),
            },
            'log': '/tmp/xrootd-detail-{instance}.log',
        },
        'process': {
            'silent': False,
            'id_filename': None,
            'log': apputils.default_log_config(),
        },
        'data': {
            'organizations': {
                'filename': None,
                'counter_limit': 1000,
            },
            'horizon': '5m',
            'purge': '15m',
            'fake_port': None,
            'peers': {
                'timeout': '20m',
            },
            'dictids': {
                'timeout': '2h',
                'short_timeout': '2m',
            },
            'sequencing': {
                "window": 240,
                "timeout": '750ms',
            },
            'domains': {
                'filename': None,
            },
        },
    }

    ## Parse command-line arguments and load configuration.
    import yaml
    from lancs_gridmon.trees import merge_trees, expand_vars
    from getopt import gnu_getopt
    opts, args = gnu_getopt(raw_args, "zh:u:U:t:T:E:i:o:d:P:n:c:",
                            [ 'log=', 'log-file=', 'pid-file=', 'pcap=',
                              'pcap-limit=', 'fake-port=' ])

    ## Treat all plain arguments as YAML files to be loaded and
    ## merged.
    for arg in args:
        with open(arg, 'r') as fh:
            loaded = yaml.load(fh, Loader=yaml.SafeLoader)
            pass
        merge_trees(config, loaded, mismatch=+1)
        continue

    ## Load in optional configuration files before overriding with
    ## other command-line arguments.
    for opt, val in opts:
        if opt == '-c':
            try:
                with open(val, 'r') as fh:
                    loaded = yaml.load(fh, Loader=yaml.SafeLoader)
                    pass
                merge_trees(config, loaded, mismatch=+1)
            except FileNotFoundError:
                pass
            pass
        continue

    ## Override with in-line options.
    for opt, val in opts:
        if opt == '-h':
            config['data']['horizon'] = val
        elif opt == '-z':
            config['process']['silent'] = True
        elif opt == '-i':
            config['data']['dictids']['timeout'] = val
        elif opt == '-n':
            config['instance'] = val
        elif opt == '-c':
            ## already handled
            pass
        elif opt == '-o':
            config['destination']['log'] = val
        elif opt == '-d':
            config['data']['domains']['filename'] = val
        elif opt == '-P' or opt == '--pcap':
            config['source']['pcap']['filename'] = val
        elif opt == '--pcap-limit':
            config['pcaplim'] = int(val)
        elif opt == '--fake-port':
            config['data']['fake_port'] = int(val)
        elif opt == '-u':
            config['source']['xrootd']['port'] = int(val)
        elif opt == '-U':
            config['source']['xrootd']['host'] = val
        elif opt == '-E':
            config['destination']['push']['endpoint'] = val
        elif opt == '-t':
            config['destination']['scrape']['port'] = int(val)
        elif opt == '-T':
            config['destination']['scrape']['host'] = val
        elif opt == '--log':
            config['process']['log']['level'] = val
        elif opt == '--log-file':
            config['process']['log']['filename'] = val
        elif opt == '--pid-file':
            if not val.endswith('.pid'):
                raise RuntimeError('pid filename %s must end with .pid' % val)
            config['process']['id_filename'] = val
        else:
            raise AssertionError('unreachable')
        continue

    expand_vars(config, instance=config['instance'])

    convert_memory(config, 'source', 'xrootd', 'queue', 'chunk_size')
    convert_memory(config, 'source', 'xrootd', 'queue', 'ram_size')
    convert_memory(config, 'source', 'xrootd', 'rcvbuf')
    convert_duration(config, 'data', 'purge')
    convert_duration(config, 'data', 'peers', 'timeout')
    convert_duration(config, 'data', 'dictids', 'timeout')
    convert_duration(config, 'data', 'dictids', 'short_timeout')
    convert_duration(config, 'data', 'sequencing', 'timeout')
    convert_duration(config, 'data', 'horizon')
    if 'level' in config['process']['log']:
        if isinstance(config['process']['log']['level'], str):
            config['process']['log']['level'] = \
                getattr(logging, config['process']['log']['level'].upper())
            pass
        if not isinstance(config['process']['log']['level'], int):
            raise RuntimeError('bad log level [%s]\n' %
                               config['process']['log']['level'])
        pass

    return config

config = get_config(sys.argv[1:])

if config['process']['silent']:
    apputils.silence_output()
    pass

normalize_path(config['source']['pcap'], 'filename')
normalize_path(config['destination'], 'log')
normalize_path(config['data']['domains'], 'filename')
normalize_path(config['process']['log'], 'filename')
normalize_path(config['process'], 'id_filename')

epoch = 0
if config['source']['pcap']['filename'] is None:
    pcapsrc = None
    now = time.time()
else:
    from lancs_gridmon.pcap import PCAPSource
    pcapsrc = PCAPSource(config['source']['pcap']['filename'],
                         config['source']['pcap']['limit'],
                         config['source']['pcap']['args'])
    epoch = now = pcapsrc.get_start() - 60 * 20
    pass

## Prepare to convert hostnames into domains, according to a
## configuration file that will be reloaded if its timestamp changes.
if config['data']['domains']['filename'] is None:
    domcfg = None
else:
    domcfg = lancs_gridmon.domains.WatchingDomainDeriver(
        config['data']['domains']['filename'])
    pass

## Map XRootD dictids, LFN path prefixes and usernames to VO names.
vo_db = WatchingVODatabase(**config['data']['organizations'])

## Prepare to process summary messages.
sum_wtr = metrics.RemoteMetricsWriter(
    endpoint=config['destination']['push']['endpoint'],
    schema=xrootd_summary_schema,
    job=config['destination']['push']['summary_job'],
    labels=config['destination']['push']['labels'],
    expiry=10*60)
sum_proc = XRootDSummaryConverter(sum_wtr)

## Prepare to process detailed messages.
det_wtr = metrics.RemoteMetricsWriter(
    endpoint=config['destination']['push']['endpoint'],
    schema=xrootd_detail_schema,
    job=config['destination']['push']['detail_job'],
    labels=config['destination']['push']['labels'],
    expiry=10*60)
det_rec = XRootDDetailRecorder(now, config['destination']['log'], det_wtr,
                               epoch=epoch,
                               horizon=config['data']['horizon'])

det_proc = XRootDPeerManager(now,
                             det_rec.store_event,
                             det_rec.advance,
                             domains=domcfg,
                             epoch=epoch,
                             vo_db=vo_db,
                             fake_port=config['data']['fake_port'],
                             id_to=config['data']['dictids']['timeout'],
                             id_sto=config['data']['dictids']['short_timeout'],
                             peer_to=config['data']['peers']['timeout'],
                             purge=config['data']['purge'],
                             seq_to=config['data']['sequencing']['timeout'],
                             seq_win=config['data']['sequencing']['window'])

## Rotate logs on SIGHUP.  This includes the access log generated from
## the detailed monitoring.
apputils.prepare_log_rotation(config['process']['log'], action=det_rec.relog)

## Receive detailed and summary messages on the same socket, and send
## them to the right processor.
msg_fltr = XRootDFilter(sum_proc.convert, det_proc.process)


if pcapsrc is None:
    udp_qdir = os.path.expanduser(config['source']['xrootd']['queue']['path'])
    udp_q = \
        UDPQueuer(udp_qdir,
                  chunk_size=config['source']['xrootd']['queue']['chunk_size'],
                  ram_size=config['source']['xrootd']['queue']['ram_size'],
                  dest=msg_fltr.process)
    udp_srv = UDPServer((config['source']['xrootd']['host'],
                         config['source']['xrootd']['port']),
                        udp_q.handler())
    udp_srv.max_packet_size = 65536
    if 'rcvbuf' in config['source']['xrootd']:
        rcvbuf = config['source']['xrootd']['rcvbuf']
        udp_srv.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        rcvbuf = udp_srv.socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        logging.info('rcvbuf set to %d' % rcvbuf)
        pass

    ## Make sure SIGTERM gracefull shuts down the UDP processing.
    is_termed = False
    udp_term = threading.Thread(target=UDPServer.shutdown, args=(udp_srv,))
    def on_term(signum, frame):
        global udp_q, is_termed, udp_term
        if is_termed:
            logging.info('duplicate sigterm ignored')
            return
        is_termed = True
        logging.info('terminating by signal')
        if udp_q is not None:
            udp_q.halt()
            pass
        logging.info('queue terminated')
        ## We must call shutdown() in a different thread.
        udp_term.start()
        logging.info('socket asked to shut down')
        pass
    signal.signal(signal.SIGTERM, on_term)
else:
    udp_q = None
    pcapsrc.set_action(msg_fltr.process)
    udp_srv = pcapsrc
    pass

def update_live_metrics(start_time, pmgr, hist):
    now = (time.time() * 1000) // 1000
    data = dict()
    data[now] = {
        'meta': {
            'start': start_time,
        },
    }
    pmgr.aggregate(data[now]['meta'])

    import resource
    ru = resource.getrusage(resource.RUSAGE_SELF)
    data[now]['meta']['rusage'] = dict()
    for k in [ 'utime', 'stime', 'maxrss', 'ixrss', 'idrss', 'isrss',
               'minflt', 'majflt', 'nswap', 'inblock', 'oublock',
               'msgsnd', 'msgrcv', 'nsignals', 'nvcsw', 'nivcsw' ]:
        v = getattr(ru, 'ru_' + k, None)
        if v is not None:
            data[now]['meta']['rusage'][k] = v
            pass
        continue

    import psutil
    ps = psutil.Process().memory_info()
    data[now]['meta']['psutil'] = { 'mem': dict() }
    for k in [ 'rss', 'vms', 'shared', 'text', 'lib', 'data', 'dirty' ]:
        v = getattr(ps, k, None)
        if v is not None:
            data[now]['meta']['psutil']['mem'][k] = v / 1024
            pass
        continue

    hist.install(data)
    pass

from lancs_gridmon.metrics import keys as metric_keys, walk as metric_walk

meta_schema = [
    {
        'base': 'xrootd_collector_rusage_utime',
        'type': 'counter',
        'help': 'user-mode execution time',
        'unit': 'seconds',
        'select': metric_keys('meta', 'rusage', 'utime'),
        'samples': {
            '_total': ('%.3f', metric_walk('meta', 'rusage', 'utime')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_stime',
        'type': 'counter',
        'help': 'kernel-mode execution time',
        'unit': 'seconds',
        'select': metric_keys('meta', 'rusage', 'stime'),
        'samples': {
            '_total': ('%.3f', metric_walk('meta', 'rusage', 'stime')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_maxrss',
        'type': 'counter',
        'help': 'maximum resident set size',
        'unit': 'kilobytes',
        'select': metric_keys('meta', 'rusage', 'maxrss'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'maxrss')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_psutil_mem_rss',
        'type': 'gauge',
        'help': 'current resident set size',
        'unit': 'kilobytes',
        'select': metric_keys('meta', 'psutil', 'mem', 'rss'),
        'samples': {
            '': ('%d', metric_walk('meta', 'psutil', 'mem', 'rss')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_minflt',
        'type': 'counter',
        'help': 'page faults serviced without I/O',
        'select': metric_keys('meta', 'rusage', 'minflt'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'minflt')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_majflt',
        'type': 'counter',
        'help': 'page faults serviced requiring I/O',
        'select': metric_keys('meta', 'rusage', 'majflt'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'majflt')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_inblock',
        'type': 'counter',
        'help': 'filesytem input events',
        'select': metric_keys('meta', 'rusage', 'inblock'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'inblock')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_oublock',
        'type': 'counter',
        'help': 'filesytem output events',
        'select': metric_keys('meta', 'rusage', 'oublock'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'oublock')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_msgsnd',
        'type': 'counter',
        'help': 'messages sent',
        'select': metric_keys('meta', 'rusage', 'msgsnd'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'msgsnd')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_msgrcv',
        'type': 'counter',
        'help': 'messages received',
        'select': metric_keys('meta', 'rusage', 'msgrcv'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'msgrcv')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_nsignals',
        'type': 'counter',
        'help': 'signals received',
        'select': metric_keys('meta', 'rusage', 'nsignals'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'nsignals')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_nswap',
        'type': 'counter',
        'help': 'swap-outs',
        'select': metric_keys('meta', 'rusage', 'nswap'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'nswap')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_nvcsw',
        'type': 'counter',
        'help': 'voluntary context switches',
        'select': metric_keys('meta', 'rusage', 'nvcsw'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'nvcsw')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_rusage_nivcsw',
        'type': 'counter',
        'help': 'involuntary context switches',
        'select': metric_keys('meta', 'rusage', 'nivcsw'),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'rusage', 'nivcsw')),
        },
        'attrs': dict(),
    },

    {
        'base': 'xrootd_collector_start_time',
        'type': 'counter',
        'help': 'start time of collector',
        'unit': 'seconds',
        'select': lambda e: [ tuple() ],
        'samples': {
            '_total': ('%.3f', metric_walk('meta', 'start')),
        },
        'attrs': { },
    },

    {
        'base': 'xrootd_collector_fake_port_overrides',
        'type': 'counter',
        'help': 'number of times redirection port was distinctly overridden',
        'select': lambda e: [ tuple() ],
        'samples': {
            '_total': ('%d', metric_walk('meta', 'fake_port_overrides')),
        },
        'attrs': { },
    },

    {
        'base': 'xrootd_collector_sequencing',
        'type': 'counter',
        'help': 'resequencing events for detailed reports',
        'select': metric_keys('meta', 'sequencing', 2),
        'samples': {
            '_total': ('%d', metric_walk('meta', 'sequencing', 2)),
        },
        'attrs': {
            'stream': ('%s', lambda t, d: t[0]),
            'event': ('%s', lambda t, d: t[1]),
        },
    },
]

## Serve the combined schemata's documentation.  Use a separate
## thread.  There are no thread-safety considerations, as there is no
## shared mutable data.
www_hist = metrics.MetricHistory(xrootd_summary_schema + \
                                 xrootd_detail_schema + \
                                 meta_schema,
                                 horizon=30)
www_updater = functools.partial(update_live_metrics, now, det_proc,
                                www_hist)
www_srv = HTTPServer((config['destination']['scrape']['host'],
                      config['destination']['scrape']['port']),
                     www_hist.http_handler(prescrape=www_updater))
www_thrd = threading.Thread(target=HTTPServer.serve_forever, args=(www_srv,))

with apputils.ProcessIDFile(config['process']['id_filename']):
    www_thrd.start()
    if udp_q is not None:
        udp_q.start()
        pass
    logging.info('starting')
    det_rec.start()
    try:
        udp_srv.serve_forever()
        det_rec.advance_to_clear()
    except KeyboardInterrupt:
        pass
    logging.info('stopping')
    if udp_q is not None:
        udp_q.halt()
        pass
    www_hist.halt()
    www_srv.shutdown()
    www_srv.server_close()
    
    pass
