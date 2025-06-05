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
from socketserver import UDPServer
from http.server import HTTPServer

import lancs_gridmon.metrics as metrics
import lancs_gridmon.apps as apputils
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

def get_config(raw_args):
    config = {
        'source': {
            'xrootd': {
                'host': '',
                'port': 9484,
            },
            'pcap': {
                'filename': None,
                'limit': None,
            },
        },
        'destination': {
            'scrape': {
                'host': 'localhost',
                'port': 8743,
            },
            'push': {
                'endpoint': None,
            },
            'log': '/tmp/xrootd-detail.log',
        },
        'process': {
            'silent': False,
            'id_filename': None,
            'log': {
                'format': '%(asctime)s %(levelname)s %(message)s',
                'datefmt': '%Y-%m-%dT%H:%M:%S',
            },
        },
        'data': {
            'horizon': '30m', ## in use?
            'fake_port': None,
            'dictids': {
                'timeout': '2h',
            },
            'domains': {
                'filename': None,
            },
        },
    }

    ## Parse command-line arguments and load configuration.
    import yaml
    from lancs_gridmon.trees import merge_trees
    from getopt import gnu_getopt
    opts, args = gnu_getopt(raw_args, "zh:u:U:t:T:E:i:o:d:P:",
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

    ## Override with in-line options.
    for opt, val in opts:
        if opt == '-h':
            config['data']['horizon'] = val
        elif opt == '-z':
            config['process']['silent'] = True
        elif opt == '-i':
            config['data']['dictids']['timeout'] = val
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

    convert_duration(config, 'data', 'dictids', 'timeout')
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

logging.basicConfig(**config['process']['log'])

epoch = 0
if config['source']['pcap']['filename'] is None:
    pcapsrc = None
    now = time.time()
else:
    from lancs_gridmon.pcap import PCAPSource
    pcapsrc = PCAPSource(config['source']['pcap']['filename'],
                         config['source']['pcap']['limit'])
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

## Prepare to process summary messages.
sum_wtr = metrics.RemoteMetricsWriter(
    endpoint=config['destination']['push']['endpoint'],
    schema=xrootd_summary_schema,
    job='xrootd',
    expiry=10*60)
sum_proc = XRootDSummaryConverter(sum_wtr)

## Prepare to process detailed messages.
det_wtr = metrics.RemoteMetricsWriter(
    endpoint=config['destination']['push']['endpoint'],
    schema=xrootd_detail_schema,
    job='xrootd_detail',
    expiry=10*60)
det_rec = XRootDDetailRecorder(now, config['destination']['log'], det_wtr,
                               epoch=epoch)
det_proc = XRootDPeerManager(now,
                             det_rec.store_event,
                             det_rec.advance,
                             domains=domcfg,
                             epoch=epoch,
                             fake_port=config['data']['fake_port'],
                             id_to=config['data']['dictids']['timeout'])

## Rotate logs on SIGHUP.  This includes the access log generated from
## the detailed monitoring.
apputils.prepare_log_rotation(config['process']['log'], action=det_rec.relog)

## Receive detailed and summary messages on the same socket, and send
## them to the right processor.
msg_fltr = XRootDFilter(sum_proc.convert, det_proc.process)

if pcapsrc is None:
    udp_srv = UDPServer((config['source']['xrootd']['host'],
                         config['source']['xrootd']['port']),
                        msg_fltr.datagram_handler())
    udp_srv.max_packet_size = 65536
else:
    pcapsrc.set_action(msg_fltr.process)
    udp_srv = pcapsrc
    pass

## Serve the combined schemata's documentation.  Use a separate
## thread.  There are no thread-safety considerations, as there is no
## shared mutable data.
www_hist = metrics.MetricHistory(xrootd_summary_schema + xrootd_detail_schema,
                                 horizon=30)
www_srv = HTTPServer((config['destination']['scrape']['host'],
                      config['destination']['scrape']['port']),
                     www_hist.http_handler())
www_thrd = threading.Thread(target=HTTPServer.serve_forever, args=(www_srv,))

with apputils.ProcessIDFile(config['process']['id_filename']):
    www_thrd.start()
    logging.info('starting')
    try:
        udp_srv.serve_forever()
        det_rec.advance_to_clear()
    except KeyboardInterrupt:
        pass
    logging.info('stopping')
    www_hist.halt()
    www_srv.shutdown()
    www_srv.server_close()
    
    pass
