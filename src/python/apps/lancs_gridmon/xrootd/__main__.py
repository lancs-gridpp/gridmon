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

def get_config(raw_args):
    config = {
        'udp': {
            'host': '',
            'port': 9484,
        },
        'http': {
            'host': 'localhost',
            'port': 8743,
        },
        'horizon': 60 * 30,
        'silent': False,
        'endpoint': None,
        'pidfile': None,
        'pcapfile': None,
        'fake_log': '/tmp/xrootd-detail.log',
        'domain_conf': None,
        'id_timeout_min': 120,
        'log_params': {
            'format': '%(asctime)s %(levelname)s %(message)s',
            'datefmt': '%Y-%m-%dT%H:%M:%S',
        },
    }

    from getopt import gnu_getopt
    opts, args = gnu_getopt(sys.argv[1:], "zh:u:U:t:T:E:i:o:d:P:",
                            [ 'log=', 'log-file=', 'pid-file=', 'pcap=',
                              'pcap-limit=' ])
    for opt, val in opts:
        if opt == '-h':
            config['horizon'] = int(val) * 60
        elif opt == '-z':
            config['silent'] = True
        elif opt == '-i':
            config['id_timeout_min'] = int(val)
        elif opt == '-o':
            config['fake_log'] = val
        elif opt == '-d':
            config['domain_conf'] = val
        elif opt == '-P' or opt == '--pcap':
            config['pcapfile'] = val
        elif opt == '--pcap-limit':
            config['pcaplim'] = int(val)
        elif opt == '-u':
            config['udp']['port'] = int(val)
        elif opt == '-U':
            config['udp']['host'] = val
        elif opt == '-E':
            config['endpoint'] = val
        elif opt == '-t':
            config['http']['port'] = int(val)
        elif opt == '-T':
            config['http']['host'] = val
        elif opt == '--log':
            config['log_params']['level'] = getattr(logging, val.upper(), None)
            if not isinstance(config['log_params']['level'], int):
                raise RuntimeError('bad log level [%s]\n' % val)
            pass
        elif opt == '--log-file':
            config['log_params']['filename'] = val
        elif opt == '--pid-file':
            if not val.endswith('.pid'):
                raise RuntimeError('pid filename %s must end with .pid\n' % val)
            config['pidfile'] = val
        else:
            raise AssertionError('unreachable')
        continue

    return config

config = get_config(sys.argv[1:])

if config['silent']:
    apputils.silence_output()
    pass

logging.basicConfig(**config['log_params'])

epoch = 0
if config['pcapfile'] is None:
    pcapsrc = None
    now = time.time()
else:
    from lancs_gridmon.pcap import PCAPSource
    pcapsrc = PCAPSource(config['pcapfile'], config.get('pcaplim', None))
    epoch = now = pcapsrc.get_start() - 60 * 20
    pass

## Prepare to convert hostnames into domains, according to a
## configuration file that will be reloaded if its timestamp changes.
if config['domain_conf'] is None:
    domcfg = None
else:
    domcfg = lancs_gridmon.domains.WatchingDomainDeriver(config['domain_conf'])
    pass

## Prepare to process summary messages.
sum_wtr = metrics.RemoteMetricsWriter(endpoint=config['endpoint'],
                                      schema=xrootd_summary_schema,
                                      job='xrootd',
                                      expiry=10*60)
sum_proc = XRootDSummaryConverter(sum_wtr)

## Prepare to process detailed messages.
det_wtr = metrics.RemoteMetricsWriter(endpoint=config['endpoint'],
                                      schema=xrootd_detail_schema,
                                      job='xrootd_detail',
                                      expiry=10*60)
det_rec = XRootDDetailRecorder(now, config['fake_log'], det_wtr,
                               epoch=epoch)
det_proc = XRootDPeerManager(now,
                             det_rec.store_event,
                             det_rec.advance,
                             domains=domcfg,
                             epoch=epoch,
                             id_to_min=config['id_timeout_min'])

## Rotate logs on SIGHUP.  This includes the access log generated from
## the detailed monitoring.
apputils.prepare_log_rotation(config['log_params'], action=det_rec.relog)

## Receive detailed and summary messages on the same socket, and send
## them to the right processor.
msg_fltr = XRootDFilter(sum_proc.convert, det_proc.process)

if pcapsrc is None:
    udp_srv = UDPServer((config['udp']['host'], config['udp']['port']),
                        msg_fltr.datagram_handler())
else:
    pcapsrc.set_action(msg_fltr.process)
    udp_srv = pcapsrc
    pass

## Serve the combined schemata's documentation.  Use a separate
## thread.  There are no thread-safety considerations, as there is no
## shared mutable data.
www_hist = metrics.MetricHistory(xrootd_summary_schema + xrootd_detail_schema,
                                 horizon=30)
www_srv = HTTPServer((config['http']['host'], config['http']['port']),
                     www_hist.http_handler())
www_thrd = threading.Thread(target=HTTPServer.serve_forever, args=(www_srv,))

with apputils.ProcessIDFile(config['pidfile']):
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
