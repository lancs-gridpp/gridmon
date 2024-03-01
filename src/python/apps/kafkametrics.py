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

import sys
import logging
import traceback
from getopt import gnu_getopt
import yaml
import logging
import functools
from http.server import HTTPServer
import threading
import os
import signal
import time

from kafka import KafkaConsumer
import kafka.errors as ke

import metrics
import utils

horizon = 30 * 60
silent = False
pidfile = None
log_params = {
    'format': '%(asctime)s %(levelname)s %(message)s',
    'datefmt': '%Y-%d-%mT%H:%M:%S',
}

http_host = "localhost"
http_port = 8567

boot = set()
topics = set()
group = 'monitor'

opts, args = gnu_getopt(sys.argv[1:], 'h:f:T:t:z',
                        [ 'log=', 'log-file=', 'pid-file='])
for opt, val in opts:
    if opt == '-h':
        horizon = int(val)
    elif opt == '-z':
        silent = True
    elif opt == '-f':
        with open(val, 'r') as fh:
            doc = yaml.load(fh, Loader=yaml.SafeLoader)
            boot.update(doc.get('bootstrap', []))
            topics.update(doc.get('topics', []))
            group = doc.get('group', group)
            pass
    elif opt == '-T':
        http_host = val
    elif opt == '-t':
        http_port = int(val)
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
        pass
    continue

boot = ', '.join(boot)
topics = list(topics)


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


stats_lock = threading.Lock()
stats = {
    'up': False,
    'reset': time.time(),
    'topics': dict({ t: {
        'key_bytes': 0,
        'value_bytes': 0,
        'count': 0,
    } for t in topics }),
}

def update_live_metrics(hist, stats, lock):
    data = { }

    with lock:
        data['up'] = stats['up']
        data['reset'] = stats['reset']
        utils.merge(data.setdefault('topics', { }), stats['topics'])
        pass

    ## Record this data as almost immediate metrics.
    now = int(time.time() * 1000) / 1000
    rec = { now: data }
    hist.install(rec)
    pass

schema = [
    {
        'base': 'kafka_key_volume',
        'type': 'counter',
        'unit': 'bytes',
        'select': lambda e: [ (t, e['reset']) for t in e['topics'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d['topics'][t[0]]['key_bytes']),
            '_created': ('%.3f', lambda t, d: t[1]),
        },
        'attrs': {
            'topic': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'kafka_value_volume',
        'type': 'counter',
        'unit': 'bytes',
        'select': lambda e: [ (t, e['reset']) for t in e['topics'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d['topics'][t[0]]['value_bytes']),
            '_created': ('%.3f', lambda t, d: t[1]),
        },
        'attrs': {
            'topic': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'kafka_events',
        'type': 'counter',
        'select': lambda e: [ (t, e['reset']) for t in e['topics'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d['topics'][t[0]]['count']),
            '_created': ('%.3f', lambda t, d: t[1]),
        },
        'attrs': {
            'topic': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'kafka_up',
        'type': 'gauge',
        'select': lambda e: [ (e['up'],) ],
        'samples': {
            '': ('%d', lambda t, d: 1 if t[0] else 0),
        },
    },
]

methist = metrics.MetricHistory(schema, horizon=horizon)
updater = functools.partial(update_live_metrics, methist, stats, stats_lock)
partial_handler = functools.partial(metrics.MetricsHTTPHandler,
                                    hist=methist,
                                    prescrape=updater)
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

    srv_thrd = threading.Thread(target=HTTPServer.serve_forever,
                                args=(webserver,),
                                daemon=True)
    srv_thrd.start()

    # key_deserializer=lambda x: x.decode('utf-8')
    # value_deserializer=lambda x: x.decode('utf-8')
    try:
        while True:
            try:
                restart_time = time.time()
                cons = KafkaConsumer(*topics,
                                     bootstrap_servers=boot,
                                     group_id=group)
                stats['up'] = True
                for msg in cons:
                    topic = msg.topic
                    keybytes = len(msg.key)
                    valuebytes = len(msg.value)
                    with stats_lock:
                        logging.debug('%s: %d/%d' %
                                      (topic, keybytes, valuebytes))
                        stats['topics'][topic]['key_bytes'] += keybytes
                        stats['topics'][topic]['value_bytes'] += valuebytes
                        stats['topics'][topic]['count'] += 1
                        pass
                    continue
            except ke.NoBrokersAvailable:
                fail_time = time.time()
                restart_delay = restart_time + 30 - fail_time
                if restart_delay > 0:
                    time.sleep(restart_delay)
                stats['up'] = False
                ## Try again.
                pass
            continue
    except InterruptedError:
        pass
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(traceback.format_exc())
        sys.exit(1)
    finally:
        logging.info('Polling halted')
        methist.halt()
        webserver.shutdown()
        pass
finally:
    if pidfile is not None:
        os.remove(pidfile)
        pass
    pass
pass
