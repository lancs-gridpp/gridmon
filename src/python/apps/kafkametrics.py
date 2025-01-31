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

import re
import sys
import logging
import traceback
from getopt import gnu_getopt
import yaml
import logging
import functools
import hashlib
import pathlib
from datetime import datetime
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
    'datefmt': '%Y-%m-%dT%H:%M:%S',
}

http_host = "localhost"
http_port = 8567

config = { }

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
            for q, newqconf in doc.get('queues', { }).items():
                qconf = config.setdefault(q, {
                    'bootstrap': set(),
                    'topics': set(),
                    'group': None,
                })
                qconf['bootstrap'].update(newqconf.get('bootstrap', []))
                qconf['topics'].update(newqconf.get('topics', []))
                qconf['group'] = newqconf.get('group')
                rec_spec = newqconf.get('record')
                if rec_spec is not None:
                    rec_dir = rec_spec.get('path')
                    if rec_dir is not None:
                        rec_dir = os.path.expanduser(rec_dir)
                        qconf['record'] = { 'path': pathlib.Path(rec_dir) }
                        rec_excl = rec_spec.get('exclude')
                        rec_excl = None if rec_excl is None \
                            else re.compile(rec_excl)
                        qconf['record']['exclude'] = rec_excl
                        pass
                    pass
                continue
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
    'reset': time.time(),
    'queues': {
        name: {
            'up': False,
            'conns': 0,
            'topics': dict({ t: {
                'key_bytes': 0,
                'value_bytes': 0,
                'count': 0,
            } for t in conf['topics'] }),
        } for name, conf in config.items()
    },
}

def update_live_metrics(hist, stats, lock):
    data = { }

    with lock:
        data['reset'] = stats['reset']
        for name, stat in stats['queues'].items():
            dat = data.setdefault('queues', { }).setdefault(name, { })
            dat['up'] = stat['up']
            dat['conns'] = stat['conns']
            utils.merge(dat.setdefault('topics', { }), stat['topics'])
            continue
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
        'select': lambda e: [ (q, t, e['reset'])
                              for q in e['queues']
                              for t in e['queues'][q]['topics'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d['queues'][t[0]] \
                       ['topics'][t[1]]['key_bytes']),
            '_created': ('%.3f', lambda t, d: t[2]),
        },
        'attrs': {
            'topic': ('%s', lambda t, d: t[1]),
            'queue': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'kafka_value_volume',
        'type': 'counter',
        'unit': 'bytes',
        'select': lambda e: [ (q, t, e['reset'])
                              for q in e['queues']
                              for t in e['queues'][q]['topics'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d['queues'][t[0]] \
                       ['topics'][t[1]]['value_bytes']),
            '_created': ('%.3f', lambda t, d: t[2]),
        },
        'attrs': {
            'topic': ('%s', lambda t, d: t[1]),
            'queue': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'kafka_messages',
        'type': 'counter',
        'select': lambda e: [ (q, t, e['reset'])
                              for q in e['queues']
                              for t in e['queues'][q]['topics'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d['queues'][t[0]] \
                       ['topics'][t[1]]['count']),
            '_created': ('%.3f', lambda t, d: t[2]),
        },
        'attrs': {
            'topic': ('%s', lambda t, d: t[1]),
            'queue': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'kafka_up',
        'type': 'gauge',
        'select': lambda e: [ (q, e['queues'][q]['up'])
                              for q in e['queues'] ],
        'samples': {
            '': ('%d', lambda t, d: 1 if t[1] else 0),
        },
        'attrs': {
            'queue': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'kafka_connections',
        'type': 'counter',
        'select': lambda e: [ (q, e['queues'][q]['conns'], e['reset'])
                              for q in e['queues']
                              if 'conns' in e['queues'][q] ],
        'samples': {
            '_total': ('%d', lambda t, d: t[1]),
            '_created': ('%.3f', lambda t, d: t[2]),
        },
        'attrs': {
            'queue': ('%s', lambda t, d: t[0]),
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

def listen_to_kafka(queue, conf, stats, stats_lock):
    topics = list(conf['topics'])
    boot = list(conf['bootstrap'])
    group_id = conf['group']
    rec_spec = conf['record']
    if rec_spec is not None:
        rec_dir = rec_spec['path']
        rec_excl = rec_spec['exclude']
        m = '%s recorded in %s'
        t = (queue, str(rec_dir))
        if rec_excl is not None:
            m += ' excluding %s'
            t += (rec_excl.pattern,)
            pass
        logging.info(m % t)
        rec_dir.mkdir(exist_ok=True, parents=False)
    else:
        rec_dir = None
        rec_excl = None
        pass

    while True:
        try:
            restart_time = time.time()
            with stats_lock:
                stats['conns'] += 1
                pass
            logging.info('conn(%s) as %s\n' % (boot, group_id))
            cons = KafkaConsumer(*topics,
                                 bootstrap_servers=boot,
                                 group_id=group_id)
            with stats_lock:
                stats['up'] = True
                pass
            logging.debug('consuming %s on %s as %s' % (topics, queue, group_id))
            for msg in cons:
                topic = msg.topic
                if rec_dir is not None and \
                   (msg.key is not None or msg.value is not None) \
                   and (rec_excl is None or not rec_excl.match(topic)):
                    ## Create a way to distinctly identify this
                    ## message.  Use the timestamp and a digest of key
                    ## and value.
                    tst = datetime.utcfromtimestamp(msg.timestamp / 1000.0) \
                                  .isoformat('T', 'milliseconds') \
                                  .replace(':', '-')
                    digin = bytes()
                    if msg.key is not None:
                        digin += msg.key
                        pass
                    if msg.value is not None:
                        digin += msg.value
                        pass
                    dig = hashlib.md5(digin).hexdigest()[0:4]
                    if msg.key is not None:
                        op = rec_dir.joinpath(topic, f'{tst}.{dig}.key')
                        op.parent.mkdir(parents=True, exist_ok=True)
                        with op.open('wb') as f:
                            f.write(msg.key)
                            pass
                        pass
                    if msg.value is not None:
                        op = rec_dir.joinpath(topic, f'{tst}.{dig}.value')
                        op.parent.mkdir(parents=True, exist_ok=True)
                        with op.open('wb') as f:
                            f.write(msg.value)
                            pass
                        pass
                    pass
                keybytes = 0 if msg.key is None else len(msg.key)
                valuebytes = 0 if msg.value is None else len(msg.value)
                logging.debug('got %d:%d of %s on %s as %s' % \
                              (keybytes, valuebytes, topic, queue, group_id))
                with stats_lock:
                    stats['topics'][topic]['key_bytes'] += keybytes
                    stats['topics'][topic]['value_bytes'] += valuebytes
                    stats['topics'][topic]['count'] += 1
                    pass
                continue
        except ke.NoBrokersAvailable:
            fail_time = time.time()
            restart_delay = restart_time + 30 - fail_time
            if restart_delay > 0:
                with stats_lock:
                    stats['up'] = False
                    pass

                ## Try again, but not necessarily straight away.
                time.sleep(restart_delay)
                continue
        break
    pass

try:
    if pidfile is not None:
        with open(pidfile, "w") as f:
            f.write('%d\n' % os.getpid())
            pass
        pass

    ## Start a thread for each queue.
    for queue, qconf in config.items():
        rec_dir = qconf.get('record', {}).get('path')
        rec_excl = qconf.get('record', {}).get('exclude')
        if rec_dir is not None:
            rec_dir.mkdir(exist_ok=True)
            pass
        thrd = threading.Thread(target=listen_to_kafka,
                                args=(queue, qconf,
                                      stats['queues'][queue], stats_lock),
                                daemon=True)
        thrd.start()
        continue

    webserver.serve_forever()
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
    if pidfile is not None:
        os.remove(pidfile)
        pass
    pass
pass
