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

import logging
import traceback

schema = [
    {
        'base': 'xrootd_expect',
        'help': 'metadata for an XRootD server expected to exist',
        'select': lambda e: [ (n, i) for n in e.get('node', { })
                              if 'xroots' in e['node'][n]
                              and 'xroot-host' in e['node'][n]
                              for i in e['node'][n]['xroots'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[1],
                      lambda t, d: d['node'][t[0]]['xroot-host']),
            'pgm': 'xrootd',

            ## deprecated
            # 'name': ('%s', lambda t, d: t[1]),
            # 'host': ('%s', lambda t, d: d['node'][t[0]]['xroot-host']),
        },
    },

    ## deprecated
    {
        'base': 'ip_ping',
        'help': 'RTT to IP in ms',
        'type': 'gauge',
        'select': lambda e: [ (n, i) for n in e.get('node', { })
                              if 'iface' in e['node'][n]
                              for i in e['node'][n]['iface']
                              if 'rtt' in e['node'][n]['iface'][i] ],
        'samples': {
            '': ('%.3f', lambda t, d: d['node'][t[0]]['iface'][t[1]]['rtt']),
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),

            ## deprecated
            # 'exported_instance': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'ip_ping',
        'help': 'RTT to IP',
        'type': 'gauge',
        'unit': 'milliseconds',
        'select': lambda e: [ (n, i) for n in e.get('node', { })
                              if 'iface' in e['node'][n]
                              for i in e['node'][n]['iface']
                              if 'rtt' in e['node'][n]['iface'][i] ],
        'samples': {
            '': ('%.3f', lambda t, d: d['node'][t[0]]['iface'][t[1]]['rtt']),
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'ip_up',
        'help': 'whether a host is reachable',
        'type': 'gauge',
        'select': lambda e: [ (n, i) for n in e.get('node', { })
                              if 'iface' in e['node'][n]
                              for i in e['node'][n]['iface']
                              if 'up' in e['node'][n]['iface'][i] ],
        'samples': {
            '': ('%d', lambda t, d: d['node'][t[0]]['iface'][t[1]]['up']),
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),

            ## deprecated
            # 'exported_instance': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'ip_role',
        'help': 'whether a host is reachable',
        'type': 'gauge',
        'select': lambda e: [ (n, i, r) for n in e.get('node', { })
                              if 'static' in e['node'][n]
                              and 'iface' in e['node'][n]
                              for i in e['node'][n]['iface']
                              if 'roles' in e['node'][n]['iface'][i]
                              for r in e['node'][n]['iface'][i]['roles'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),
            'role': ('%s', lambda t, d: t[2]),
        },
    },

    {
        'base': 'ip_metadata',
        'help': 'extra info about an IP address',
        'select': lambda e: [ (n, i) for n in e.get('node', { })
                              if 'static' in e['node'][n]
                              and 'iface' in e['node'][n]
                              for i in e['node'][n]['iface'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),
            'node': ('%s', lambda t, d: t[0]),
            'device': ('%s',
                       lambda t, d:
                       d['node'][t[0]]['iface'][t[1]].get('device')),
            'network': ('%s',
                       lambda t, d:
                       d['node'][t[0]]['iface'][t[1]].get('network')),

            ## deprecated
            # 'exported_instance': ('%s', lambda t, d: t[0]),
            # 'hostname': ('%s', lambda t, d: t[1]),
            # 'building': ('%s', lambda t, d: d['node'][t[0]].get('building')),
            # 'room': ('%s', lambda t, d: d['node'][t[0]].get('room')),
            # 'rack': ('%s', lambda t, d: d['node'][t[0]].get('rack')),
            # 'level': ('%s', lambda t, d: d['node'][t[0]].get('level')),
        },
    },

    ## deprecated
    {
        'base': 'ip_heartbeat',
        'help': 'time connectivity was last tested',
        'type': 'counter',
        'select': lambda e: [ tuple() ] if 'heartbeat' in e else [],
        'samples': {
            '_total': ('%.3f', lambda t, d: d['heartbeat']),
            '_created': 0,
        },
        'attrs': { },
    },

    {
        'base': 'machine_location',
        'help': 'physical location of a machine',
        'select': lambda e: [ (n,) for n in e.get('node', { })
                              if 'static' in e['node'][n] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'building': ('%s', lambda t, d: d['node'][t[0]].get('building')),
            'room': ('%s', lambda t, d: d['node'][t[0]].get('room')),
            'rack': ('%s', lambda t, d: d['node'][t[0]].get('rack')),
            'level': ('%s', lambda t, d: d['node'][t[0]].get('level')),

            ## deprecated
            # 'exported_instance': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'machine_osd_drives',
        'help': 'how many drives are allocated as OSDs',
        'type': 'gauge',
        'select': lambda e: [ (n,) for n in e.get('node', { })
                              if 'osds' in e['node'][n] ],
        'samples': {
            '': ('%d', lambda t, d: d['node'][t[0]]['osds']),
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),

            ## deprecated
            # 'exported_instance': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'machine_role',
        'help': 'a purpose of a machine',
        'type': 'gauge',
        'select': lambda e: [ (n, r) for n in e.get('node', { })
                              if 'static' in e['node'][n]
                              and 'roles' in e['node'][n]
                              for r in e['node'][n]['roles'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'role': ('%s', lambda t, d: t[1]),

            ## deprecated
            # 'exported_instance': ('%s', lambda t, d: t[0]),
        },
    },
]

if __name__ == '__main__':
    from getopt import getopt
    import yaml
    import sys
    import os
    import time
    import subprocess
    import re
    from pprint import pprint
    import functools
    from http.server import HTTPServer
    import threading
    import errno

    ## Local libraries
    import metrics
    from utils import merge

    http_host = "localhost"
    http_port = 9363
    silent = False
    horizon = 120
    metrics_endpoint = None
    log_params = {
        'format': '%(asctime)s %(message)s',
        'datefmt': '%Y-%d-%mT%H:%M:%S',
    }
    confs = list()
    opts, args = getopt(sys.argv[1:], "zh:t:T:M:f:",
                        [ 'log=', 'log-file=' ])
    for opt, val in opts:
        if opt == '-z':
            silent = True
        elif opt == '-h':
            horizon = int(val)
        elif opt == '-f':
            confs.append(val)
        elif opt == '-T':
            http_host = val
        elif opt == '-t':
            http_port = int(val)
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


    ## Serve HTTP metric documentation.
    methist = metrics.MetricHistory(schema, horizon=horizon)
    if metrics_endpoint is None:
        hist = methist
    else:
        hist = metrics.RemoteMetricsWriter(endpoint=metrics_endpoint,
                                           schema=schema,
                                           job='statics', expiry=horizon)
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

    ## Use a separate thread to run the server, which we can stop by
    ## calling shutdown().
    srv_thrd = threading.Thread(target=HTTPServer.serve_forever,
                                args=(webserver,),
                                daemon=True)
    srv_thrd.start()

    pingfmt = re.compile(r'rtt min/avg/max/mdev = ' +
                         r'([0-9]+\.[0-9]+)/([0-9]+\.[0-9]+)/' +
                         r'([0-9]+\.[0-9]+)/([0-9]+\.[0-9]+) ms')
    tbase = time.time()
    try:
        while True:
            ## Read machine specs from -f arguments.
            specs = { }
            for arg in confs:
                with open(arg, 'r') as fh:
                    doc = yaml.load(fh, Loader=yaml.SafeLoader)
                    merge(specs, doc.get('machines', { }), mismatch=+1)
                    pass
                continue

            ## Prepare to gather metrics.
            beat = int(time.time() * 1000) / 1000.0
            logging.info('Starting sweep')
            data = { }
            data.setdefault(beat, { })['heartbeat'] = beat

            ## Ping all interfaces.
            for node, nspec in specs.items():
                if not nspec.get('enabled', True):
                    continue
                for iface, sub in nspec.get('interfaces', { }).items():
                    cmd = [ 'ping', '-c', '1', '-w', '1', iface ]
                    logging.info('Ping %s of %s' % (iface, node))
                    logging.debug('Command: %s' % cmd)
                    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                            universal_newlines=True)
                    lines = proc.stdout.readlines()
                    rc = proc.wait()
                    assert rc is not None
                    pt = int(time.time() * 1000) / 1000.0
                    entry = data.setdefault(pt, { })
                    nent = entry.setdefault('node', { }).setdefault(node, { })
                    ient = nent.setdefault('iface', { }).setdefault(iface, { })
                    if rc == 0:
                        mt = pingfmt.match(lines[-1])
                        if mt is not None:
                            ient['rtt'] = float(mt.group(1))
                            ient['up'] = 1
                            pass
                        pass
                    else:
                        logging.debug('No pong for %s of %s' % (iface, node))
                        ient['up'] = 0
                        pass
                    continue
                continue

            ## Add in the static metrics.
            entry = data.setdefault(int(time.time() * 1000) / 1000.0, { })
            for node, nspec in specs.items():
                if not nspec.get('enabled', True):
                    continue

                nent = entry.setdefault('node', { }).setdefault(node, { })

                for k in [ 'building', 'room', 'rack', 'level', 'osds' ]:
                    if k in nspec:
                        nent[k] = nspec[k]
                        pass
                    continue
                nent['roles'] = set(nspec.get('roles', [ ]))
                nent['static'] = True

                ## Get sets of interfaces with specific roles.  Also, copy
                ## other attributes.
                iroles = { }
                for iface, sub in nspec.get('interfaces', { }).items():
                    for role in sub.get('roles', [ ]):
                        iroles.setdefault(role, set()).add(iface)
                        continue
                    ient = nent.setdefault('iface', { }).setdefault(iface, { })
                    for k in [ 'network', 'device' ]:
                        if k in sub:
                            ient[k] = sub[k]
                            pass
                        continue
                    ient['roles'] = set(sub.get('roles', [ ]))
                    continue

                ## Process XRootD expectations.
                if 'xroot' in iroles:
                    nent['xroot-host'] = list(iroles['xroot'])[0]
                    xrds = nent.setdefault('xroots', set())
                    for xname in nspec.get('xroots', { }):
                        xrds.add(xname)
                        continue
                    pass
                continue

            logging.info('Sweep complete')
            hist.install(data)

            ## Wait up to a minute before the next run.
            tbase += 60
            rem = tbase - time.time()
            if rem > 0:
                time.sleep(rem)
                pass

            continue
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(traceback.format_exc())
        sys.exit(1)
        pass

    methist.halt()
    webserver.server_close()
    pass
