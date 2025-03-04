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

def _walk(root, path):
    if len(path) == 0:
        return root
    return _walk(root[path[0]], path[1:])

def _extract_user_labels(keys, data):
    res = { }
    for k, v in data['drives'][keys[0]][keys[1]].items():
        res[k] = v['fmt'] % v['value']
        continue
    return res

schema = [
    {
        'base': 'xrootd_expect',
        'help': 'metadata for an XRootD server expected to exist',
        'select': lambda e: [ (n, c, i, p) for n in e.get('node', { })
                              if 'xroot-host' in e['node'][n]
                              and 'cluster' in e['node'][n]
                              for c in e['node'][n]['cluster']
                              if 'xroots' in e['node'][n]['cluster'][c]
                              for i in e['node'][n]['cluster'][c]['xroots']
                              if 'pgms' in e['node'][n]['cluster'][c] \
                              ['xroots'][i]
                              for p in e['node'][n]['cluster'][c] \
                              ['xroots'][i]['pgms'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'cluster': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[2],
                      lambda t, d: d['node'][t[0]]['xroot-host']),
            'pgm': lambda t, d: t[3],

            ## deprecated
            'ceph_cluster': ('%s', lambda t, d: t[1]),
            # 'name': ('%s', lambda t, d: t[1]),
            # 'host': ('%s', lambda t, d: d['node'][t[0]]['xroot-host']),
        },
    },

    {
        'base': 'cluster_meta',
        'help': 'cluster metadata',
        'select': lambda e: [ (c,) for c in e.get('cluster', { })
                              if 'name' in e['cluster'][c] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'cluster': ('%s', lambda t, d: t[0]),
            'cluster_name': ('%s', lambda t, d: d['cluster'][t[0]]['name']),
        },
    },

    {
        'base': 'cluster_expect_ceph',
        'help': 'present if Ceph instance expected in cluster',
        'select': lambda e: [ (c,) for c in e.get('cluster', { })
                              if 'ceph' in e['cluster'][c]
                              and e['cluster'][c]['ceph'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'cluster': ('%s', lambda t, d: t[0]),
        },
    },

    # ## deprecated
    # {
    #     'base': 'ip_ping',
    #     'help': 'RTT to IP in ms',
    #     'type': 'gauge',
    #     'select': lambda e: [ (n, i) for n in e.get('node', { })
    #                           if 'iface' in e['node'][n]
    #                           for i in e['node'][n]['iface']
    #                           if 'rtt' in e['node'][n]['iface'][i] ],
    #     'samples': {
    #         '': ('%.3f', lambda t, d: d['node'][t[0]]['iface'][t[1]]['rtt']),
    #     },
    #     'attrs': {
    #         'iface': ('%s', lambda t, d: t[1]),

    #         ## deprecated
    #         # 'exported_instance': ('%s', lambda t, d: t[0]),
    #     },
    # },

    {
        'base': 'ssl_expiry',
        'help': 'time of SSL certificate expiry',
        'type': 'counter',
        'unit': 'seconds',
        'select': lambda e: \
        [ (n, i, p, c) for n in e.get('node', { })
          if 'iface' in e['node'][n]
          for i in e['node'][n]['iface']
          if 'ssl' in e['node'][n]['iface'][i]
          for p in e['node'][n]['iface'][i]['ssl']
          for c in e['node'][n]['iface'][i]['ssl'][p]
          if 'expiry' in e['node'][n]['iface'][i]['ssl'][p][c] ],
        'samples': {
            '': ('%.3f',
                 lambda t, d: d['node'][t[0]]['iface'][t[1]]['ssl'] \
                 [t[2]][t[3]]['expiry']),
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),
            'port': ('%s', lambda t, d: t[2]),
            'cert': ('%s', lambda t, d: t[3]),
        },
    },

    {
        'base': 'ip_ping',
        'help': 'RTT to IP',
        'type': 'gauge',
        'unit': 'milliseconds',
        'select': lambda e: [ (n, i, v) for n in e.get('node', { })
                              if 'iface' in e['node'][n]
                              for i in e['node'][n]['iface']
                              if 'proto' in e['node'][n]['iface'][i]
                              for v in e['node'][n]['iface'][i]['proto']
                              if 'rtt' in e['node'][n]['iface'][i]['proto'][v] ],
        'samples': {
            '': ('%.3f', lambda t, d: d['node'][t[0]]['iface'][t[1]] \
                 ['proto'][t[2]]['rtt']),
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),
            'proto': ('%s', lambda t, d: t[2]),
        },
    },

    {
        'base': 'ip_up',
        'help': 'whether a host is reachable',
        'type': 'gauge',
        'select': lambda e: [ (n, i, v) for n in e.get('node', { })
                              if 'iface' in e['node'][n]
                              for i in e['node'][n]['iface']
                              if 'proto' in e['node'][n]['iface'][i]
                              for v in e['node'][n]['iface'][i]['proto']
                              if 'up' in e['node'][n]['iface'][i]['proto'][v] ],
        'samples': {
            '': ('%d', lambda t, d: d['node'][t[0]]['iface'][t[1]] \
                 ['proto'][t[2]]['up']),
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),
            'proto': ('%s', lambda t, d: t[2]),
        },
    },

    {
        'base': 'ip_role',
        'help': 'the purpose of an interface within its host',
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
        'base': 'machine_alarm_load_max',
        'help': 'maximum expected load',
        'type': 'gauge',
        'select': lambda e: [ (n,) for n in e.get('node', dict())
                              if 'alarms' in e['node'][n]
                              and 'load_max' in e['node'][n]['alarms'] ],
        'samples': {
            '': ('%.3f', lambda t, d: d['node'][t[0]]['alarms']['load_max']),
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'machine_alarm_cpu_max',
        'help': 'maximum expected cpu mode fraction',
        'type': 'gauge',
        'select': lambda e: [ (n, m) for n in e.get('node', dict())
                              if 'alarms' in e['node'][n]
                              and 'cpu_max' in e['node'][n]['alarms']
                              for m in e['node'][n]['alarms']['cpu_max'] ],
        'samples': {
            '': ('%.3f', lambda t, d: d['node'][t[0]]['alarms'] \
                 ['cpu_max'][t[1]]),
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'mode': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'machine_alarm_cpu_min',
        'help': 'minimum expected cpu mode fraction',
        'type': 'gauge',
        'select': lambda e: [ (n, m) for n in e.get('node', dict())
                              if 'alarms' in e['node'][n]
                              and 'cpu_min' in e['node'][n]['alarms']
                              for m in e['node'][n]['alarms']['cpu_min'] ],
        'samples': {
            '': ('%.3f', lambda t, d: d['node'][t[0]]['alarms'] \
                 ['cpu_min'][t[1]]),
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'mode': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'machine_drive_layout',
        'help': 'which mapping from device path to physical position to use',
        'select': lambda e: [ (n,) for n in e.get('node', { })
                              if 'dloid' in e['node'][n] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'dloid': ('%s', lambda t, d: d['node'][t[0]].get('dloid')),
        },
    },

    {
        'base': 'machine_osd_drives',
        'help': 'how many drives are allocated as OSDs',
        'type': 'gauge',
        'select': lambda e: [ (n, c) for n in e.get('node', { })
                              if 'cluster' in e['node'][n]
                              for c in e['node'][n]['cluster']
                              if 'osds' in e['node'][n]['cluster'][c] ],
        'samples': {
            '': ('%d', lambda t, d: d['node'][t[0]]['cluster'][t[1]]['osds']),
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'cluster': ('%s', lambda t, d: t[1]),

            ## deprecated
            'ceph_cluster': ('%s', lambda t, d: t[1]),
            # 'exported_instance': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'machine_role',
        'help': 'a purpose of a machine',
        'type': 'gauge',
        'select': lambda e: [ (n, c, r) for n in e.get('node', { })
                              if 'static' in e['node'][n]
                              and 'cluster' in e['node'][n]
                              for c in e['node'][n]['cluster']
                              if 'roles' in e['node'][n]['cluster'][c]
                              for r in e['node'][n]['cluster'][c]['roles'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'cluster': ('%s', lambda t, d: t[1]),
            'role': ('%s', lambda t, d: t[2]),

            ## deprecated
            'ceph_cluster': ('%s', lambda t, d: t[1]),
            # 'exported_instance': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'site_subgroup_depth',
        'help': 'depth of group within another',
        'type': 'gauge',
        'select': lambda e: [ (p, c) for p in e.get('groups', { })
                              for c in e['groups'][p]['subs'] ],
        'samples': {
            '': ('%d', lambda t, d: d['groups'][t[0]]['subs'][t[1]]),
        },
        'attrs': {
            'group': ('%s', lambda t, d: t[0]),
            'subgroup': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'site_group_depth',
        'help': 'depth of site within a group',
        'type': 'gauge',
        'select': lambda e: [ (p, c) for p in e.get('groups', { })
                              for c in e['groups'][p]['sites'] ],
        'samples': {
            '': ('%d', lambda t, d: d['groups'][t[0]]['sites'][t[1]]),
        },
        'attrs': {
            'group': ('%s', lambda t, d: t[0]),
            'site': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'site_domain',
        'help': 'site ownership of domain name',
        'type': 'info',
        'select': lambda e: [ (s, d) for s in e.get('sites', { })
                              for d in e['sites'][s]['domains'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'site': ('%s', lambda t, d: t[0]),
            'domain': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'dlo_meta',
        'help': 'metadata mapping device paths to physical slots',
        'type': 'info',
        'select': lambda e: [ (lyt, path) for lyt in e.get('drives', { })
                              for path in e['drives'][lyt] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'dloid': ('%s', lambda t, d: t[0]),
            'path': ('%s', lambda t, d: t[1]),
            '': lambda t, d: _extract_user_labels(t, d),
        },
    },

    {
        'base': 'vo_meta',
        'help': 'VO metadata',
        'type': 'info',
        'select': lambda e: [ (c, vo) for c in e.get('cluster', { })
                              if 'vo' in e['cluster'][c]
                              for vo in e['cluster'][c]['vo']
                              if 'name' in e['cluster'][c]['vo'][vo] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'cluster': ('%s', lambda t, d: t[0]),
            'vo_id': ('%s', lambda t, d: t[1]),
            'vo_name': ('%s', lambda t, d: d['cluster'][t[0]] \
            ['vo'][t[1]]['name']),
        },
    },

    {
        'base': 'vo_affiliation',
        'help': 'VO member or affiliate identity',
        'type': 'info',
        'select': lambda e: ([ (c, vo, 'job_user', idx)
                               for c in e.get('cluster', { })
                               if 'vo' in e['cluster'][c]
                               for vo in e['cluster'][c]['vo']
                               if 'jobs' in e['cluster'][c]['vo'][vo]
                               and 'users' in e['cluster'][c]['vo'][vo]['jobs']
                               for idx in e['cluster'][c]['vo'][vo] \
                               ['jobs']['users'] ] +
                             [ (c, vo, 'job_account', idx)
                               for c in e.get('cluster', { })
                               if 'vo' in e['cluster'][c]
                               for vo in e['cluster'][c]['vo']
                               if 'jobs' in e['cluster'][c]['vo'][vo]
                               and 'accounts' in e['cluster'][c] \
                               ['vo'][vo]['jobs']
                               for idx in e['cluster'][c]['vo'][vo] \
                               ['jobs']['accounts'] ] +
                             [ (c, vo, 'transfer_user', idx)
                               for c in e.get('cluster', { })
                               if 'vo' in e['cluster'][c]
                               for vo in e['cluster'][c]['vo']
                               if 'transfers' in e['cluster'][c]['vo'][vo]
                               and 'users' in e['cluster'][c] \
                               ['vo'][vo]['transfers']
                               for idx in e['cluster'][c]['vo'][vo] \
                               ['transfers']['users'] ] +
                             [ (c, vo, 'cert', idx)
                               for c in e.get('cluster', { })
                               if 'vo' in e['cluster'][c]
                               for vo in e['cluster'][c]['vo']
                               if 'dns' in e['cluster'][c]['vo'][vo]
                               for idx in e['cluster'][c]['vo'][vo]['dns'] ]),
        'samples': {
            '': 1,
        },
        'attrs': {
            'cluster': ('%s', lambda t, d: t[0]),
            'vo_id': ('%s', lambda t, d: t[1]),
            'affiliation': ('%s', lambda t, d: t[2]),
            'affiliate': ('%s', lambda t, d: t[3]),
        },
    },
]

def update_live_metrics(hist, confs):
    import drives

    ## Read site and site-group specs from -f arguments.
    sites = { }
    group_specs = { }
    clus_specs = { }
    drive_specs = { }
    for arg in confs:
        with open(arg, 'r') as fh:
            doc = yaml.load(fh, Loader=yaml.SafeLoader)
            merge(sites, doc.get('sites', { }), mismatch=+1)
            merge(group_specs, doc.get('site_groups', { }), mismatch=+1)
            merge(clus_specs, doc.get('clusters', { }), mismatch=+1)
            dpts = doc.get('drive_paths', { })
            lyt_pats = drives.get_layout_patterns(dpts)
            merge(drive_specs,
                  drives.get_layouts(dpts, lyt_pats),
                  mismatch=+1)
            pass
        continue

    ## Separate group member references into sites and subgroups.
    groups = { }
    unprop = set()
    for group_name, membs in group_specs.items():
        grp = groups.setdefault(group_name, { 'sites': { }, 'subs': { } })
        for memb in membs:
            grp['sites' if memb in sites else 'subs'][memb] = 1
            unprop.add(group_name)
            continue
        continue

    ## Resolve group membership.  For every group, make all its
    ## members indirect members of every group that contains it.
    while unprop:
        for grp_name in unprop.copy():
            grp_data = groups[grp_name]
            for par_name, par_data in groups.items():
                if grp_name not in par_data['subs']:
                    continue
                ## par_name contains grp_name.
                if grp_name == par_name:
                    raise RuntimeError('group contains itself: %s' % grp_name)

                ## Propagate members of grp_name to par_name.
                for kind in [ 'sites', 'subs' ]:
                    for site, dep in grp_data[kind].items():
                        ## The depth of these propagations is one more
                        ## than what's recorded so far.
                        dep += 1

                        ## Is it already there, and at the same or
                        ## shallower depth?  If so, do nothing;
                        ## otherwise, replace with the new depth, and mark the .
                        pdep = par_data[kind].get(site)
                        if pdep is None or pdep > dep:
                            par_data[kind][site] = dep
                            unprop.add(par_name)
                            pass
                        continue
                    continue
                continue
            unprop.remove(grp_name)
            continue
        continue

    ## Create an entry for right now.
    data = { }
    nd = data[int(time.time() * 1000) / 1000.0] = { }

    ## Populate drive data.
    nd.setdefault('drives', drive_specs)

    ## Populate site data.
    sd = nd.setdefault('sites', { })
    for site_name, site_data in sites.items():
        sdom = sd.setdefault(site_name, { }).setdefault('domains', set())
        for domain in site_data.get('domains', [ ]):
            sdom.add(domain)
            continue
        continue
    for clus, cspec in clus_specs.items():
        cent = nd.setdefault('cluster', { }).setdefault(clus, { })
        if 'name' in cspec:
            cent['name'] = cspec['name']
            pass
        if 'ceph' in cspec and cspec['ceph']:
            cent['ceph'] = True
            pass
        if 'vos' in cspec:
            cent['vo'] = cspec['vos']
        continue

    ## Populate site grouping data.
    nd['groups'] = groups

    hist.install(data)
    pass

if __name__ == '__main__':
    from getopt import getopt
    import yaml
    import sys
    import os
    import signal
    import time
    import subprocess
    import re
    from pprint import pprint
    import functools
    from http.server import HTTPServer
    import threading
    import errno
    import socket

    ## Local libraries
    import metrics
    from utils import merge

    http_host = "localhost"
    http_port = 9363
    ssl_interval = 6 * 60 * 60
    silent = False
    horizon = 120
    metrics_endpoint = None
    pidfile = None
    log_params = {
        'format': '%(asctime)s %(levelname)s %(message)s',
        'datefmt': '%Y-%m-%dT%H:%M:%S',
    }
    confs = list()
    opts, args = getopt(sys.argv[1:], "zh:t:T:M:f:",
                        [ 'log=', 'log-file=', 'pid-file=' ])
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
        elif opt == '--pid-file':
            if not val.endswith('.pid'):
                sys.stderr.write('pid filename %s must end with .pid\n' % val)
                sys.exit(1)
                pass
            pidfile = val
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
    if 'filename' in log_params:
        def handler(signum, frame):
            logging.root.handlers = []
            logging.basicConfig(**log_params)
            logging.info('rotation')
            pass
        signal.signal(signal.SIGHUP, handler)
        pass

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
    updater = functools.partial(update_live_metrics, methist, confs)
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

    share_dir = os.environ['GRIDMON_SHAREDIR']
    cert_exp_cmd = os.path.join(share_dir, 'get-cert-expiry')

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

        pingfmt = re.compile(r'rtt min/avg/max/mdev = ' +
                             r'([0-9]+\.[0-9]+)/([0-9]+\.[0-9]+)/' +
                             r'([0-9]+\.[0-9]+)/([0-9]+\.[0-9]+) ms')
        tbase = int(time.time() * 1000) / 1000.0
        sslbase = tbase - ssl_interval
        try:
            while True:
                ## Read cluster/machine specs and implied roles from
                ## -f arguments.
                role_impls = { }
                mach_specs = { }
                for arg in confs:
                    with open(arg, 'r') as fh:
                        doc = yaml.load(fh, Loader=yaml.SafeLoader)
                        merge(mach_specs, doc.get('machines', { }), mismatch=+1)

                        ## Invert the machine role implications.
                        for role_so, role_ifs in doc.get('machine_roles', { }) \
                                                    .get('implied', { }).items():
                            for role_if in role_ifs:
                                role_impls.setdefault(role_if, set()) \
                                          .add(role_so)
                                continue
                            continue
                        pass
                    continue

                ## Recursively apply role implications.
                while True:
                    changed = False
                    for k, ms in role_impls.items():
                        for ak in ms.copy():
                            for v in role_impls.get(ak, set()):
                                if v not in ms:
                                    changed = True
                                    ms.add(v)
                                    pass
                                continue
                            continue
                        continue
                    if changed:
                        continue
                    break

                ## Prepare to gather metrics.
                beat = int(time.time() * 1000) / 1000.0
                logging.info('Starting sweep')
                data = { }
                data.setdefault(beat, { })['heartbeat'] = beat

                if beat - sslbase >= ssl_interval:
                    sslbase += ssl_interval
                    do_ssl = True
                else:
                    do_ssl = False
                    pass

                ## Ping all interfaces.
                for node, nspec in mach_specs.items():
                    if not nspec.get('enabled', True):
                        continue
                    for iface, sub in nspec.get('interfaces', { }).items():
                        pt = int(time.time() * 1000) / 1000.0
                        entry = data.setdefault(pt, { })
                        nent = entry.setdefault('node', { }) \
                                    .setdefault(node, { })
                        ient = nent.setdefault('iface', { }) \
                                   .setdefault(iface, { })
                        try:
                            adents = socket.getaddrinfo(iface, 0)
                        except socket.gaierror:
                            continue
                        for adent in adents:
                            if adent[1] != socket.SocketKind.SOCK_RAW:
                                continue
                            ipv = None
                            if adent[0] == socket.AddressFamily.AF_INET:
                                ipv = 4
                            elif adent[0] == socket.AddressFamily.AF_INET6:
                                ipv = 6
                            else:
                                continue
                            adstr = adent[4][0]

                            cmd = [ 'ping', '-%d' % ipv, '-c', '1',
                                    '-w', '1', adstr ]
                            logging.info('Ping v%d %s(%s) of %s' % \
                                         (ipv, iface, adstr, node))
                            logging.debug('Command: %s' % cmd)
                            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                                    universal_newlines=True)
                            lines = proc.stdout.readlines()
                            rc = proc.wait()
                            assert rc is not None
                            ipent = ient.setdefault('proto', { }) \
                                        .setdefault('ipv%d' % ipv, { })
                            if rc == 0:
                                mt = pingfmt.match(lines[-1])
                                if mt is not None:
                                    ipent['rtt'] = float(mt.group(1))
                                    ipent['up'] = 1
                                    pass
                                pass
                            elif rc == 2:
                                ## The hostname did not resolve for
                                ## the IP version.  Just ignore.
                                pass
                            else:
                                logging.debug('No pong for %s (IPv%d) of %s' %
                                              (iface, ipv, node))
                                ipent['up'] = 0
                                pass
                            continue
                        from pprint import pprint
                        if do_ssl:
                            sslent = ient.setdefault('ssl', { })
                            for sslch in sub.get('ssl_checks',
                                                 sub.get('ssl-checks', [])):
                                port = sslch.get('port')
                                port = 443 if port is None else int(port)
                                name = sslch.get('name', iface)
                                addr = iface + ':' + str(port)
                                cmd = [ cert_exp_cmd, '--connect=' + addr,
                                        '--name=' + name ]
                                logging.debug('ssl cmd: %s' % cmd)
                                proc = subprocess.Popen(cmd,
                                                        stdout=subprocess.PIPE,
                                                        universal_newlines=True)
                                lines = proc.stdout.readlines()
                                rc = proc.wait()
                                assert rc is not None
                                certent = sslent.setdefault(port, { }) \
                                    .setdefault(name, { })
                                certent['error'] = rc
                                if rc == 0 and len(lines) > 0:
                                    exptime = int(lines[0])
                                    certent['expiry'] = exptime
                                    logging.info('%s:%d (%s) = %d' %
                                                 (iface, port, name, exptime))
                                    pass
                                continue
                            pass
                        continue
                    continue

                ## Add in the static metrics.
                entry = data.setdefault(int(time.time() * 1000) / 1000.0, { })
                for node, nspec in mach_specs.items():
                    if not nspec.get('enabled', True):
                        continue

                    nent = entry.setdefault('node', { }).setdefault(node, { })

                    for k in [ 'building', 'room', 'rack', 'level' ]:
                        if k in nspec:
                            nent[k] = nspec[k]
                            pass
                        continue
                    dloid = nspec.get('drive_layout')
                    if dloid is not None:
                        nent['dloid'] = dloid
                        pass

                    nent['alarms'] = nspec.get('alarms', dict())

                    for clus, cspec in nspec.get('clusters', { }).items():
                        if 'osds' in cspec:
                            nent.setdefault('cluster', { }) \
                                .setdefault(clus, { })['osds'] = cspec['osds']
                            pass
                        for role in cspec.get('roles', [ ]):
                            rent = nent.setdefault('cluster', { }) \
                                       .setdefault(clus, { }) \
                                       .setdefault('roles', set())
                            rent.add(role)
                            for oth in role_impls.get(role, [ ]):
                                rent.add(oth)
                                continue
                            continue
                        continue

                    nent['static'] = True

                    ## Get sets of interfaces with specific roles.  Also, copy
                    ## other attributes.
                    iroles = { }
                    for iface, sub in nspec.get('interfaces', { }).items():
                        for role in sub.get('roles', [ ]):
                            iroles.setdefault(role, set()).add(iface)
                            continue
                        ient = nent.setdefault('iface', { }) \
                                   .setdefault(iface, { })
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
                        for clus, cspec in nspec.get('clusters', { }).items():
                            xrds = nent.setdefault('cluster', { }) \
                                       .setdefault(clus, { }) \
                                       .setdefault('xroots', { })
                            for xname in cspec.get('xroots', { }):
                                xrds.setdefault(xname, { }) \
                                    .setdefault('pgms', set()).add('xrootd')
                                continue
                            for xname in cspec.get('cmses', { }):
                                xrds.setdefault(xname, { }) \
                                    .setdefault('pgms', set()).add('cmsd')
                                continue
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
    finally:
        if pidfile is not None:
            os.remove(pidfile)
            pass
        pass
    pass
