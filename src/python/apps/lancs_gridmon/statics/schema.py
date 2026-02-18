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

from lancs_gridmon.metrics import keys as metric_keys, walk as metric_walk

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
        'base': 'ip_bonding',
        'help': 'relationship between master and slave in bonding',
        'type': 'gauge',
        'select': metric_keys('node', 1, 'iface', 1, 'slaves', 1),
        'samples': {
            '': 1,
        },
        'attrs': {
            'node': ('%s', lambda t, d: t[0]),
            'device': ('%s', lambda t, d: t[2]),
            'master': ('%s', metric_walk('node', 1, 'iface', 1, 'device')),
        },
    },

    {
        'base': 'ip_speed_min',
        'unit': 'bits_per_second',
        'type': 'gauge',
        'help': 'minimum expected interface speed',
        'select': metric_keys('node', 1, 'iface', 1, 'speed', 'min'),
        'samples': {
            '': ('%d', metric_walk('node', 1, 'iface', 1, 'speed', 'min')),
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),
            'node': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'ip_speed_max',
        'unit': 'bits_per_second',
        'type': 'gauge',
        'help': 'maximum expected interface speed',
        'select': metric_keys('node', 1, 'iface', 1, 'speed', 'max'),
        'samples': {
            '': ('%d', metric_walk('node', 1, 'iface', 1, 'speed', 'max')),
        },
        'attrs': {
            'iface': ('%s', lambda t, d: t[1]),
            'node': ('%s', lambda t, d: t[0]),
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
