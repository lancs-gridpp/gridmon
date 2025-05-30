## Copyright (c) 2022-2025, Lancaster University
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

schema = [
    {
        'base': 'xrootd_tpc',
        'type': 'counter',
        'help': 'third-party copies',
        'select': lambda e: [ (pgm, h, i, dire, ipv, prot, nstr, cdom, pdom)
                              for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'tpc' in e[pgm][h][i]
                              for dire in e[pgm][h][i]['tpc']
                              for ipv in e[pgm][h][i]['tpc'][dire]
                              for prot in e[pgm][h][i]['tpc'][dire][ipv]
                              for nstr in e[pgm][h][i]['tpc'][dire][ipv][prot]
                              for cdom in e[pgm][h][i]['tpc'][dire][ipv][prot] \
                              [nstr]
                              for pdom in e[pgm][h][i]['tpc'][dire][ipv][prot] \
                              [nstr][cdom] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]]['tpc'][t[3]] \
                       [t[4]][t[5]][t[6]][t[7]][t[8]]['count']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]]['tpc'][t[3]] \
                         [t[4]][t[5]][t[6]][t[7]][t[8]]['count']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'streams': ('%s', lambda t, d: t[6]),
            'commander_domain': ('%s', lambda t, d: t[7]),
            'peer_domain': ('%s', lambda t, d: t[8]),
        },
    },

    {
        'base': 'xrootd_tpc_success',
        'type': 'counter',
        'help': 'successful third-party copies',
        'select': lambda e: [ (pgm, h, i, dire, ipv, prot, nstr, cdom, pdom)
                              for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'tpc' in e[pgm][h][i]
                              for dire in e[pgm][h][i]['tpc']
                              for ipv in e[pgm][h][i]['tpc'][dire]
                              for prot in e[pgm][h][i]['tpc'][dire][ipv]
                              for nstr in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot]
                              for cdom in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot][nstr]
                              for pdom in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot][nstr][cdom] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]]['tpc'][t[3]] \
                       [t[4]][t[5]][t[6]][t[7]][t[8]]['success']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]]['tpc'][t[3]] \
                         [t[4]][t[5]][t[6]][t[7]][t[8]]['success']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'streams': ('%s', lambda t, d: t[6]),
            'commander_domain': ('%s', lambda t, d: t[7]),
            'peer_domain': ('%s', lambda t, d: t[8]),
        },
    },

    {
        'base': 'xrootd_tpc_volume',
        'type': 'counter',
        'unit': 'bytes',
        'help': 'volume transferred by third-party copies',
        'select': lambda e: [ (pgm, h, i, dire, ipv, prot, nstr, cdom, pdom)
                              for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'tpc' in e[pgm][h][i]
                              for dire in e[pgm][h][i]['tpc']
                              for ipv in e[pgm][h][i]['tpc'][dire]
                              for prot in e[pgm][h][i]['tpc'][dire][ipv]
                              for nstr in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot]
                              for cdom in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot][nstr]
                              for pdom in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot][nstr][cdom] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]]['tpc'][t[3]] \
                       [t[4]][t[5]][t[6]][t[7]][t[8]]['volume']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]]['tpc'][t[3]] \
                         [t[4]][t[5]][t[6]][t[7]][t[8]]['volume']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'streams': ('%s', lambda t, d: t[6]),
            'commander_domain': ('%s', lambda t, d: t[7]),
            'peer_domain': ('%s', lambda t, d: t[8]),
        },
    },

    {
        'base': 'xrootd_tpc_duration',
        'type': 'counter',
        'unit': 'seconds',
        'help': 'time spent on third-party copies',
        'select': lambda e: [ (pgm, h, i, dire, ipv, prot, nstr, cdom, pdom)
                              for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'tpc' in e[pgm][h][i]
                              for dire in e[pgm][h][i]['tpc']
                              for ipv in e[pgm][h][i]['tpc'][dire]
                              for prot in e[pgm][h][i]['tpc'][dire][ipv]
                              for nstr in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot]
                              for cdom in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot][nstr]
                              for pdom in e[pgm][h][i]['tpc'][dire][ipv] \
                              [prot][nstr][cdom] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]]['tpc'][t[3]] \
                       [t[4]][t[5]][t[6]][t[7]][t[8]]['duration']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]]['tpc'][t[3]] \
                         [t[4]][t[5]][t[6]][t[7]][t[8]]['duration']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'streams': ('%s', lambda t, d: t[6]),
            'commander_domain': ('%s', lambda t, d: t[7]),
            'peer_domain': ('%s', lambda t, d: t[8]),
        },
    },

    {
        'base': 'xrootd_dictid_skip',
        'type': 'counter',
        'help': 'dictids skipped over',
        'select': lambda e: [ (pgm, h, i) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'dicts' in e[pgm][h][i]
                              if 'skip' in e[pgm][h][i]['dicts'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       ['dicts']['skip']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         ['dicts']['skip']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
        },
    },

    {
        'base': 'xrootd_dictid_unknown',
        'type': 'counter',
        'help': 'undefined referenced dictids',
        'select': lambda e: [ (pgm, h, i, rec, f) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'dicts' in e[pgm][h][i]
                              if 'unk' in e[pgm][h][i]['dicts']
                              for rec in e[pgm][h][i]['dicts']['unk']
                              for f in e[pgm][h][i]['dicts']['unk'][rec] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       ['dicts']['unk'][t[3]][t[4]]['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         ['dicts']['unk'][t[3]][t[4]]['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'record': ('%s', lambda t, d: t[3]),
            'field': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_write',
        'type': 'counter',
        'unit': 'bytes',
        'help': 'bytes received per protocol, instance, domain',
        'select': lambda e: [ (pgm, h, i, pro, d) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'prot' in e[pgm][h][i]
                              for pro in e[pgm][h][i]['prot']
                              for d in e[pgm][h][i]['prot'][pro]
                              if 'write' in e[pgm][h][i]['prot'][pro][d] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       ['prot'][t[3]][t[4]]['write']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         ['prot'][t[3]][t[4]]['write']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_read',
        'type': 'counter',
        'unit': 'bytes',
        'help': 'bytes sent per protocol, instance, domain',
        'select': lambda e: [ (pgm, h, i, pro, d) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'prot' in e[pgm][h][i]
                              for pro in e[pgm][h][i]['prot']
                              for d in e[pgm][h][i]['prot'][pro]
                              if 'read' in e[pgm][h][i]['prot'][pro][d] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       ['prot'][t[3]][t[4]]['read']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         ['prot'][t[3]][t[4]]['read']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_readv',
        'type': 'counter',
        'unit': 'bytes',
        'help': 'bytes sent per protocol, instance, domain',
        'select': lambda e: [ (pgm, h, i, pro, d) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'prot' in e[pgm][h][i]
                              for pro in e[pgm][h][i]['prot']
                              for d in e[pgm][h][i]['prot'][pro]
                              if 'readv' in e[pgm][h][i]['prot'][pro][d] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       ['prot'][t[3]][t[4]]['readv']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         ['prot'][t[3]][t[4]]['readv']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_closes',
        'type': 'counter',
        'help': 'number of closes',
        'select': lambda e: [ (pgm, h, i, pro, d) for pgm in e
                              for h in e[pgm]
                              for i in e[pgm][h]
                              if 'prot' in e[pgm][h][i]
                              for pro in e[pgm][h][i]['prot']
                              for d in e[pgm][h][i]['prot'][pro]
                              if 'closes' in e[pgm][h][i]['prot'][pro][d] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       ['prot'][t[3]][t[4]]['closes']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         ['prot'][t[3]][t[4]]['closes']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_closes_forced',
        'type': 'counter',
        'help': 'number of forced closes',
        'select': lambda e: [
            (pgm, h, i, pro, d) for pgm in e
            for h in e[pgm]
            for i in e[pgm][h]
            if 'prot' in e[pgm][h][i]
            for pro in e[pgm][h][i]['prot']
            for d in e[pgm][h][i]['prot'][pro]
            if 'forced-closes' in e[pgm][h][i]['prot'][pro][d] and \
            'value' in e[pgm][h][i]['prot'][pro][d]['forced-closes']
        ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]][t[1]][t[2]] \
                       ['prot'][t[3]][t[4]]['forced-closes']['value']),
            '_created': ('%.3f', lambda t, d: d[t[0]][t[1]][t[2]] \
                         ['prot'][t[3]][t[4]]['forced-closes']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
        },
    },

    {
        'base': 'xrootd_data_disconnects',
        'type': 'counter',
        'help': 'number of disconnnects',
        'select': lambda e: [
            (pgm, h, i, pro, d, ipv, aut) for pgm in e
            for h in e[pgm]
            for i in e[pgm][h]
            if 'prot' in e[pgm][h][i]
            for pro in e[pgm][h][i]['prot']
            for d in e[pgm][h][i]['prot'][pro]
            if 'ip_version' in e[pgm][h][i]['prot'][pro][d]
            for ipv in e[pgm][h][i]['prot'][pro][d]['ip_version']
            if 'auth' in e[pgm][h][i]['prot'][pro][d]['ip_version'][ipv]
            for aut in e[pgm][h][i]['prot'][pro][d]['ip_version'][ipv]['auth']
            if 'disconnects' in e[pgm][h][i]['prot'][pro][d] \
            ['ip_version'][ipv]['auth'][aut]
        ],
        'samples': {
            '_total': ('%d',
                       lambda t, d: d[t[0]][t[1]][t[2]]['prot'][t[3]][t[4]] \
                       ['ip_version'][t[5]]['auth'][t[6]] \
                       ['disconnects']['value']),
            '_created': ('%.3f',
                         lambda t, d: d[t[0]][t[1]][t[2]]['prot'][t[3]][t[4]] \
                         ['ip_version'][t[5]]['auth'][t[6]] \
                         ['disconnects']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
            'ip_version': ('%s', lambda t, d: t[5]),
            'auth': ('%s', lambda t, d: t[6]),
        },
    },

    {
        'base': 'xrootd_data_opens',
        'type': 'counter',
        'help': 'number of opens',
        'select': lambda e: [
            (pgm, h, i, pro, d, ipv, aut) for pgm in e
            for h in e[pgm]
            for i in e[pgm][h]
            if 'prot' in e[pgm][h][i]
            for pro in e[pgm][h][i]['prot']
            for d in e[pgm][h][i]['prot'][pro]
            if 'ip_version' in e[pgm][h][i]['prot'][pro][d]
            for ipv in e[pgm][h][i]['prot'][pro][d]['ip_version']
            if 'auth' in e[pgm][h][i]['prot'][pro][d]['ip_version'][ipv]
            for aut in e[pgm][h][i]['prot'][pro][d]['ip_version'][ipv]['auth']
            if 'opens' in e[pgm][h][i]['prot'][pro][d] \
            ['ip_version'][ipv]['auth'][aut]
        ],
        'samples': {
            '_total': ('%d',
                       lambda t, d: d[t[0]][t[1]][t[2]]['prot'][t[3]][t[4]] \
                       ['ip_version'][t[5]]['auth'][t[6]] \
                       ['opens']['value']),
            '_created': ('%.3f',
                         lambda t, d: d[t[0]][t[1]][t[2]]['prot'][t[3]][t[4]] \
                         ['ip_version'][t[5]]['auth'][t[6]] \
                         ['opens']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
            'ip_version': ('%s', lambda t, d: t[5]),
            'auth': ('%s', lambda t, d: t[6]),
        },
    },

    {
        'base': 'xrootd_data_opens_rw',
        'type': 'counter',
        'help': 'number of opens for read-write',
        'select': lambda e: [
            (pgm, h, i, pro, d, ipv, aut) for pgm in e
            for h in e[pgm]
            for i in e[pgm][h]
            if 'prot' in e[pgm][h][i]
            for pro in e[pgm][h][i]['prot']
            for d in e[pgm][h][i]['prot'][pro]
            if 'ip_version' in e[pgm][h][i]['prot'][pro][d]
            for ipv in e[pgm][h][i]['prot'][pro][d]['ip_version']
            if 'auth' in e[pgm][h][i]['prot'][pro][d]['ip_version'][ipv]
            for aut in e[pgm][h][i]['prot'][pro][d]['ip_version'][ipv]['auth']
            if 'rw-opens' in e[pgm][h][i]['prot'][pro][d] \
            ['ip_version'][ipv]['auth'][aut]
        ],
        'samples': {
            '_total': ('%d',
                       lambda t, d: d[t[0]][t[1]][t[2]]['prot'][t[3]][t[4]] \
                       ['ip_version'][t[5]]['auth'][t[6]] \
                       ['rw-opens']['value']),
            '_created': ('%.3f',
                         lambda t, d: d[t[0]][t[1]][t[2]]['prot'][t[3]][t[4]] \
                         ['ip_version'][t[5]]['auth'][t[6]] \
                         ['rw-opens']['zero']),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'protocol': ('%s', lambda t, d: t[3]),
            'client_domain': ('%s', lambda t, d: t[4]),
            'ip_version': ('%s', lambda t, d: t[5]),
            'auth': ('%s', lambda t, d: t[6]),
        },
    },
]
