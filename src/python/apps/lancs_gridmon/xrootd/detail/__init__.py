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

from lancs_gridmon.metrics import keys as metric_keys, walk as metric_walk

schema = [
    {
        'base': 'xrootd_redirection',
        'type': 'counter',
        'help': 'redirections',
        'select': metric_keys('detail', 3, 'redir', 4),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'redir', 4, 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'redir', 4, 'zero')),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'ip_version': ('%s', lambda t, d: t[3]),
            'protocol': ('%s', lambda t, d: t[4]),
            'redhost': ('%s', lambda t, d: t[5]),
            'redport': ('%d', lambda t, d: t[6]),
        },
    },

    {
        'base': 'xrootd_tpc',
        'type': 'counter',
        'help': 'third-party copies',
        'select': metric_keys('detail', 3, 'tpc', 7),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'tpc', 7,
                                         'count', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'tpc', 7,
                                             'count', 'zero')),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'org': ('%s', lambda t, d: t[6]),
            'streams': ('%s', lambda t, d: t[7]),
            'commander_domain': ('%s', lambda t, d: t[8]),
            'peer_domain': ('%s', lambda t, d: t[9]),
        },
    },

    {
        'base': 'xrootd_tpc_failure',
        'type': 'counter',
        'help': 'failed third-party copies',
        'select': metric_keys('detail', 3, 'tpc', 7),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'tpc', 7,
                                         'failure', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'tpc', 7,
                                             'failure', 'zero')),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'org': ('%s', lambda t, d: t[6]),
            'streams': ('%s', lambda t, d: t[7]),
            'commander_domain': ('%s', lambda t, d: t[8]),
            'peer_domain': ('%s', lambda t, d: t[9]),
        },
    },

    {
        'base': 'xrootd_tpc_success',
        'type': 'counter',
        'help': 'successful third-party copies',
        'select': metric_keys('detail', 3, 'tpc', 7),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'tpc', 7,
                                         'success', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'tpc', 7,
                                             'success', 'zero')),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'org': ('%s', lambda t, d: t[6]),
            'streams': ('%s', lambda t, d: t[7]),
            'commander_domain': ('%s', lambda t, d: t[8]),
            'peer_domain': ('%s', lambda t, d: t[9]),
        },
    },

    {
        'base': 'xrootd_tpc_volume',
        'type': 'counter',
        'unit': 'bytes',
        'help': 'volume transferred by third-party copies',
        'select': metric_keys('detail', 3, 'tpc', 7),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3,
                                         'tpc', 7, 'volume', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3,
                                             'tpc', 7, 'volume', 'zero')),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'org': ('%s', lambda t, d: t[6]),
            'streams': ('%s', lambda t, d: t[7]),
            'commander_domain': ('%s', lambda t, d: t[8]),
            'peer_domain': ('%s', lambda t, d: t[9]),
        },
    },

    {
        'base': 'xrootd_tpc_duration',
        'type': 'counter',
        'unit': 'seconds',
        'help': 'time spent on third-party copies',
        'select': metric_keys('detail', 3, 'tpc', 7),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3,
                                         'tpc', 7, 'duration', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3,
                                             'tpc', 7, 'duration', 'zero')),
        },
        'attrs': {
            'pgm': ('%s', lambda t, d: t[0]),
            'xrdid': ('%s@%s', lambda t, d: t[2], lambda t, d: t[1]),
            'direction': ('%s', lambda t, d: t[3]),
            'ip_version': ('%s', lambda t, d: t[4]),
            'protocol': ('%s', lambda t, d: t[5]),
            'org': ('%s', lambda t, d: t[6]),
            'streams': ('%s', lambda t, d: t[7]),
            'commander_domain': ('%s', lambda t, d: t[8]),
            'peer_domain': ('%s', lambda t, d: t[9]),
        },
    },

    {
        'base': 'xrootd_dictid_skip',
        'type': 'counter',
        'help': 'dictids skipped over',
        'select': metric_keys('detail', 3, 'dicts', 'skip'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3,
                                         'dicts', 'skip', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3,
                                             'dicts', 'skip', 'zero')),
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
        'select': metric_keys('detail', 3, 'dicts', 'unk', 2),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3,
                                         'dicts', 'unk', 2, 'value')),
            '_created': ('%.3f', metric_walk('detail', 3,
                                             'dicts', 'unk', 2, 'zero')),
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
        'select': metric_keys('detail', 3, 'prot', 2, 'write'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'prot', 2,
                                         'write', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'prot', 2,
                                             'write', 'zero')),
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
        'select': metric_keys('detail', 3, 'prot', 2, 'read'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'prot', 2,
                                         'read', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'prot', 2,
                                             'read', 'zero')),
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
        'select': metric_keys('detail', 3, 'prot', 2, 'readv'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'prot', 2,
                                         'readv', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'prot', 2,
                                             'readv', 'zero')),
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
        'select': metric_keys('detail', 3, 'prot', 2, 'closes'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'prot', 2,
                                         'closes', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'prot', 2,
                                             'closes', 'zero')),
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
        'select': metric_keys('detail', 3, 'prot', 2, 'forced-closes'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'prot', 2,
                                         'forced-closes', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'prot', 2,
                                             'forced-closes', 'zero')),
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
        'select': metric_keys('detail', 3, 'prot', 2, 'ip_version', 1,
                              'auth', 1, 'disconnects'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'prot', 2,
                                         'ip_version', 1, 'auth', 1,
                                         'disconnects', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'prot', 2,
                                             'ip_version', 1, 'auth', 1,
                                             'disconnects', 'zero')),
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
        'select': metric_keys('detail', 3, 'prot', 2, 'ip_version', 1,
                              'auth', 1, 'opens'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'prot', 2,
                                         'ip_version', 1, 'auth', 1,
                                         'opens', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'prot', 2,
                                             'ip_version', 1, 'auth', 1,
                                             'opens', 'zero')),
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
        'select': metric_keys('detail', 3, 'prot', 2, 'ip_version', 1,
                              'auth', 1, 'rw-opens'),
        'samples': {
            '_total': ('%d', metric_walk('detail', 3, 'prot', 2,
                                         'ip_version', 1, 'auth', 1,
                                         'rw-opens', 'value')),
            '_created': ('%.3f', metric_walk('detail', 3, 'prot', 2,
                                             'ip_version', 1, 'auth', 1,
                                             'rw-opens', 'zero')),
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
