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

from lancs_gridmon.metrics import keys as metric_keys, walk as metric_walk

_vofmt = re.compile(r'(?P<vo>[^/]+)(?P<attrs>.*)')
_voattrfmt = re.compile(r'/(?P<key>[^=]+)=(?P<value>[^/]+)')

def vo2vo(inp):
    result = dict()
    for vostr in inp:
        vm = _vofmt.fullmatch(vostr)
        if not vm:
            continue
        vo = vm['vo']
        ent = { 'vo': vo, 'role': '', 'group': '' }
        result[vostr] = ent
        attrsstr = vm['attrs']
        for am in _voattrfmt.finditer(attrsstr):
            k, v = am['key'], am['value']
            if k == 'Role':
                ent['role'] = v
            elif k == 'Group':
                ent['group'] = v
                pass
            continue
        continue
    return result

def ep2ep(inp, defl):
    if len(inp) == 1 and inp[0] == 'all':
        return { k: 0 for k in defl }
    return { k: 0 for k in inp }

def serving_state2code(s):
    if s == 'open':
        return 100
    return 0

def srr2data(srr):
    data = dict()
    root = srr.get('storageservice')
    if root is None:
        return data
    srv = root.get('name')
    if srv is None:
        return data
    now = root.get('latestupdate')
    if now is None:
        return data
    ev = {
        'endpoint': dict(),
        'share': dict(),
        'impl': root.get('implementation'),
        'implvers': root.get('implementationversion'),
        'quality': root.get('qualitylevel'),
    }
    data[now * 1000] = { srv: ev }

    for ent in root.get('storageendpoints', dict()):
        ev['endpoint'][ent['name']] = {
            'base': ent['endpointurl'],
            'type': ent['interfacetype'],
            'quality': ent['qualitylevel'],
        }
        continue

    for ent in root.get('storageshares', dict()):
        ev['share'][ent['name']] = {
            'capacity': ent['totalsize'],
            'used': ent['usedsize'],
            'path': { k: 0 for k in ent['path'] },
            'state': serving_state2code(ent['servingstate']),
            'vo': vo2vo(ent['vos']),
            'endpoint': ep2ep(ent['assignedendpoints'], ev['endpoint']), 
        }
        continue

    return data

schema = [
    {
        'base': 'srr_service_info',
        'type': 'info',
        'help': 'metadata for SRR services',
        'select': metric_keys(1),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'srv': ('%s', lambda t, d: t[0]),
            'srv_type': ('%s', metric_walk(1, 'impl')),
            'srv_vers': ('%s', metric_walk(1, 'implvers')),
            'srv_qual': ('%s', metric_walk(1, 'quality')),
        },
    },

    {
        'base': 'srr_endpoint_info',
        'type': 'info',
        'help': 'metadata for SRR endpoints',
        'select': metric_keys(1, 'endpoint', 1),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'srv': ('%s', lambda t, d: t[0]),
            'ep': ('%s', lambda t, d: t[1]),
            'base': ('%s', metric_walk(1, 'endpoint', 1, 'base')),
            'ep_type': ('%s', metric_walk(1, 'endpoint', 1, 'type')),
            'ep_qual': ('%s', metric_walk(1, 'endpoint', 1, 'quality')),
        },
    },

    {
        'base': 'srr_share_path_info',
        'type': 'info',
        'help': 'metadata for SRR endpoints',
        'select': metric_keys(1, 'share', 1, 'path', 1),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'srv': ('%s', lambda t, d: t[0]),
            'share': ('%s', lambda t, d: t[1]),
            'path': ('%s', lambda t, d: t[2]),
        },
    },

    {
        'base': 'srr_share_capacity',
        'type': 'gauge',
        'unit': 'bytes',
        'help': 'allocated capacity for SRR share',
        'select': metric_keys(1, 'share', 1),
        'samples': {
            '': ('%d', metric_walk(1, 'share', 1, 'capacity')),
        },
        'attrs': {
            'srv': ('%s', lambda t, d: t[0]),
            'share': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'srr_share_usage',
        'type': 'gauge',
        'unit': 'bytes',
        'help': 'used capacity for SRR share',
        'select': metric_keys(1, 'share', 1),
        'samples': {
            '': ('%d', metric_walk(1, 'share', 1, 'used')),
        },
        'attrs': {
            'srv': ('%s', lambda t, d: t[0]),
            'share': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'srr_share_state',
        'type': 'gauge',
        'help': 'serving state of SRR share',
        'select': metric_keys(1, 'share', 1),
        'samples': {
            '': ('%d', metric_walk(1, 'share', 1, 'state')),
        },
        'attrs': {
            'srv': ('%s', lambda t, d: t[0]),
            'share': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'srr_share_vo_info',
        'type': 'info',
        'help': 'VO authorization for SRR share',
        'select': metric_keys(1, 'share', 1, 'vo', 1),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'srv': ('%s', lambda t, d: t[0]),
            'share': ('%s', lambda t, d: t[1]),
            'vo_id': ('%s', metric_walk(1, 'share', 1, 'vo', 1, 'vo')),
            'vo_role': ('%s', metric_walk(1, 'share', 1, 'vo', 1, 'role')),
            'vo_group': ('%s', metric_walk(1, 'share', 1, 'vo', 1, 'group')),
        },
    },

    {
        'base': 'srr_share_endpoint_info',
        'type': 'info',
        'help': 'accessibility by endpoint for SRR share',
        'select': metric_keys(1, 'share', 1, 'endpoint', 1),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'srv': ('%s', lambda t, d: t[0]),
            'share': ('%s', lambda t, d: t[1]),
            'ep': ('%s', lambda t, d: t[2]),
        },
    },
]
