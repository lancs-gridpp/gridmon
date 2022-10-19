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

import subprocess
import time
import datetime
import json
import re
import sys

_devpathfmt = re.compile(r'/dev/disk/by-path/(.*scsi.*)')

def get_device_set(args=[]):
    cmd = args + [ 'ceph', 'device', 'ls', '--format=json' ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    doc = json.loads(proc.stdout.read().decode("utf-8"))
    result = { }
    for elem in doc:
        if 'devid' not in elem or 'location' not in elem:
            continue
        if len(elem['location']) == 0:
            continue
        devid = elem['devid']
        pathtxt = elem['location'][0]['path']
        mt = _devpathfmt.match(pathtxt)
        if mt is not None:
            (path,) = mt.groups()
            if path is not None:
                result[devid] = {
                    'host': elem['location'][0]['host'],
                    'path': path,
                }
                pass
            pass
        continue
    return result

_timestamp = \
    re.compile(r'([0-9]{4})([0-9]{2})([0-9]{2})-([0-9]{2})([0-9]{2})([0-9]{2})')

def decode_time(txt):
    yrtxt, montxt, daytxt, hrtxt, mintxt, sectxt = _timestamp.match(txt).groups()
    return int(datetime.datetime(int(yrtxt), int(montxt), int(daytxt),
                                 int(hrtxt), int(mintxt), int(sectxt),
                                 tzinfo=datetime.timezone.utc).timestamp())

def get_device_metrics(result, devid, args=[], start=None, end=None, adorn=None):
    cmd = args + [ 'ceph', 'device', 'get-health-metrics',
                   '--format=json', devid ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    doc = json.loads(proc.stdout.read().decode("utf-8"))
    mod = False
    for tstxt in doc:
        ts = decode_time(tstxt)
        if start is not None and ts < start:
            continue
        if end is not None and ts >= end:
            continue
        # sys.stderr.write('Time %d\n' % ts)
        ent = doc[tstxt]
        out = result.setdefault(ts,
                                { }).setdefault(devid,
                                                { } if adorn is None
                                                else dict(adorn))
        if 'scsi_error_counter_log' in ent:
            out['uncorrected'] = { }
            log = ent['scsi_error_counter_log']
            for mode in log:
                out['uncorrected'][mode] = log[mode]['total_uncorrected_errors']
                mod = True
                continue
            pass
        if 'scsi_grown_defect_list' in ent:
            out['defects'] = ent['scsi_grown_defect_list']
            mod = True
            pass
        continue
    return mod
    
    

class CephHealthCollector:
    def __init__(self, cmdpfx, lag=20, horizon=0):
        self.lag = lag
        self.last = 0
        # self.last = int(time.time()) - self.lag - horizon
        self.cmdpfx = cmdpfx
        pass

    def update(self, limit=None):
        ## Our time range is from self.last to curr.
        curr = int(time.time()) - self.lag

        newdata = { }
        devset = get_device_set(args=self.cmdpfx)
        for devid in devset:
            if limit is not None and limit <= 0:
                break

            ## Get the history for this device within our time range.
            ## (This actually gets the entire history, and then
            ## filters it, as the Ceph command used does not offer
            ## filtering, except to get a specific timestamp.)
            sys.stderr.write('Getting %s\n' % devid)
            if get_device_metrics(newdata,
                                  devid,
                                  args=self.cmdpfx,
                                  start=self.last,
                                  end=curr,
                                  adorn=devset[devid]) and limit is not None:
                limit -= 1
                pass

            continue

        ## The next time range is begins where we finished.
        self.last = curr
        return newdata

    pass


schema = [
    {
        'base': 'cephhealth_scsi_grown_defect_list',
        'type': 'counter',
        'help': 'number of defects',
        'select': lambda e: [ (t,) for t in e
                               if 'defects' in e[t] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]]['defects']),
            '_created': ('%d', lambda t, d: 0),
        },
        'attrs': {
            'devid': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'cephhealth_scsi_uncorrected',
        'type': 'counter',
        'help': 'uncorrected errors',
        'select': lambda e: [ (t, m) for t in e
                              if 'uncorrected' in e[t]
                              for m in e[t]['uncorrected'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0]]['uncorrected'][t[1]]),
            '_created': ('%d', lambda t, d: 0),
        },
        'attrs': {
            'devid': ('%s', lambda t, d: t[0]),
            'mode': ('%s', lambda t, d: t[1]),
        },
    },

    {
        'base': 'cephhealth_metadata',
        'type': 'info',
        'select': lambda e: [ (t,) for t in e
                              if 'path' in e[t] ],
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'devid': ('%s', lambda t, d: t[0]),
            'path': ('%s', lambda t, d: d[t[0]]['path']),
        },
    },
]
