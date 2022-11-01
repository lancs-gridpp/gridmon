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
import traceback
import logging

def get_pools(args=[]):
    ## TODO: Is there a Python library that will do this more
    ## directly?
    cmd = args + [ 'rados', 'lspools' ]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                universal_newlines=True)
        return set([ i.strip() for i in proc.stdout.readlines() ])
    except FileNotFoundError:
        logging.error('Command not found: %s' % cmd)
    except:
        logging.error(traceback.format_exc())
        logging.error('Failed to execute %s' % cmd)
        pass
    return set()

def get_inconsistent_pgs(pools, args=[]):
    ## For each pool, get the set of PG ids for inconsistent PGs.
    groups = set()
    for pool in pools:
        cmd = args + [ 'rados', 'list-inconsistent-pg', pool ]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            doc = json.loads(proc.stdout.read().decode("utf-8"))
            for pgid in doc:
                groups.add(pgid)
                continue
        except FileNotFoundError:
            logging.error('Command not found: %s' % cmd)
        except json.decoder.JSONDecodeError:
            logging.error('No JSON data from %s' % cmd)
        except:
            logging.error(traceback.format_exc())
            logging.error('Failed to execute %s' % cmd)
            pass
        continue
    return groups

_pgidfmt = re.compile(r'([0-9]+)\.(.+)')

def get_osd_complaints(pgids, args=[]):
    ## For each inconsistent PG, find out which of the OSDs it uses
    ## have errors.  Generate a dict from OSD number to id of PG
    ## complaining about the OSD.
    osds = { }
    for pgid in pgids:
        pool, subid = _pgidfmt.match(pgid).groups()
        pool = int(pool)
        cmd = args + [ 'rados', 'list-inconsistent-obj', pgid ]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            doc = json.loads(proc.stdout.read().decode("utf-8"))
            for incons in doc['inconsistents']:
                for shard in incons['shards']:
                    if len(shard['errors']) > 0:
                        osds.setdefault(shard['osd'], {}) \
                            .setdefault(pool, set()) \
                            .add(subid)
                        continue
                    continue
                continue
        except FileNotFoundError:
            logging.error('Command not found: %s' % cmd)
        except json.decoder.JSONDecodeError:
            logging.error('No JSON data for %s from %s' % (pgid, cmd))
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.error('Failed to execute %s' % cmd)
            pass
        continue
    return osds

def convert_osd_complaints_to_metrics(complaints):
    msg = ''
    msg += '# TYPE cephhealth_osd_pg_complaint info\n'
    msg += '# HELP cephhealth_osd_pg_complaint ' + \
        'PGs referencing OSDs with errors\n'
    for osdid in complaints:
        elem = complaints[osdid]
        for pool in elem:
            for pgid in elem[pool]:
                msg += ('cephhealth_osd_pg_complaint{ceph_daemon="osd.%d"' +
                        ',pool_id="%d",pg_id="%s"} 1\n') % (osdid, pool, pgid)
            continue
        continue
    return msg

def get_osd_complaints_as_metrics(args=[]):
    pools = get_pools(args=args)
    pgids = get_inconsistent_pgs(pools, args=args)
    complaints = get_osd_complaints(pgids, args=args)
    return convert_osd_complaints_to_metrics(complaints)

_devpathfmt = re.compile(r'/dev/disk/by-path/(.*scsi.*)')

def get_device_set(args=[]):
    cmd = args + [ 'ceph', 'device', 'ls', '--format=json' ]
    result = { }
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        doc = json.loads(proc.stdout.read().decode("utf-8"))
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
    except FileNotFoundError:
        logging.error('Command not found: %s' % cmd)
    except json.decoder.JSONDecodeError:
        logging.error('No JSON data from %s' % cmd)
    except:
        logging.error(traceback.format_exc())
        logging.error('Failed to execute %s' % cmd)
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
    mod = False
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        doc = json.loads(proc.stdout.read().decode("utf-8"))
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
                    out['uncorrected'][mode] = \
                        log[mode]['total_uncorrected_errors']
                    mod = True
                    continue
                pass
            if 'scsi_grown_defect_list' in ent:
                out['defects'] = ent['scsi_grown_defect_list']
                mod = True
                pass
            continue
    except json.decoder.JSONDecodeError:
        logging.error('No JSON data for %s from %s' % (pgid, cmd))
    except:
        logging.error(traceback.format_exc())
        logging.error('Failed to execute %s' % cmd)
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
            logging.debug('Getting %s' % devid)
            if get_device_metrics(newdata,
                                  devid,
                                  args=self.cmdpfx,
                                  start=self.last,
                                  end=curr,
                                  adorn=devset[devid]):
                if limit is not None:
                    limit -= 1
                    pass
            else:
                logging.info('no data for %s' % devid)
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
