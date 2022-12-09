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
        logging.debug('Command: %s' % cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                universal_newlines=True)
        return set([ i.strip() for i in proc.stdout.readlines() ])
    except KeyboardInterrupt as e:
        raise e
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
            logging.debug('Command: %s' % cmd)
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            doc = json.loads(proc.stdout.read().decode("utf-8"))
            for pgid in doc:
                groups.add(pgid)
                continue
        except KeyboardInterrupt as e:
            raise e
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

def get_status(args=[]):
    cmd = args + [ 'ceph', 'status', '--format=json']
    try:
        logging.debug('Command: %s' % cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        return json.loads(proc.stdout.read().decode("utf-8"))
    except KeyboardInterrupt as e:
        raise e
    except FileNotFoundError:
        logging.error('Command not found: %s' % cmd)
    except json.decoder.JSONDecodeError:
        logging.error('No JSON data for Ceph status from %s' % cmd)
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error('Failed to execute %s' % cmd)
        pass
    return None

def get_status_metrics(args=[]):
    status = get_status(args=args)
    result = { }
    if status is None:
        return result
    hth = status.get('health')
    if hth is None:
        return result
    cks = hth.get('checks')
    if cks is None:
        return result
    for k, v in cks.items():
        smy = v.get('summary')
        if smy is None:
            continue
        cnt = smy.get('count')
        if cnt is None:
            continue
        result[k] = { 'count': cnt }
        continue
    return result

# def convert_status_to_metrics(status):
#     if status is None:
#         return ''
#     hth = status.get('health')
#     if hth is None:
#         return ''
#     cks = hth.get('checks')
#     if cks is None:
#         return ''
#     msg = ''
#     msg += '# TYPE cephhealth_status_check gauge\n'
#     msg += '# HELP counted check status or something\n'
#     for k, v in cks.items():
#         smy = v.get('summary')
#         if smy is None:
#             continue
#         cnt = smy.get('count')
#         if cnt is None:
#             continue
#         msg += 'cephhealth_status_check{'
#         msg += 'type="%s"' % k
#         # sev = v.get('severity')
#         # if sev is not None:
#         #     msg += ',severity="%s"' % sev
#         #     pass
#         # mut = v.get('muted')
#         # if mut is not None:
#         #     msg += ',muted="%s"' % mut
#         #     pass
#         msg += '} %d\n' % cnt
#         continue
#     return msg

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
            logging.debug('Command: %s' % cmd)
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
        except KeyboardInterrupt as e:
            raise e
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



# def convert_osd_complaints_to_metrics(complaints):
#     msg = ''
#     msg += '# TYPE cephhealth_osd_pg_complaint info\n'
#     msg += '# HELP cephhealth_osd_pg_complaint ' + \
#         'PGs referencing OSDs with errors\n'
#     for osdid in complaints:
#         elem = complaints[osdid]
#         for pool in elem:
#             for pgid in elem[pool]:
#                 msg += ('cephhealth_osd_pg_complaint{ceph_daemon="osd.%d"' +
#                         ',pool_id="%d",pg_id="%s"} 1\n') % (osdid, pool, pgid)
#             continue
#         continue
#     return msg

# def get_osd_complaints_as_metrics(args=[]):
#     pools = get_pools(args=args)
#     pgids = get_inconsistent_pgs(pools, args=args)
#     complaints = get_osd_complaints(pgids, args=args)
#     status = get_status(args=args)
#     msg = ''
#     msg += convert_osd_complaints_to_metrics(complaints)
#     msg += convert_status_to_metrics(status)
#     return msg

class CephHealthMetricPusher:
    def __init__(self, hist, cmdpfx=[], limit=None):
        self.cmdpfx = cmdpfx
        self.hist = hist
        self.disk_limit = limit
        pass

    def update(self):
        osds = get_osd_set(args=self.cmdpfx)
        if osds is None:
            return
        limit = self.disk_limit
        for osd in osds:
            ## Get metrics for one OSD, and write them into the
            ## history.
            mets = get_osd_disk_metrics(osd, args=self.cmdpfx)
            if mets is None:
                continue
            self.hist.install(mets)
            if limit is not None:
                limit -= 1
                if limit == 0:
                    break
                pass
            continue
        pass

    pass

def get_osd_set(args=[]):
    cmd = args + [ 'ceph', 'osd', 'ls', '--format=json' ]
    try:
        logging.debug('Command: %s' % cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        return json.loads(proc.stdout.read().decode("utf-8"))
    except KeyboardInterrupt as e:
        raise e
    except FileNotFoundError:
        logging.error('Command not found: %s' % cmd)
    except json.decoder.JSONDecodeError:
        logging.error('No JSON data from %s' % cmd)
    except:
        logging.error(traceback.format_exc())
        logging.error('Failed to execute %s' % cmd)
        pass
    return None

_scsi_metrics = [
    ('correction_algorithm_invocations', 'invoked', int),
    ('errors_corrected_by_eccdelayed', 'eccdelayed', int),
    ('errors_corrected_by_eccfast', 'eccfast', int),
    ('errors_corrected_by_rereads_rewrites', 'rerw', int),
    ('gigabytes_processed', 'processed', float),
    ('total_errors_corrected', 'corrected', int),
    ('total_uncorrected_errors', 'uncorrected', int),
]

def get_osd_disk_metrics(osd, args=[]):
    cmd = args + [ 'ceph', 'device', 'query-daemon-health-metrics',
                   '--format=json', 'osd.%d' % osd ]
    try:
        logging.debug('Command: %s' % cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        doc = json.loads(proc.stdout.read().decode("utf-8"))
        result = { }
        for devid, ent in doc.items():
            ## Get the timestamp.
            lct = ent.get('local_time')
            if lct is None:
                continue
            ts = lct.get('time_t')
            if ts is None:
                continue

            ## Get the SCSI fields.
            out = { }
            dfl = ent.get('scsi_grown_defect_list')
            if dfl is not None:
                out['defects'] = dfl
                pass
            log = ent.get('scsi_error_counter_log', { })
            for mode, membs in log.items():
                for src, dst, proc in _scsi_metrics:
                    val = membs.get(src)
                    if val is None:
                        continue
                    val = proc(val)
                    out.setdefault(dst, { })[mode] = val
                    continue
                continue

            ## No fields?  Not worth reporting.
            if len(out) == 0:
                continue

            result.setdefault(int(ts), {
                'disks': { },
                'checks': { },
                'osds': { },
            })['disks'].setdefault(devid, out)
            continue
        return result
    except KeyboardInterrupt as e:
        raise e
    except FileNotFoundError:
        logging.error('Command not found: %s' % cmd)
    except json.decoder.JSONDecodeError:
        logging.error('No JSON data from %s' % cmd)
    except:
        logging.error(traceback.format_exc())
        logging.error('Failed to execute %s' % cmd)
        pass
    return None

_devpathfmt = re.compile(r'/dev/disk/by-path/(.*scsi.*)')

def get_device_set(args=[]):
    cmd = args + [ 'ceph', 'device', 'ls', '--format=json' ]
    result = { }
    try:
        logging.debug('Command: %s' % cmd)
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
        logging.debug('Command: %s' % cmd)
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
    except KeyboardInterrupt as e:
        raise e
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

def update_live_metrics(hist, args=[]):
    data = { }

    ## Get complaints that OSDs have about PGs.
    pools = get_pools(args=args)
    pgids = get_inconsistent_pgs(pools, args=args)
    data['osds'] = get_osd_complaints(pgids, args=args)

    ## Get metadata about discs.
    data['disks'] = get_device_set(args=args)

    ## Get general health status.
    data['checks'] = get_status_metrics(args=args)

    ## Record this data as almost immediate metrics.
    now = int(time.time() * 1000) / 1000
    rec = { now: data }
    hist.install(rec)
    pass

schema = [
    {
        'base': 'cephhealth_status_check',
        'type': 'gauge',
        'help': 'counted check status or something',
        'select': lambda e: [ (t,) for t in e['checks'] ],
        'samples': {
            '': ('%d', lambda t, d: d['checks'][t[0]]['count']),
        },
        'attrs': {
            'type': ('%s', lambda t, d: t[0]),
        },
    },

    {
        'base': 'cephhealth_osd_pg_complaint',
        'type': 'info',
        'help': 'PGs referencing OSDs with errors',
        'select': lambda e: [ (osd, pg) for osd in e['osds']
                              if 'pg_complaints' in e['osds'][osd]
                              for pg in e['osds'][osd]['pg_complaints'] ],
        'samples': {
            '': 1,
        },
        'attrs': {
            'ceph_daemon': ('osd.%d', lambda t, d: t[0]),
            'pg_id': ('%s', lambda t, d: t[1]),
            'pool_id': ('%s', lambda t, d: d['osds'][t[0]]
                        ['pg_complaints'][t[1]]['pool_id']),
        },
    },

    {
        'base': 'cephhealth_metadata',
        'help': 'DEPRECATED use cephhealth_disk_fitting instead',
        'type': 'info',
        'select': lambda e: [ (t,) for t in e['disks']
                              if 'path' in e['disks'][t] ],
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'devid': ('%s', lambda t, d: t[0]),
            'path': ('%s', lambda t, d: d['disks'][t[0]]['path']),
        },
    },

    {
        'base': 'cephhealth_disk_fitting',
        'help': 'SCSI path to device within its host',
        'type': 'info',
        'select': lambda e: [ (t,) for t in e['disks']
                              if 'path' in e['disks'][t] ],
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'devid': ('%s', lambda t, d: t[0]),
            'path': ('%s', lambda t, d: d['disks'][t[0]]['path']),
        },
    },

    {
        'base': 'cephhealth_scsi_grown_defect_list',
        'type': 'counter',
        'help': 'number of defects',
        'select': lambda e: [ (t,) for t in e['disks']
                               if 'defects' in e['disks'][t] ],
        'samples': {
            '_total': ('%d', lambda t, d: d['disks'][t[0]]['defects']),
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
        'select': lambda e: [ (t, m) for t in e['disks']
                              if 'uncorrected' in e['disks'][t]
                              for m in e['disks'][t]['uncorrected'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d['disks'][t[0]]['uncorrected'][t[1]]),
            '_created': ('%d', lambda t, d: 0),
        },
        'attrs': {
            'devid': ('%s', lambda t, d: t[0]),
            'mode': ('%s', lambda t, d: t[1]),
        },
    },
]

if __name__ == '__main__':
    import functools
    from http.server import HTTPServer
    import threading
    import os
    import pprint
    import subprocess
    import errno
    from getopt import gnu_getopt

    ## Local libraries
    import metrics

    todfmt = re.compile(r'([0-9]{1,2}):([0-9]{1,2})')
    def get_tod_offset(text):
        m = todfmt.match(text)
        if m is None:
            return None
        hrt, mnt = m.groups()
        hr = int(hrt)
        mn = int(mnt)
        if hr < 0 or hr > 23:
            return None
        if mn < 0 or mn > 59:
            return None
        return (hr * 60 + mn) * 60

    http_host = "localhost"
    http_port = 8799
    horizon = 30
    lag = 20
    silent = False
    metrics_endpoint = None
    disk_limit = None
    skip = True
    log_params = {
        'format': '%(asctime)s %(message)s',
        'datefmt': '%Y-%d-%mT%H:%M:%S',
    }
    schedule = set()
    opts, args = gnu_getopt(sys.argv[1:], "zh:l:T:t:s:M:",
                            [ 'disk-limit=', 'log=', 'log-file=', 'now' ])
    for opt, val in opts:
        if opt == '-h':
            horizon = int(val)
        elif opt == '-l':
            lag = int(val)
        elif opt == '-z':
            silent = True
        elif opt == '--now':
            skip = False
        elif opt == '-s':
            tod = get_tod_offset(val)
            if tod is None:
                sys.stderr.write('bad time of day: %s' % val)
                sys.exit(1)
                pass
            schedule.add(tod)
        elif opt == '-T':
            http_host = val
        elif opt == '-M':
            metrics_endpoint = val
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
        elif opt == '--disk-limit':
            disk_limit = int(val)
            pass
        continue

    ## If no schedule is provided, define the current time of day as the
    ## sole entry.
    if len(schedule) == 0:
        now = datetime.datetime.utcnow()
        tod = (now.hour * 60 + now.minute) * 60
        schedule.add(tod)
        pass

    if silent:
        with open('/dev/null', 'w') as devnull:
            fd = devnull.fileno()
            os.dup2(fd, sys.stdout.fileno())
            os.dup2(fd, sys.stderr.fileno())
            pass
        pass

    logging.basicConfig(**log_params)

    def get_next_in_schedule(schedule):
        ## What time is it now?  When did this day start?  When does
        ## tomorrow start?
        calnow = datetime.datetime.now(tz=datetime.timezone.utc)
        caltoday = datetime.datetime(calnow.year, calnow.month, calnow.day,
                                     tzinfo=datetime.timezone.utc)
        tod = (calnow - caltoday).total_seconds()
        caltomorrow = caltoday + datetime.timedelta(days=1)
        tomorrow = datetime.datetime.timestamp(caltomorrow)
        today = datetime.datetime.timestamp(caltoday)

        ## Try each time of day in the schedule, to see whether it is next
        ## today or tomorrow.
        best = None
        for scand in schedule:
            cand = scand + (tomorrow if scand < tod else today)
            if best is None or cand < best:
                best = cand
                continue
        return best

    ## Define a remote-write endpoint, and an object to pull disc
    ## metrics on command, and remote-write them immediately to that
    ## endpoint.
    rmw = metrics.RemoteMetricsWriter(endpoint=metrics_endpoint,
                                      schema=schema,
                                      job='cephhealth',
                                      expiry=horizon)
    pusher = CephHealthMetricPusher(rmw, cmdpfx=args, limit=disk_limit)

    ## Define how to get on-demand metrics.  Use a separate thread to
    ## run the server, which we can stop by calling
    ## webserver.shutdown().
    # cephcoll = CephHealthCollector(args, lag=lag, horizon=horizon)
    methist = metrics.MetricHistory(schema, horizon=horizon)
    updater = functools.partial(update_live_metrics, methist, args=args)
    #nowmets = functools.partial(get_osd_complaints_as_metrics, args=args)
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
    srv_thrd = threading.Thread(target=HTTPServer.serve_forever,
                                args=(webserver,),
                                daemon=True)
    srv_thrd.start()

    logging.info('Schedule: %s' % [
        '%02d:%02d:%02d' % (int(x / 3600),
                            int(x / 60) % 60,
                            x % 60) for x in schedule ])

    def check_delay(hist, start):
        if not hist.check():
            return False
        now = int(time.time())
        delay = start - now
        return delay > 0

    try:
        while methist.check():
            if skip:
                skip = False
            else:
                ## Get data now, and push it.
                logging.info('Getting latest data')
                pusher.update()
                logging.info('Installed')

            ## Work out when we do this again.
            start = get_next_in_schedule(schedule)
            lim = datetime.datetime.fromtimestamp(start)
            logging.info('Waiting until %s in %s' % \
                         (lim,
                          datetime.timedelta(seconds=start - time.time())))
            while check_delay(methist, start):
                time.sleep(1)
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
