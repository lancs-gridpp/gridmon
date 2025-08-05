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
import socket
import xml
from defusedxml import ElementTree

from lancs_gridmon.xrootd.summary import schema as xrootd_summary_schema

class ReportReceiver:
    def __init__(self, bindprop, hist):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(bindprop)
        self.hist = hist
        self.running = True
        pass

    def keep_polling(self):
        while self.hist.check():
            try:
                self.poll()
                pass
            except KeyboardInterrupt:
                break
            except:
                traceback.print_exc()
                pass
            continue
        pass

    def halt(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    def poll(self):
        ## Receive XML in a UDP packet.
        try:
            (msg, addr) = self.sock.recvfrom(65536)
        except OSError:
            return
        try:
            tree = ElementTree.fromstring(msg)
        except xml.etree.ElementTree.ParseError:
            return
        logging.info('Report from %s:%d' % addr)
        if tree.tag != 'statistics':
            logging.warning('Ignored non-stats %s from %s:%d' %
                            ((tree.tag,) + addr))
            return

        pgm = tree.attrib['pgm']
        # if pgm != 'xrootd':
        #     logging.warning('Ignored non-xrootd program %s from %s:%d' %
        #                     ((pgm,) + addr))
        #     return

        ## Extract timestamp data.
        timestamp = int(tree.attrib['tod'])
        start = int(tree.attrib['tos'])
        vers = tree.attrib['ver']

        ## Index all the <stats> elements by id.
        stats = { }
        for stat in tree.findall('stats'):
            kind = stat.attrib.get('id')
            if kind is None:
                continue
            stats[kind] = stat
            continue

        ## Get an instance identifier.
        blk = stats.get('info')
        if blk is None:
            logging.warning('no info from %s:%d' % addr)
            return
        host = blk.find('host').text
        name = blk.find('name').text
        inst = (host, name, pgm)
        logging.info('instance %s:%s@%s from %s:%d' %
                     ((pgm, name, host) + addr))

        ## Extract other metadata.
        port = int(blk.find('port').text)
        site = tree.attrib['site']

        ## Extract the fields we're interested in.
        data = { }
        data['start'] = start
        data['port'] = port
        data['ver'] = vers
        if site is not None:
            data['site'] = site
            pass

        blk = stats.get('buff')
        if blk is not None:
            sub = data.setdefault('buff', { })
            for key in [ 'reqs', 'mem', 'buffs', 'adj' ]:
                sub[key] = int(blk.find('./' + key).text)
                continue
            pass

        blk = stats.get('link')
        if blk is not None:
            sub = data.setdefault('link', { })
            for key in [ 'num', 'maxn', 'tot', 'in', 'out',
                         'ctime', 'tmo', 'stall', 'sfps' ]:
                sub[key] = int(blk.find('./' + key).text)
                continue
            pass
        blk = stats.get('poll')
        if blk is not None:
            sub = data.setdefault('poll', { })
            for key in [ 'att', 'ev', 'en', 'int' ]:
                sub[key] = int(blk.find('./' + key).text)
                continue
            pass

        blk = stats.get('sched')
        if blk is not None:
            sub = data.setdefault('sched', { })
            for key in [ 'jobs', 'inq', 'maxinq', 'threads',
                         'idle', 'tcr', 'tde', 'tlimr' ]:
                sub[key] = int(blk.find('./' + key).text)
                continue
            pass

        blk = stats.get('cms')
        if blk is not None:
            sub = data.setdefault('cms', { })
            for key in [ 'role' ]:
                sub[key] = blk.find('./' + key).text
                continue
            pass

        blk = stats.get('sgen')
        if blk is not None:
            sub = data.setdefault('sgen', { })
            for key in [ 'as', 'et', 'toe' ]:
                sub[key] = int(blk.find('./' + key).text)
                continue
            pass

        blk = stats.get('oss')
        if blk is not None:
            sub = data.setdefault('oss', { })
            for i in range(int(blk.find('./paths').text)):
                # print('  Searching for path %d' % i)
                elem = blk.find('./paths/stats[@id="%d"]' % i)
                # print(ElementTree.tostring(blk, encoding="unicode"))
                name = elem.find('./lp').text[1:-1]
                psub = sub.setdefault('paths', { }).setdefault(name, { })
                psub['rp'] = elem.find('./rp').text[1:-1]
                for key in [ 'free', 'ifr', 'ino', 'tot' ]:
                    psub[key] = int(elem.find('./' + key).text)
                continue
            for i in range(int(blk.find('./space').text)):
                # print('  Searching for space %d' % i)
                elem = blk.find('./space/stats[@id="%d"]' % i)
                name = elem.find('./name').text
                psub = sub.setdefault('spaces', { }).setdefault(name, { })
                for key in [ 'free', 'fsn', 'maxf', 'qta', 'tot', 'usg' ]:
                    psub[key] = int(elem.find('./' + key).text)
                continue
            pass

        blk = stats.get('ofs')
        if blk is not None:
            sub = data.setdefault('ofs', { })
            for key in [ 'opr', 'opw', 'opp', 'ups', 'han', 'rdr',
                         'bxq', 'rep', 'err', 'dly', 'sok', 'ser' ]:
                sub[key] = int(blk.find('./' + key).text)
                continue
            sub['role'] = blk.find('./role').text
            psub = sub.setdefault('tpc', { })
            for key in [ 'grnt', 'deny', 'err', 'exp' ]:
                psub[key] = int(blk.find('./tpc/' + key).text)
                continue
            pass

        blk = stats.get('xrootd')
        if blk is not None:
            sub = data.setdefault('xrootd', { })
            for key in [ 'num', 'err', 'rdr', 'dly' ]:
                sub[key] = int(blk.find('./' + key).text)
                continue
            psub = sub.setdefault('ops', { })
            elem = blk.find('./ops')
            for key in [ 'open', 'rf', 'rd', 'pr', 'rv', 'rs', 'wv', 'ws',
                         'wr', 'sync', 'getf', 'putf', 'misc' ]:
                psub[key] = int(elem.find('./' + key).text)
                continue
            psub = sub.setdefault('sig', { })
            elem = blk.find('./sig')
            for key in [ 'ok', 'bad', 'ign' ]:
                psub[key] = int(elem.find('./' + key).text)
                continue
            psub = sub.setdefault('aio', { })
            elem = blk.find('./aio')
            for key in [ 'num', 'max', 'rej' ]:
                psub[key] = int(elem.find('./' + key).text)
                continue
            psub = sub.setdefault('lgn', { })
            elem = blk.find('./lgn')
            for key in [ 'num', 'af', 'au', 'ua' ]:
                psub[key] = int(elem.find('./' + key).text)
                continue
            pass

        blk = stats.get('proc')
        if blk is not None:
            sub = data.setdefault('proc', { })
            sub['sys'] = int(blk.find('./sys/s').text) \
                + int(blk.find('./sys/u').text) / 1000000.0
            sub['usr'] = int(blk.find('./usr/s').text) \
                + int(blk.find('./usr/u').text) / 1000000.0
            pass

        ## Get the entry we want to populate, indexed by timestamp and
        ## by (host, name).
        self.hist.install( { timestamp: { "summary": { inst: data } } } )
        return

    pass

if __name__ == '__main__':
    import time
    import threading
    from pprint import pprint
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import functools
    import sys
    import os
    import signal
    from getopt import getopt

    ## Local libraries
    import lancs_gridmon.metrics as metrics

    ## This is a sample to populate the history with for test
    ## purposes.
    sample_now = time.time()
    sample = {
        sample_now: {
            ('xrootd-server', 'main'): {
                'buff': {
                    'adj': 0,
                    'buffs': 664,
                    'mem': 501411840,
                    'reqs': 170749
                },
                'link': {
                    'ctime': 2622954,
                    'in': 2386464921,
                    'maxn': 412,
                    'num': 203,
                    'out': 4940357118960,
                    'sfps': 0,
                    'stall': 0,
                    'tmo': 0,
                    'tot': 89157
                },
                'ofs': {
                    'bxq': 0,
                    'dly': 0,
                    'err': 0,
                    'han': 37,
                    'opp': 0,
                    'opr': 23,
                    'opw': 14,
                    'rdr': 0,
                    'rep': 0,
                    'role': 'server',
                    'ser': 0,
                    'sok': 0,
                    'tpc': {
                        'deny': 0,
                        'err': 0,
                        'exp': 0,
                        'grnt': 0
                    },
                    'ups': 0
                },
                'oss': {
                    'paths': {
                        '/cephfs': {
                            'free': 9991486255104,
                            'ifr': -1,
                            'ino': 315834240,
                            'rp': '/cephfs',
                            'tot': 10966251003904
                        }
                    }
                },
                'poll': {
                    'att': 203,
                    'en': 213581,
                    'ev': 213427,
                    'int': 0
                },
                'proc': {
                    'sys': 11107.215161,
                    'usr': 10124.33064
                },
                'sched': {
                    'idle': 11,
                    'inq': 0,
                    'jobs': 315890,
                    'maxinq': 4,
                    'tcr': 237,
                    'tde': 180,
                    'threads': 57,
                    'tlimr': 0
                },
                'sgen': {
                    'as': 1,
                    'et': 6,
                    'toe': 1651696117
                },
                'start': sample_now - 16801,
                'xrootd': {
                    'aio': {
                        'max': 0,
                        'num': 0,
                        'rej': 0
                    },
                    'dly': 0,
                    'err': 36732,
                    'lgn': {
                        'af': 0,
                        'au': 30,
                        'num': 1327,
                        'ua': 0
                    },
                    'num': 70231,
                    'ops': {
                        'getf': 0,
                        'misc': 249972,
                        'open': 13242,
                        'pr': 0,
                        'putf': 0,
                        'rd': 4705135,
                        'rf': 0,
                        'rs': 0,
                        'rv': 0,
                        'sync': 0,
                        'wr': 1425,
                        'ws': 0,
                        'wv': 0
                    },
                    'rdr': 0,
                    'sig': {
                        'bad': 0,
                        'ign': 0,
                        'ok': 0
                    }
                }
            }
        }
    }

    udp_host = ''
    udp_port = 9485
    http_host = 'localhost'
    http_port = 8744
    horizon = 60 * 30
    silent = False
    fake_data = False
    endpoint = None
    pidfile = None
    log_params = {
        'format': '%(asctime)s %(levelname)s %(message)s',
        'datefmt': '%Y-%m-%dT%H:%M:%S',
    }
    opts, args = getopt(sys.argv[1:], "zh:u:U:t:T:E:X",
                        [ 'log=', 'log-file=', 'pid-file=' ])
    for opt, val in opts:
        if opt == '-h':
            horizon = int(val) * 60
        elif opt == '-z':
            silent = True
        elif opt == '-u':
            udp_port = int(val)
        elif opt == '-U':
            udp_host = val
        elif opt == '-E':
            endpoint = val
        elif opt == '-t':
            http_port = int(val)
        elif opt == '-T':
            http_host = val
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
        elif opt == '-X':
            fake_data = True
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

    ## Record XRootD stats history, indexed by timestamp and instance.
    ## Alternatively, prepare to push stats as soon as they're
    ## converted.
    rmw = history = metrics.MetricHistory(xrootd_summary_schema,
                                          horizon=horizon)
    if endpoint is not None:
        rmw = metrics.RemoteMetricsWriter(endpoint=endpoint,
                                          schema=xrootd_summary_schema,
                                          job='xrootd',
                                          expiry=10*60)
    elif fake_data:
        history.install(sample)
        receiver = None
        pass

    ## Serve the history on demand.  Even if we don't store anything
    ## in the history, the HELP, TYPE and UNIT strings are exposed,
    ## which doesn't seem to be possible with remote-write.
    partial_handler = functools.partial(metrics.MetricsHTTPHandler,
                                        hist=history)
    webserver = HTTPServer((http_host, http_port), partial_handler)
    logging.info('Created HTTP server on http://%s:%d' %
                 (http_host, http_port))

    if endpoint is not None or not fake_data:
        ## Create a UDP socket to listen on, convert XML stats from
        ## XRootD into timestamped metrics, and drop them into the
        ## history/remote writer.
        logging.info('Creating UDP XRootD receiver on %s:%d' %
                     (udp_host, udp_port))
        receiver = ReportReceiver((udp_host, udp_port), rmw)
        pass

    try:
        if pidfile is not None:
            with open(pidfile, "w") as f:
                f.write('%d\n' % os.getpid())
                pass
            pass

        ## Use a separate thread to run the server, which we can stop
        ## by calling shutdown().
        srv_thrd = threading.Thread(target=HTTPServer.serve_forever,
                                    args=(webserver,))
        srv_thrd.start()

        try:
            receiver.keep_polling()
        except KeyboardInterrupt:
            pass

        history.halt()
        logging.info('Halted history')
        webserver.shutdown()
        logging.info('Halted webserver')
        receiver.halt()
        logging.info('Halted receiver')
        webserver.server_close()
        logging.info('Server stopped.')
    finally:
        if pidfile is not None:
            os.remove(pidfile)
            pass
        pass
    pass
