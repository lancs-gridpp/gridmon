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

import logging

class MetricConverter:
    def __init__(self, hist):
        self._hist = hist
        pass

    ## Return true if the supplied data was neither used nor logged.
    def convert(self, ts, addr, tree):
        pgm = tree.attrib['pgm']

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
            logging.warning('summary from %s:%d lacks <info>' % addr)
            return False
        host = blk.find('host').text
        name = blk.find('name').text
        inst = (host, name, pgm)
        logging.info('summary from %s:%d %s:%s@%s okay' %
                     (addr + (pgm, name, host)))

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
        self._hist.install( { timestamp: { inst: data } } )
        return False

    pass
