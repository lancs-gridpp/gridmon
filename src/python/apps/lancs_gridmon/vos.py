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

import os
import yaml
import logging

from lancs_gridmon.paths import LongestPathMapping as VOPathMapping

class WatchingVODatabase:
    def __init__(self, filename, counter_limit=1000, default='', **kwargs):
        self._filename = filename
        self._mtime = None
        self._default = default

        ## These are the methods for determining VO.
        self._issuers = dict()
        self._paths = None
        self._xfer_users = dict()
        ## TODO: Add other methods, like job users/accounts, and DNs.

        self._counter = 0
        self._limit = counter_limit
        pass

    def __update(self):
        try:
            if self._filename is None:
                return

            ## Avoid checking all the time.
            if self._counter > 0:
                return

            ## Determine whether we need to reload.
            logging.debug('mtime of %s for VOs' % self._filename)
            new_mtime = os.path.getmtime(self._filename)
            if self._paths is not None and new_mtime <= self._mtime:
                return

            ## Load in the data as YAML.
            logging.info('opening %s for VOs' % self._filename)
            with open(self._filename, 'r') as fh:
                data = yaml.load(fh, Loader=yaml.SafeLoader)
                pass

            ## Extract the data as structures ready for look-up.
            self._issuers = dict()
            self._xfer_users = dict()
            for k, v in data.items():
                for uri in v.get('token_issuers', list()):
                    self._issuers[uri] = k
                    continue
                for usr in v.get('transfers', dict()).get('users', list()):
                    self._xfer_users[usr] = k
                    continue
                continue
            self._paths = VOPathMapping(data,
                                        lambda x: x.get('transfers', dict()) \
                                        .get('paths', list()))

            ## Record the latest-parsed modification time.
            self._mtime = new_mtime

        finally:
            ## Keep count of how often we've checked.
            self._counter += 1
            if self._counter >= self._limit:
                self._counter = 0
                pass
            pass
        return

    def __set_from_xrootd(self, dest, org_key, subj_key, inf):
        if inf is None:
            return False

        if 'token' in inf:
            if 'args' in inf['token']:
                if 'auth' in inf['token']['args']:
                    if len(inf['token']['args']['auth']) > 0:
                        if 'org' in inf['token']['args']['auth'][0]:
                            raw = inf['token']['args']['auth'][0]['org']
                            dest[org_key] = self._issuers.get(raw, raw)
                            if 'subj' in inf['token']['args']:
                                dest[subj_key] = inf['token']['args']['subj']
                                pass
                            return True
                        pass
                    pass
                pass
            pass

        if 'args' in inf:
            if 'auth' in inf['args']:
                if len(inf['args']['auth']) > 0:
                    if 'org' in inf['args']['auth'][0]:
                        dest[org_key] = inf['args']['auth'][0]['org']
                        return True
                    pass
                pass
            pass

        return False

    def __set_from_path(self, dest, org_key, info):
        if info is None:
            return False
        if self._paths is None:
            return False
        org = self._paths[info]
        if org is None:
            return False
        dest[org_key] = org
        return True

    def __set_from_xfer_user(self, dest, org_key, username):
        if username is None:
            return False
        org = self._xfer_users.get(username)
        if org is None:
            return False
        dest[org_key] = org
        return True

    def set_vo(self,
               dest,
               org_key,
               subj_key,
               xrootd=None,
               path=None,
               xfer_user=None):
        ## Check for updates occasionally.
        self.__update()

        ## Attempt to set the VO using various sources.
        if self.__set_from_xrootd(dest, org_key, subj_key, xrootd):
            return True
        if self.__set_from_path(dest, org_key, path):
            return True
        if self.__set_from_xfer_user(dest, org_key, xfer_user):
            return True

        ## Report failure if no source was effective.  Use the default
        ## if specified.
        if self._default is not None:
            dest[org_key] = self._default
            pass
        return False

    ## TODO: Generate metrics.
    pass

if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('filename')
    ap.add_argument('-p', '--path', action='append', required=False)
    ap.add_argument('-x', '--xfer-user', action='append', required=False)
    args = ap.parse_args()
    print(args)
    db = WatchingVODatabase(args.filename)
    for p in args.path or list():
        test = dict()
        if db.set_vo(test, 'org', 'subj', path=p):
            print('path %s -> %s' % (p, test['org']))
        else:
            print('path %s no match' % p)
            pass
        continue
    for u in args.xfer_user or list():
        test = dict()
        if db.set_vo(test, 'org', 'subj', xfer_user=u):
            print('user %s -> %s' % (u, test['org']))
        else:
            print('user %s no match' % u)
            pass
        continue
    pass
