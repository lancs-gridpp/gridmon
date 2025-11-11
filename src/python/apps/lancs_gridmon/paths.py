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

class Node:
    def __init__(self):
        self._result = None
        self._subs = dict()
        pass

    def install(self, elems, r):
        if len(elems) == 0:
            self._result = r
            return
        first = elems[0]
        sub = self._subs.get(first, None)
        if sub is None:
            sub = Node()
            self._subs[first] = sub
            pass
        return sub.install(elems[1:], r)

    def seek(self, elems, r=None):
        if self._result is not None:
            r = self._result
            pass
        if len(elems) == 0:
            return r
        sub = self._subs.get(elems[0], None)
        if sub is None:
            return r
        return sub.seek(elems[1:], r)

    def __repr__(self):
        return '>%s:%s' % (self._result, self._subs)

    pass


_slashes = re.compile('/+')

def _split_path(p):
    return [ i for i in _slashes.split(p) if i ]

class LongestPathMapping:
    def __init__(self, cfg=None, sel=lambda x: x):
        self._root = Node()
        if cfg is None:
            return
        for r, paths in cfg.items():
            for p in sel(paths):
                self._root.install(_split_path(p), r)
                continue
            continue
        pass

    def __getitem__(self, key):
        return self._root.seek(_split_path(key))

    pass

if __name__ == '__main__':
    import sys
    import yaml
    with open(sys.argv[1], 'r') as fh:
        loaded = yaml.load(fh, Loader=yaml.SafeLoader)
        pass
    lpm = LongestPathMapping(loaded, lambda x: x.get('paths', list()))
    for arg in sys.argv[2:]:
        r = lpm[arg]
        print('%s: %s' % (arg, r))
        continue
    pass
