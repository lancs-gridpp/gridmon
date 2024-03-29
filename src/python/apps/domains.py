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

_esc_fmt = re.compile(r'\$(\$|[0-9]|\([0-9]+\))')

def derive_domain(text, rules):
    for test in rules:
        expr = re.compile(test['match'])
        mtc = expr.match(text)
        if mtc is None:
            continue
        res = ''
        last = 0
        rpl = test['value']
        for m in _esc_fmt.finditer(rpl):
            res += rpl[last:m.start()]
            foo = m.group(1)
            if foo == '$':
                res += foo
            elif foo[0] == '(':
                num = int(foo[1:-1])
                res += mtc.group(num)
            else:
                num = int(foo)
                res += mtc.group(num)
                pass
            last = m.end()
            continue
        res += rpl[last:]
        return res
    return None

import os
import yaml

class WatchingDomainDeriver:
    def __init__(self, filename):
        self.filename = filename
        self.rules = None
        self.mtime = None
        pass

    def _update(self):
        new_mtime = os.path.getmtime(self.filename)
        if self.rules is not None and new_mtime <= self.mtime:
            return
        with open(self.filename, 'r') as fh:
            self.rules = yaml.load(fh, Loader=yaml.SafeLoader)['domains']
            pass
        self.mtime = new_mtime
        return

    def derive(self, text):
        self._update()
        return derive_domain(text, self.rules)

    pass

if __name__ == '__main__':
    import sys, readline
    wdd = WatchingDomainDeriver(sys.argv[1])
    for line in sys.stdin:
        host = line.rstrip()
        dom = wdd.derive(host)
        print('%s -> %s' % (host, dom))
        continue
    pass
