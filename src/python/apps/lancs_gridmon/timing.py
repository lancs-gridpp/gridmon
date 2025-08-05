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

_elems = [ ('w', 7), ('d', 24), ('h', 60),
           ('m', 60), ('s', 1000, 'X'), ('ms', 1000), ('us', 1) ]

_dur_pat = r'^' + r''.join([ r'(?:(\d+)(?:' + t[0] + r'(?![a-z]))' +
                             (r'?' if len(t) >= 3 and '?' in t[2] else r'') +
                             r')?' for t in _elems ]) + r'$'
_dur_fmt = re.compile(_dur_pat)

def parse_duration(s):
    m = _dur_fmt.match(str(s))
    if m is None:
        return None
    r = 0
    for i, spec in enumerate(_elems):
        if m[i + 1] is not None:
            r += int(m[i + 1])
            pass
        r *= spec[1]
        continue
    return r / 1000000

if __name__ == '__main__':
    import sys
    for s in sys.argv[1:]:
        print('%s -> %s' % (s, parse_duration(s)))
        continue
    pass
