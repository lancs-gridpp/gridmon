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

import collections

def _escape_char(c, chars):
    return '\\' + c if c in chars else c

def _escape(text, chars):
    return ''.join([ _escape_char(c, chars) for c in text ])

def escape_key(text):
    return _escape(text, '\\\n\r =')

def escape_value(text):
    if text == '':
        return '""'

    ## Count spaces and equals.
    n = 0
    for c in text:
        if c == ' ':
            n += 1
            if n >= 3:
                return '"%s"' % _escape(text, '\\"\n\r')
            pass
        continue
    return _escape(text, '\\"\n\r ')

def _encode(data, pfx, ctxt):
    result = []
    for k, v in data.items():
        if isinstance(v, collections.Mapping):
            result += _encode(v, pfx + k + '_', ctxt.get(k) or { })
        else:
            fmt = ctxt.get(k, '%s')
            result.append(escape_key(pfx + k) + '=' + escape_value(fmt % (v,)))
            continue
        pass
    return result

def encode(data, ctxt={}):
    return ' '.join(_encode(data, '', { }))
