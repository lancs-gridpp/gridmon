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

import sys
import re
import email
from email.header import decode_header
from datetime import datetime
from pprint import pformat
from getopt import gnu_getopt

import metrics

## Parse command-line arguments.
endpoint = None
queue = None
site = None
state = None
opts, args = gnu_getopt(sys.argv[1:], 'M:q:s:xr')
for opt, val in opts:
    if opt == '-M':
        endpoint = val
        pass
    elif opt == '-s':
        site = val
        pass
    elif opt == '-q':
        queue = val
        pass
    elif opt == '-x':
        state = 1
        pass
    elif opt == '-r':
        state = 0
        pass
    continue

if site is not None or queue is not None or state is not None:
    if site is None or queue is None or state is None:
        sys.stderr.write('must specify all of -s site -q queue -x/-r\n')
        exit(1)
        pass
    evtime = datetime.now()
    pass
else:
    sfmt = r'.*\[([^\]]+)\]\s+(Auto-Excluded|reset online)\s+([-a-zA-Z0-9_]+)'
    dfmt = '%a, %d %b %Y %H:%M:%S %z'

    ## Read the email from standard input.
    msg = email.message_from_binary_file(sys.stdin.buffer)

    ## Decode the subject line, and match it against our expression.  If
    ## there's no match, quietly exit.  Otherwise, pick out the site and
    ## queue, and the new status.
    subj = ''.join([ t[0] if isinstance(t[0], str)
                     else str(t[0], t[1] or 'US-ASCII')
                     for t in decode_header(msg['subject']) ])
    expr = re.compile(sfmt)
    mt = expr.match(subj)
    if not mt:
        exit(0)
        pass
    queue = mt.group(1)
    state = 1 if mt.group(2) == 'Auto-Excluded' else 0
    site = mt.group(3)

    ## Use the Date field for the timestamp, as it's harder for us to
    ## understand the timezone in the subject line.
    rawedate = ''.join([ t[0] if isinstance(t[0], str)
                         else str(t[0], t[1] or 'US-ASCII')
                         for t in decode_header(msg['date']) ])
    evtime = datetime.strptime(rawedate, dfmt)
    pass

## Create the metrics structure with a sole entry.
data = {
    evtime.timestamp(): {
        (site, queue): {
            'state': state,
        },
    },
}

## Transmit the sole value as a metric.
schema = [
    {
        'base': 'hammercloud_state',
        'type': 'gauge',
        'help': 'whether an exclusion is occurring',
        'select': lambda e: [ t for t in e
                              if 'state' in e[t] ],
        'samples': {
            '': ('%d', lambda t, d: d[t]['state']),
        },
        'attrs': {
            'site': ('%s', lambda t, d: t[0]),
            'queue': ('%s', lambda t, d: t[1]),
        },
    },
]
metrics = metrics.RemoteMetricsWriter(endpoint=endpoint,
                                      schema=schema,
                                      job='hammercloud',
                                      expiry=10*60)
metrics.install(data)
