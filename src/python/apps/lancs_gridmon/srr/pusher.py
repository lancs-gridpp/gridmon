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
from getopt import gnu_getopt
import json
import time
from pprint import pprint

import lancs_gridmon.metrics as metrics
import lancs_gridmon.apps as apputils
from lancs_gridmon.srr.schema import schema, srr2data
from lancs_gridmon.metrics import RemoteMetricsWriter

infilename = None
outendpoint = None
logcfg = apputils.default_log_config()
opts, args = gnu_getopt(sys.argv[1:], 'i:o:',
                        [ 'input=', 'output=', 'log=', 'log-file=' ])
for opt, val in opts:
    if opt == '-i' or opt == '--input':
        infilename = val
    elif opt == '-o' or opt == '--output':
        outendpoint = val
    elif opt == '--log':
        logcfg['level'] = val
    elif opt == '--log-file':
        logcfg['filename'] = val
        pass
    continue

if 'level' in logcfg:
    if isinstance(logcfg['level'], str):
        logcfg['level'] = getattr(logging, logcfg['level'].upper())
        pass
    if not isinstance(logcfg['level'], int):
        raise RuntimeError('bad log level [%s]\n' % logcfg['level'])
    pass

apputils.prepare_log_rotation(logcfg)

with open(infilename, 'r') as infd:
    doc = json.loads(infd.read())
    pass

data = srr2data(doc)

rmw = RemoteMetricsWriter(outendpoint, schema)
rmw.install(data)
