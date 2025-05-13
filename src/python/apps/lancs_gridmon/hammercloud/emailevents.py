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
import datetime
from pprint import pformat
from getopt import gnu_getopt
import yaml

import lancs_gridmon.metrics as metrics

## Parse command-line arguments.
endpoint = None
queue_type = None
queue = None
state = None
panel = 0
dashboard = None
tags = set()
token = None

opts, args = gnu_getopt(sys.argv[1:], 'D:q:t:xr')
for opt, val in opts:
    if opt == '-q':
        queue = val
        pass
    elif opt == '-t':
        queue_type = val
        pass
    elif opt == '-x':
        state = 1
        pass
    elif opt == '-r':
        state = 0
        pass
    elif opt == '-D':
        with open(val, 'r') as fh:
            doc = yaml.load(fh, Loader=yaml.SafeLoader)
            endpoint = doc.get('endpoint', endpoint)
            dashboard = doc.get('dashboard', dashboard)
            panel = doc.get('panel', panel)
            token = doc.get('token', token)
            tags.update(doc.get('tags', []))
            tags.update(doc.get('hammercloud', { }).get('tags', [ ]))
            pass
        pass
    continue

if queue is not None or queue_type is not None or state is not None:
    if queue is None or queue_type is None or state is None:
        sys.stderr.write('must specify all of -q queue -t queue_type -x/-r\n')
        exit(1)
        pass
    evtime = datetime.datetime.now()
    pass
else:
    sfmt = r'.*\[([^\]]+)\]\s+(Auto-Excluded|reset online)\s+([-a-zA-Z0-9_]+).*'
    dfmt = '%a, %d %b %Y %H:%M:%S %z'

    ## Read the email from standard input.
    msg = email.message_from_binary_file(sys.stdin.buffer)

    ## Decode the subject line, and match it against our expression.
    ## If there's no match, quietly exit.  Otherwise, pick out the
    ## store and queue, and the new status.
    subj = ''.join([ t[0] if isinstance(t[0], str)
                     else str(t[0], t[1] or 'US-ASCII')
                     for t in decode_header(msg['subject']) ])
    expr = re.compile(sfmt)
    mt = expr.match(subj)
    if not mt:
        sys.stderr.write('Failed: [%s]\n' % subj)
        exit(0)
        pass
    queue_type = mt.group(1)
    state = 1 if mt.group(2) == 'Auto-Excluded' else 0
    queue = mt.group(3)

    ## Use the Date field for the timestamp, as it's harder for us to
    ## understand the timezone in the subject line.
    rawedate = ''.join([ t[0] if isinstance(t[0], str)
                         else str(t[0], t[1] or 'US-ASCII')
                         for t in decode_header(msg['date']) ])
    evtime = datetime.datetime.strptime(rawedate, dfmt)
    evtime = datetime.datetime.fromtimestamp(evtime.timestamp(),
                                             tz=datetime.timezone.utc)
    pass

sendtime = datetime.datetime.now(tz=datetime.timezone.utc)

## We now have the queue (e.g,, UKI-NORTHGRID-LANCS-HEP-CEPH), the
## queue type (e.g., UNIFIED), and the state (1 if excluded; 0 if
## reset), happening at evtime.  If the state is excluded, simply add
## a new annotation.  Otherwise, find the most recent annotation with
## the same queue and type, and set its timeEnd field.

from urllib import request
from urllib.error import URLError, HTTPError
from urllib.parse import quote_plus
import json

required_tags = set()
required_tags.add('HammerCloud')
required_tags.add('site:' + queue)
required_tags.add('queue:' + queue_type)

if state:
    ## An exclusion has occurred.  Add an annotation.
    data = {
        'dashboardUID': dashboard,
        'panelId': panel,
        'time': int(evtime.timestamp() * 1000),
        'timeEnd' : int(7258118400 * 1000), # year 2200
        'tags': list(tags.union(required_tags)),
        'text': 'HammerCloud exclusion',
    }
    try:
        req = request.Request(endpoint, data=json.dumps(data).encode('utf-8'))
        req.add_header('Content-Type', 'application/json')
        req.add_header('Accept', 'application/json')
        if token is not None:
            req.add_header('Authorization', 'Bearer ' + token)
            pass
        rsp = request.urlopen(req)
        code = rsp.getcode()
        sys.stderr.write('code %d\n' % code)
    except HTTPError as e:
        sys.stderr.write('target %s response %d "%s"\n' %
                         (endpoint, e.code, e.reason))
        pass
    except URLError as e:
        sys.stderr.write('no target %s "%s"\n' % (endpoint, e.reason))
        pass
    pass
else:
    ## A reset has occurred.  Find the previous annotation.
    endTime = int(evtime.timestamp() * 1000)
    startTime = endTime - 30 * 24 * 60 * 60 * 1000
    requri = endpoint + '?from=%d&to=%d&tags=%s&tags=%s' % \
        (startTime, endTime, quote_plus('site:' + queue),
         quote_plus('queue:' + queue_type))
    #sys.stderr.write('requri=%s\n' % requri)
    req = request.Request(endpoint)
    req.add_header('Accept', 'application/json')
    if token is not None:
        req.add_header('Authorization', 'Bearer ' + token)
        pass
    rsp = request.urlopen(req)
    code = rsp.getcode()
    if code != 200:
        sys.stderr.write('search code %d\n' % code)
        sys.exit(0)
        pass
    doc = json.loads(rsp.read().decode("utf-8"))
    #sys.stderr.write('result=%s\n' % json.dumps(doc))
    matcher = lambda e: set(e['tags']).issuperset(required_tags) and \
        ('timeEnd' not in e or e['timeEnd'] > endTime)
    matches = list(filter(matcher, doc))
    #sys.stderr.write('matches=%s\n' % json.dumps(matches))
    if len(matches) == 0:
        sys.stderr.write('nothing found\n')
        sys.exit(0)
        pass
    latest = max(matches, key=lambda e: e['time'])
    #sys.stderr.write('latest=%s\n' % json.dumps(latest))
    #sys.exit(0)
    annId = latest['id']
    data = {
        'timeEnd': endTime,
    }

    try:
        req = request.Request('%s/%d' % (endpoint, annId), method='PATCH',
                              data=json.dumps(data).encode('utf-8'))
        req.add_header('Content-Type', 'application/json')
        req.add_header('Accept', 'application/json')
        if token is not None:
            req.add_header('Authorization', 'Bearer ' + token)
            pass
        rsp = request.urlopen(req)
        code = rsp.getcode()
        sys.stderr.write('code %d\n' % code)
    except HTTPError as e:
        sys.stderr.write('target %s response %d "%s"\n' %
                         (endpoint, e.code, e.reason))
        pass
    except URLError as e:
        sys.stderr.write('no target %s "%s"\n' % (endpoint, e.reason))
        pass

    pass

sys.exit(0)

## Create the metrics structure with a sole entry.
data = {
    evtime.timestamp(): {
        (queue, queue_type): {
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
            'queue': ('%s', lambda t, d: t[0]),
            'queue_type': ('%s', lambda t, d: t[1]),
        },
    },
]
metrics = metrics.RemoteMetricsWriter(endpoint=endpoint,
                                      schema=schema,
                                      job='hammercloud',
                                      expiry=30)
sys.stderr.write('%d (%s) %s/%s is %d\n' %
                 (evtime.timestamp(),
                  evtime.strftime('%Y-%m-%dT%H:%M:%S%z'),
                  queue, queue_type, state))
if not metrics.install(data):
    sys.stderr.write('failed to write to %s\n' % endpoint)
    sys.stderr.write('%d seconds later than %d (%s)\n' %
                     (sendtime.timestamp() - evtime.timestamp(),
                      sendtime.timestamp(),
                      sendtime.strftime('%Y-%m-%dT%H:%M:%S%z')))
    exit(1)
    pass
