import sys
from getopt import gnu_getopt
import datetime
import re
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import yaml

sfmt = r'.*GGUS-Ticket-ID: #(?P<ticket>[0-9]+) (?:' + \
r'(?:' + \
r'TEAM ticket for site\s+"(?P<site1>[^"]+)"' + \
r'|' + \
r'Ticket for site\s+"(?P<site2>[^"]+)"\s+"(?P<vo1>[^"]+)"' + \
r')\s+"(?P<msg1>.*)"' + \
r'|' + \
r'TEAM TICKET SUBMITTED\s+-\s+(?P<msg2>.*)' + \
r')$'


endpoint = None
panel = 0
dashboard = None
tags = set()
token = None
site = None
ticket = None
voname = None
msg = None
opts, args = gnu_getopt(sys.argv[1:], 'E:o:t:s:m:d:p:T:k:D:')
for opt, val in opts:
    if opt == '-E':
        endpoint = val
        pass
    elif opt == '-o':
        voname = val
        pass
    elif opt == '-s':
        site = val
        pass
    elif opt == '-d':
        dashboard = val
        pass
    elif opt == '-p':
        panel = int(val)
        pass
    elif opt == '-T':
        tags.add(val)
        pass
    elif opt == '-t':
        ticket = int(val)
        pass
    elif opt == '-m':
        msg = val
        pass
    elif opt == '-k':
        token = val
        pass
    elif opt == '-D':
        with open(val, 'r') as fh:
            doc = yaml.load(fh, Loader=yaml.SafeLoader)
            endpoint = doc.get('endpoint', endpoint)
            dashboard = doc.get('dashboard', dashboard)
            panel = doc.get('panel', panel)
            token = doc.get('token', token)
            tags = set(doc.get('tags', tags))
            pass
        pass
    continue

if site is not None or msg is not None or ticket is not None:
    if site is None or msg is None or ticket is None:
        sys.stderr.write('must specify -s, -m and -t together\n')
        exit(1)
        pass
    evtime = datetime.datetime.now()
    pass
else:
    ## Read the email from standard input.
    emsg = email.message_from_binary_file(sys.stdin.buffer)

    ## Decode the subject line, and match it against our expression.
    ## If there's no match, quietly exit.  Otherwise, pick out the
    ## store and queue, and the new status.
    subj = ''.join([ t[0] if isinstance(t[0], str)
                     else str(t[0], t[1] or 'US-ASCII')
                     for t in decode_header(emsg['subject']) ]) \
             .replace('\n', '')
    expr = re.compile(sfmt)
    mt = expr.match(subj)
    if not mt:
        sys.stderr.write('Failed: [%s]\n' % subj)
        exit(0)
        pass
    msg = mt.group('msg1') or mt.group('msg2')
    ticket = int(mt.group('ticket'))
    site = mt.group('site1') or  mt.group('site2')
    voname = mt.group('vo1')

    ## Use the Date field for the timestamp.
    rawedate = ''.join([ t[0] if isinstance(t[0], str)
                         else str(t[0], t[1] or 'US-ASCII')
                         for t in decode_header(emsg['date']) ])
    evtime = parsedate_to_datetime(rawedate)
    # evtime = datetime.datetime.fromtimestamp(evtime.timestamp(),
    #                                          tz=datetime.timezone.utc)
    pass

tags.add('GGUS')

ticketurl = 'https://ggus.eu/index.php?mode=ticket_info&ticket_id=%d'
vomsg = '' if voname is None else (' (VO: %s)' % voname)
if site is not None:
    tags.add("site:" + site)
    pass
data = {
    'dashboardUID': dashboard,
    'panelId': panel,
    'time': int(evtime.timestamp() * 1000),
    'tags': list(tags),
    'text': ('<a href="' + ticketurl + '">GGUS #%d: %s%s</a>') %
    (ticket, ticket, msg, vomsg),
}


if endpoint is None:
    from pprint import pprint
    pprint(data)
    exit(0)
    pass

from urllib import request
from urllib.error import URLError, HTTPError
import json

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
