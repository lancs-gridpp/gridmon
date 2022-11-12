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

import logging
import traceback
import socket
import xml
from defusedxml import ElementTree

schema = [
    {
        'base': 'xrootd_buff_mem',
        'type': 'gauge',
        'unit': 'bytes',
        'help': 'bytes allocated to buffers',
        'select': lambda e: [ t for t in e
                              if 'buff' in e[t] and 'mem' in e[t]['buff'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['buff']['mem']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_up',
        'type': 'gauge',
        'help': 'whether an XRootD instance has reported',
        'select': lambda e: [ t for t in e ],
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_buff_buffs',
        'type': 'gauge',
        'help': 'number of allocated buffers',
        'select': lambda e: [ t for t in e
                              if 'buff' in e[t] and 'buffs' in e[t]['buff'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['buff']['buffs']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_buff_adj',
        'type': 'counter',
        'help': 'number of buffer profile adjustments',
        'select': lambda e: [ t for t in e
                              if 'buff' in e[t] and 'adj' in e[t]['buff'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['buff']['adj']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_buff_reqs',
        'type': 'counter',
        'help': 'number of buffer requests',
        'select': lambda e: [ t for t in e
                              if 'buff' in e[t] and 'reqs' in e[t]['buff'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['buff']['reqs']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_num',
        'type': 'gauge',
        'help': 'number of concurrent connections',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'num' in e[t]['link'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['link']['num']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_ctime',
        'unit': 'seconds',
        'type': 'counter',
        'help': 'cummulative time spent in connections',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'ctime' in e[t]['link'] ],
        'samples': {
            '_total': ('%.3f', lambda t, d: d[t[0:2]]['link']['ctime']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_maxn',
        'type': 'counter',
        'help': 'maximum number of concurrent connections',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'maxn' in e[t]['link'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['link']['maxn']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_tot',
        'type': 'counter',
        'help': 'total number of connections',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'tot' in e[t]['link'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['link']['tot']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_tmo',
        'type': 'counter',
        'help': 'read request timeouts',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'tmo' in e[t]['link'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['link']['tmo']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_stall',
        'type': 'counter',
        'help': 'partial data received occurences',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'stall' in e[t]['link'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['link']['stall']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_sfps',
        'type': 'counter',
        'help': 'partial sendfile ops',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'sfps' in e[t]['link'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['link']['sfps']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_in',
        'unit': 'bytes',
        'type': 'counter',
        'help': 'bytes received',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'in' in e[t]['link'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['link']['in']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_out',
        'unit': 'bytes',
        'type': 'counter',
        'help': 'bytes received',
        'select': lambda e: [ t for t in e
                              if 'link' in e[t] and 'out' in e[t]['link'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['link']['out']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sgen_et',
        'unit': 'seconds',
        'type': 'gauge',
        'help': 'elapsed time for stats completion',
        'select': lambda e: [ t for t in e
                              if 'sgen' in e[t] and 'et' in e[t]['sgen'] ],
        'samples': {
            '': ('%.6f', lambda t, d: d[t[0:2]]['sgen']['et'] / 1e6),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sgen_toe',
        'unit': 'seconds',
        'type': 'gauge',
        'help': 'stats completion time',
        'select': lambda e: [ t for t in e
                              if 'sgen' in e[t] and 'toe' in e[t]['sgen'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['sgen']['toe']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sgen_as',
        'type': 'gauge',
        'help': 'asynchronous stats',
        'select': lambda e: [ t for t in e
                              if 'sgen' in e[t] and 'as' in e[t]['sgen'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['sgen']['as']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_idle',
        'type': 'gauge',
        'help': 'scheduler threads waiting for work',
        'select': lambda e: [ t for t in e
                              if 'sched' in e[t] and 'idle' in e[t]['sched'] ],
        'samples': {
            '': ('%.6f', lambda t, d: d[t[0:2]]['sched']['idle']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_inq',
        'type': 'gauge',
        'help': 'jobs in run queue',
        'select': lambda e: [ t for t in e
                              if 'sched' in e[t] and 'inq' in e[t]['sched'] ],
        'samples': {
            '': ('%.6f', lambda t, d: d[t[0:2]]['sched']['inq']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_jobs',
        'type': 'counter',
        'help': 'jobs requiring a thread',
        'select': lambda e: [ t for t in e
                              if 'sched' in e[t] and 'jobs' in e[t]['sched'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['sched']['jobs']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_maxinq',
        'type': 'counter',
        'help': 'longest run-queue length',
        'select': lambda e: [ t for t in e
                              if 'sched' in e[t] and 'maxinq' in e[t]['sched'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['sched']['maxinq']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_tcr',
        'type': 'counter',
        'help': 'thread creations',
        'select': lambda e: [ t for t in e
                              if 'sched' in e[t] and 'tcr' in e[t]['sched'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['sched']['tcr']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_tde',
        'type': 'counter',
        'help': 'thread destructions',
        'select': lambda e: [ t for t in e
                              if 'sched' in e[t] and 'tde' in e[t]['sched'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['sched']['tde']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_threads',
        'type': 'gauge',
        'help': 'current scheduler threads',
        'select': lambda e: [ t for t in e
                              if 'sched' in e[t] and 'threads' in e[t]['sched'] ],
        'samples': {
            '': ('%.6f', lambda t, d: d[t[0:2]]['sched']['threads']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_tlimr',
        'type': 'counter',
        'help': 'thread limit attained occurrences',
        'select': lambda e: [ t for t in e
                              if 'sched' in e[t] and 'tlimr' in e[t]['sched'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['sched']['tlimr']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_poll_att',
        'type': 'gauge',
        'help': 'file descriptors attached for polling',
        'select': lambda e: [ t for t in e
                              if 'poll' in e[t] and 'att' in e[t]['poll'] ],
        'samples': {
            '': ('%.6f', lambda t, d: d[t[0:2]]['poll']['att']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_poll_en',
        'type': 'counter',
        'help': 'poll enable ops',
        'select': lambda e: [ t for t in e
                              if 'poll' in e[t] and 'en' in e[t]['poll'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['poll']['en']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_poll_ev',
        'type': 'counter',
        'help': 'polling events',
        'select': lambda e: [ t for t in e
                              if 'poll' in e[t] and 'ev' in e[t]['poll'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['poll']['ev']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_poll_int',
        'type': 'counter',
        'help': 'unsolicited polling events',
        'select': lambda e: [ t for t in e
                              if 'poll' in e[t] and 'int' in e[t]['poll'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['poll']['int']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_proc_sys',
        'unit': 'seconds',
        'type': 'counter',
        'help': 'system time',
        'select': lambda e: [ t for t in e
                              if 'proc' in e[t] and 'sys' in e[t]['proc'] ],
        'samples': {
            '_total': ('%.6f', lambda t, d: d[t[0:2]]['proc']['sys']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_proc_usr',
        'unit': 'seconds',
        'type': 'counter',
        'help': 'user time',
        'select': lambda e: [ t for t in e
                              if 'proc' in e[t] and 'usr' in e[t]['proc'] ],
        'samples': {
            '_total': ('%.6f', lambda t, d: d[t[0:2]]['proc']['usr']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_meta',
        'type': 'info',
        'help': 'reporter metadata',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'role' in e[t]['ofs'] ],
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'role': ('%s', lambda t, d: d[t[0:2]]['ofs']['role']),
        }
    },

    {
        'base': 'xrootd_meta',
        'type': 'info',
        'help': 'server metadata',
        'select': lambda e: [ t for t in e ],
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'site': ('%s', lambda t, d: d[t[0:2]].get('site')),
            'port': ('%s', lambda t, d: d[t[0:2]]['port']),
        }
    },

    {
        'base': 'xrootd_ofs_han',
        'type': 'gauge',
        'help': 'active file handles',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'han' in e[t]['ofs'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['ofs']['han']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_opp',
        'type': 'gauge',
        'help': 'files open in read-write POSC mode',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'opp' in e[t]['ofs'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['ofs']['opp']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_opw',
        'type': 'gauge',
        'help': 'files open in read-write mode',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'opw' in e[t]['ofs'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['ofs']['opw']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_opr',
        'type': 'gauge',
        'help': 'files open in read mode',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'opr' in e[t]['ofs'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['ofs']['opr']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_bxq',
        'type': 'counter',
        'help': 'user time',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'bxq' in e[t]['ofs'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['bxq']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_dly',
        'type': 'counter',
        'help': 'delays imposed',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'dly' in e[t]['ofs'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['dly']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_err',
        'type': 'counter',
        'help': 'errors encountered',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'err' in e[t]['ofs'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['err']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_rdr',
        'type': 'counter',
        'help': 'redirects processed',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'rdr' in e[t]['ofs'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['rdr']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_rep',
        'type': 'counter',
        'help': 'background replies processed',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'rep' in e[t]['ofs'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['rep']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_ser',
        'type': 'counter',
        'help': 'received events indicating failure',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'ser' in e[t]['ofs'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['ser']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_sok',
        'type': 'counter',
        'help': 'received events indicating success',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'sok' in e[t]['ofs'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['sok']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_ups',
        'type': 'counter',
        'help': 'POSC-mode file unpersisted occurrences',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t] and 'ups' in e[t]['ofs'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['ups']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_tpc_grnt',
        'type': 'counter',
        'help': 'TPCs granted',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t]
                              and 'tpc' in e[t]['ofs']
                              and 'grnt' in e[t]['ofs']['tpc'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['tpc']['grnt']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_tpc_deny',
        'type': 'counter',
        'help': 'TPCs denied',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t]
                              and 'tpc' in e[t]['ofs']
                              and 'deny' in e[t]['ofs']['tpc'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['tpc']['deny']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_tpc_err',
        'type': 'counter',
        'help': 'TPCs failed',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t]
                              and 'tpc' in e[t]['ofs']
                              and 'err' in e[t]['ofs']['tpc'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['tpc']['err']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_tpc_exp',
        'type': 'counter',
        'help': 'TPCs with expired auth',
        'select': lambda e: [ t for t in e
                              if 'ofs' in e[t]
                              and 'tpc' in e[t]['ofs']
                              and 'exp' in e[t]['ofs']['tpc'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['ofs']['tpc']['exp']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_num',
        'type': 'counter',
        'help': 'xrootd protocol selections',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'num' in e[t]['xrootd'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['num']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_dly',
        'type': 'counter',
        'help': 'xrootd delayed requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'dly' in e[t]['xrootd'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['dly']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_err',
        'type': 'counter',
        'help': 'xrootd errors encountered',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'err' in e[t]['xrootd'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['err']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_rdr',
        'type': 'counter',
        'help': 'xrootd redirections',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'rdr' in e[t]['xrootd'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['rdr']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_aio_max',
        'type': 'counter',
        'help': 'xrootd maximum simultaneous asynchronous requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'aio' in e[t]['xrootd']
                              and 'max' in e[t]['xrootd']['aio'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['aio']['max']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_aio_num',
        'type': 'counter',
        'help': 'xrootd asynchronous requests processed',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'aio' in e[t]['xrootd']
                              and 'num' in e[t]['xrootd']['aio'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['aio']['num']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_aio_rej',
        'type': 'counter',
        'help': 'xrootd asynchronous requests converted to synchronous',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'aio' in e[t]['xrootd']
                              and 'rej' in e[t]['xrootd']['aio'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['aio']['rej']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_getf',
        'type': 'counter',
        'help': 'xrootd getfile requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'getf' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['getf']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_misc',
        'type': 'counter',
        'help': 'xrootd other requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'misc' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['misc']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_open',
        'type': 'counter',
        'help': 'xrootd file-open requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'open' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['open']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_pr',
        'type': 'counter',
        'help': 'xrootd pre-read requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'pr' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['pr']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_putf',
        'type': 'counter',
        'help': 'xrootd putfile requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'putf' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['putf']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_rf',
        'type': 'counter',
        'help': 'xrootd cache-refresh requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'rf' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['rf']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_rd',
        'type': 'counter',
        'help': 'xrootd read requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'rd' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['rd']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_rs',
        'type': 'counter',
        'help': 'xrootd readv segments',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'rs' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['rs']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_rv',
        'type': 'counter',
        'help': 'xrootd readv requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'rv' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['rv']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_sync',
        'type': 'counter',
        'help': 'xrootd sync requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'sync' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['sync']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_wr',
        'type': 'counter',
        'help': 'xrootd write requests',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'ops' in e[t]['xrootd']
                              and 'wr' in e[t]['xrootd']['ops'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['ops']['wr']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_lgn_num',
        'type': 'counter',
        'help': 'xrootd login attempts',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'lgn' in e[t]['xrootd']
                              and 'num' in e[t]['xrootd']['lgn'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['lgn']['num']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_lgn_af',
        'type': 'counter',
        'help': 'xrootd login failures',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'lgn' in e[t]['xrootd']
                              and 'af' in e[t]['xrootd']['lgn'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['lgn']['af']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_lgn_au',
        'type': 'counter',
        'help': 'xrootd login authentications',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'lgn' in e[t]['xrootd']
                              and 'au' in e[t]['xrootd']['lgn'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['lgn']['au']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_lgn_ua',
        'type': 'counter',
        'help': 'xrootd unauthenticated logins',
        'select': lambda e: [ t for t in e
                              if 'xrootd' in e[t]
                              and 'lgn' in e[t]['xrootd']
                              and 'ua' in e[t]['xrootd']['lgn'] ],
        'samples': {
            '_total': ('%d', lambda t, d: d[t[0:2]]['xrootd']['lgn']['ua']),
            '_created': ('%.3f', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_start_time',
        'unit': 'seconds',
        'type': 'gauge',
        'help': 'time XRootD started',
        'select': lambda e: [ t for t in e if 'start' in e[t] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['start']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_oss_paths_free',
        'unit': 'bytes',
        'type': 'gauge',
        'help': 'available capacity',
        'select': lambda e: [ t + (rp,)
                              for t in e
                              if 'oss' in e[t]
                              and 'paths' in e[t]['oss']
                              for rp in e[t]['oss']['paths'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['oss']['paths'][t[2]]['free']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'rp': ('%s', lambda t, d: t[2]),
            'lp': ('%s', lambda t, d: d[t[0:2]]['oss']['paths'][t[2]]['lp']),
        }
    },

    {
        'base': 'xrootd_oss_paths_tot',
        'unit': 'bytes',
        'type': 'gauge',
        'help': 'total capacity',
        'select': lambda e: [ t + (rp,)
                              for t in e
                              if 'oss' in e[t]
                              and 'paths' in e[t]['oss']
                              for rp in e[t]['oss']['paths'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['oss']['paths'][t[2]]['tot']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'rp': ('%s', lambda t, d: t[2]),
            'lp': ('%s', lambda t, d: d[t[0:2]]['oss']['paths'][t[2]]['lp']),
        }
    },

    {
        'base': 'xrootd_oss_paths_ifr',
        'unit': 'inodes',
        'type': 'gauge',
        'help': 'available capacity',
        'select': lambda e: [ t + (rp,)
                              for t in e
                              if 'oss' in e[t]
                              and 'paths' in e[t]['oss']
                              for rp in e[t]['oss']['paths'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['oss']['paths'][t[2]]['ifr']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'rp': ('%s', lambda t, d: t[2]),
            'lp': ('%s', lambda t, d: d[t[0:2]]['oss']['paths'][t[2]]['lp']),
        }
    },

    {
        'base': 'xrootd_oss_paths_ino',
        'unit': 'inodes',
        'type': 'gauge',
        'help': 'total capacity',
        'select': lambda e: [ t + (rp,)
                              for t in e
                              if 'oss' in e[t]
                              and 'paths' in e[t]['oss']
                              for rp in e[t]['oss']['paths'] ],
        'samples': {
            '': ('%d', lambda t, d: d[t[0:2]]['oss']['paths'][t[2]]['ino']),
        },
        'attrs': {
            'host': ('%s', lambda t, d: t[0]),
            'name': ('%s', lambda t, d: t[1]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'rp': ('%s', lambda t, d: t[2]),
            'lp': ('%s', lambda t, d: d[t[0:2]]['oss']['paths'][t[2]]['lp']),
        }
    },
]



class ReportReceiver:
    def __init__(self, bindprop, hist):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(bindprop)
        self.hist = hist
        self.running = True
        pass

    def keep_polling(self):
        while self.hist.check():
            try:
                self.poll()
                pass
            except KeyboardInterrupt:
                break
            except:
                traceback.print_exc()
                pass
            continue
        pass

    def halt(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    def poll(self):
        ## Receive XML in a UDP packet.
        try:
            (msg, addr) = self.sock.recvfrom(65536)
        except OSError:
            return
        try:
            tree = ElementTree.fromstring(msg)
        except xml.etree.ElementTree.ParseError:
            return
        logging.info('Report from %s:%d' % addr)
        if tree.tag != 'statistics':
            logging.warning('Ignored non-stats %s from %s:%d' %
                            ((tree.tag,) + addr))
            return

        pgm = tree.attrib['pgm']
        if pgm != 'xrootd':
            logging.warning('Ignored non-xrootd program %s from %s:%d' %
                            ((pgm,) + addr))
            return

        ## Extract timestamp data.
        timestamp = int(tree.attrib['tod'])
        start = int(tree.attrib['tos'])

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
            logging.warning('no info from %s:%d' % addr)
            return
        host = blk.find('host').text
        name = blk.find('name').text
        inst = (host, name)
        logging.info('instance %s@%s from %s:%d' % ((name, host) + addr))

        ## Extract other metadata.
        port = int(blk.find('port').text)
        site = tree.attrib['site']

        ## Extract the fields we're interested in.
        data = { }
        data['start'] = start
        data['port'] = port
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
                name = elem.find('./rp').text[1:-1]
                psub = sub.setdefault('paths', { }).setdefault(name, { })
                psub['lp'] = elem.find('./lp').text[1:-1]
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
        self.hist.install( { timestamp: { inst: data } } )
        return

    pass

if __name__ == '__main__':
    import time
    import threading
    from pprint import pprint
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import functools
    import sys
    import os
    from getopt import getopt

    ## Local libraries
    import metrics

    ## This is a sample to populate the history with for test
    ## purposes.
    sample_now = time.time()
    sample = {
        sample_now: {
            ('xrootd-server', 'main'): {
                'buff': {
                    'adj': 0,
                    'buffs': 664,
                    'mem': 501411840,
                    'reqs': 170749
                },
                'link': {
                    'ctime': 2622954,
                    'in': 2386464921,
                    'maxn': 412,
                    'num': 203,
                    'out': 4940357118960,
                    'sfps': 0,
                    'stall': 0,
                    'tmo': 0,
                    'tot': 89157
                },
                'ofs': {
                    'bxq': 0,
                    'dly': 0,
                    'err': 0,
                    'han': 37,
                    'opp': 0,
                    'opr': 23,
                    'opw': 14,
                    'rdr': 0,
                    'rep': 0,
                    'role': 'server',
                    'ser': 0,
                    'sok': 0,
                    'tpc': {
                        'deny': 0,
                        'err': 0,
                        'exp': 0,
                        'grnt': 0
                    },
                    'ups': 0
                },
                'oss': {
                    'paths': {
                        '/cephfs': {
                            'free': 9991486255104,
                            'ifr': -1,
                            'ino': 315834240,
                            'lp': '/cephfs',
                            'tot': 10966251003904
                        }
                    }
                },
                'poll': {
                    'att': 203,
                    'en': 213581,
                    'ev': 213427,
                    'int': 0
                },
                'proc': {
                    'sys': 11107.215161,
                    'usr': 10124.33064
                },
                'sched': {
                    'idle': 11,
                    'inq': 0,
                    'jobs': 315890,
                    'maxinq': 4,
                    'tcr': 237,
                    'tde': 180,
                    'threads': 57,
                    'tlimr': 0
                },
                'sgen': {
                    'as': 1,
                    'et': 6,
                    'toe': 1651696117
                },
                'start': sample_now - 16801,
                'xrootd': {
                    'aio': {
                        'max': 0,
                        'num': 0,
                        'rej': 0
                    },
                    'dly': 0,
                    'err': 36732,
                    'lgn': {
                        'af': 0,
                        'au': 30,
                        'num': 1327,
                        'ua': 0
                    },
                    'num': 70231,
                    'ops': {
                        'getf': 0,
                        'misc': 249972,
                        'open': 13242,
                        'pr': 0,
                        'putf': 0,
                        'rd': 4705135,
                        'rf': 0,
                        'rs': 0,
                        'rv': 0,
                        'sync': 0,
                        'wr': 1425,
                        'ws': 0,
                        'wv': 0
                    },
                    'rdr': 0,
                    'sig': {
                        'bad': 0,
                        'ign': 0,
                        'ok': 0
                    }
                }
            }
        }
    }

    udp_host = ''
    udp_port = 9485
    http_host = 'localhost'
    http_port = 8744
    horizon = 60 * 30
    silent = False
    fake_data = False
    endpoint = None
    log_params = {
        'format': '%(asctime)s %(message)s',
        'datefmt': '%Y-%d-%mT%H:%M:%S',
    }
    opts, args = getopt(sys.argv[1:], "zh:u:U:t:T:E:X",
                        [ 'log=', 'log-file=' ])
    for opt, val in opts:
        if opt == '-h':
            horizon = int(val) * 60
        elif opt == '-z':
            silent = True
        elif opt == '-u':
            udp_port = int(val)
        elif opt == '-U':
            udp_host = val
        elif opt == '-E':
            endpoint = val
        elif opt == '-t':
            http_port = int(val)
        elif opt == '-T':
            http_host = val
        elif opt == '--log':
            log_params['level'] = getattr(logging, val.upper(), None)
            if not isinstance(log_params['level'], int):
                sys.stderr.write('bad log level [%s]\n' % val)
                sys.exit(1)
                pass
            pass
        elif opt == '--log-file':
            log_params['filename'] = val
        elif opt == '-X':
            fake_data = True
            pass
        continue

    if silent:
        with open('/dev/null', 'w') as devnull:
            fd = devnull.fileno()
            os.dup2(fd, sys.stdout.fileno())
            os.dup2(fd, sys.stderr.fileno())
            pass
        pass

    logging.basicConfig(**log_params)

    ## Record XRootD stats history, indexed by timestamp and instance.
    ## Alternatively, prepare to push stats as soon as they're
    ## converted.
    rmw = history = metrics.MetricHistory(schema, horizon=horizon)
    if endpoint is not None:
        rmw = metrics.RemoteMetricsWriter(endpoint=endpoint,
                                          schema=schema,
                                          job='xrootd',
                                          expiry=10*60)
    elif fake_data:
        history.install(sample)
        receiver = None
        pass

    ## Serve the history on demand.  Even if we don't store anything
    ## in the history, the HELP, TYPE and UNIT strings are exposed,
    ## which doesn't seem to be possible with remote-write.
    partial_handler = functools.partial(metrics.MetricsHTTPHandler,
                                        hist=history)
    webserver = HTTPServer((http_host, http_port), partial_handler)
    logging.info('Created HTTP server on http://%s:%d' %
                 (http_host, http_port))

    if endpoint is not None or not fake_data:
        ## Create a UDP socket to listen on, convert XML stats from
        ## XRootD into timestamped metrics, and drop them into the
        ## history/remote writer.
        logging.info('Creating UDP XRootD receiver on %s:%d' %
                     (udp_host, udp_port))
        receiver = ReportReceiver((udp_host, udp_port), rmw)
        pass

    ## Use a separate thread to run the server, which we can stop by
    ## calling shutdown().
    srv_thrd = threading.Thread(target=HTTPServer.serve_forever,
                                args=(webserver,))
    srv_thrd.start()

    try:
        receiver.keep_polling()
    except KeyboardInterrupt:
        pass

    history.halt()
    logging.info('Halted history')
    webserver.shutdown()
    logging.info('Halted webserver')
    receiver.halt()
    logging.info('Halted receiver')
    webserver.server_close()
    logging.info('Server stopped.')
    pass
