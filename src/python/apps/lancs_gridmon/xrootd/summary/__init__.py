## Copyright (c) 2022-2025, Lancaster University
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

schema = [
    {
        'base': 'xrootd_buff_mem',
        'type': 'gauge',
        'unit': 'bytes',
        'help': 'bytes allocated to buffers',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'buff' in e['summary'][t] and 'mem' in e['summary'][t]['buff']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['buff']['mem']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_up',
        'type': 'gauge',
        'help': 'whether an XRootD instance has reported',
        'select': lambda e: [
            t
            for t in e['summary']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_buff_buffs',
        'type': 'gauge',
        'help': 'number of allocated buffers',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'buff' in e['summary'][t] and 'buffs' in e['summary'][t]['buff']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['buff']['buffs']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_buff_adj',
        'type': 'counter',
        'help': 'number of buffer profile adjustments',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'buff' in e['summary'][t] and 'adj' in e['summary'][t]['buff']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['buff']['adj']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_buff_reqs',
        'type': 'counter',
        'help': 'number of buffer requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'buff' in e['summary'][t] and 'reqs' in e['summary'][t]['buff']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['buff']['reqs']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_num',
        'type': 'gauge',
        'help': 'number of concurrent connections',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'num' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['link']['num']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_ctime',
        'unit': 'seconds',
        'type': 'counter',
        'help': 'cummulative time spent in connections',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'ctime' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%.3f', lambda t, d: d['summary'][t[0:3]] \
                       ['link']['ctime']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_maxn',
        'type': 'counter',
        'help': 'maximum number of concurrent connections',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'maxn' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['link']['maxn']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_tot',
        'type': 'counter',
        'help': 'total number of connections',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'tot' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['link']['tot']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_tmo',
        'type': 'counter',
        'help': 'read request timeouts',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'tmo' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['link']['tmo']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_stall',
        'type': 'counter',
        'help': 'partial data received occurences',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'stall' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['link']['stall']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_sfps',
        'type': 'counter',
        'help': 'partial sendfile ops',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'sfps' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['link']['sfps']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_in',
        'unit': 'bytes',
        'type': 'counter',
        'help': 'bytes received',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'in' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['link']['in']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_link_out',
        'unit': 'bytes',
        'type': 'counter',
        'help': 'bytes received',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'link' in e['summary'][t] and 'out' in e['summary'][t]['link']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['link']['out']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sgen_et',
        'unit': 'seconds',
        'type': 'gauge',
        'help': 'elapsed time for stats completion',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sgen' in e['summary'][t] and 'et' in e['summary'][t]['sgen']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%.6f', lambda t, d: d['summary'][t[0:3]]['sgen']['et'] / 1e6),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sgen_toe',
        'unit': 'seconds',
        'type': 'gauge',
        'help': 'stats completion time',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sgen' in e['summary'][t] and 'toe' in e['summary'][t]['sgen']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['sgen']['toe']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sgen_as',
        'type': 'gauge',
        'help': 'asynchronous stats',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sgen' in e['summary'][t] and 'as' in e['summary'][t]['sgen']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['sgen']['as']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_idle',
        'type': 'gauge',
        'help': 'scheduler threads waiting for work',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sched' in e['summary'][t] and 'idle' in e['summary'][t]['sched']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%.6f', lambda t, d: d['summary'][t[0:3]]['sched']['idle']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_inq',
        'type': 'gauge',
        'help': 'jobs in run queue',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sched' in e['summary'][t] and 'inq' in e['summary'][t]['sched']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%.6f', lambda t, d: d['summary'][t[0:3]]['sched']['inq']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_jobs',
        'type': 'counter',
        'help': 'jobs requiring a thread',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sched' in e['summary'][t] and 'jobs' in e['summary'][t]['sched']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['sched']['jobs']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_maxinq',
        'type': 'counter',
        'help': 'longest run-queue length',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sched' in e['summary'][t]
            and 'maxinq' in e['summary'][t]['sched']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['sched']['maxinq']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_tcr',
        'type': 'counter',
        'help': 'thread creations',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sched' in e['summary'][t] and 'tcr' in e['summary'][t]['sched']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['sched']['tcr']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_tde',
        'type': 'counter',
        'help': 'thread destructions',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sched' in e['summary'][t] and 'tde' in e['summary'][t]['sched']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['sched']['tde']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_threads',
        'type': 'gauge',
        'help': 'current scheduler threads',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sched' in e['summary'][t]
            and 'threads' in e['summary'][t]['sched']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%.6f', lambda t, d: d['summary'][t[0:3]]['sched']['threads']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_sched_tlimr',
        'type': 'counter',
        'help': 'thread limit attained occurrences',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'sched' in e['summary'][t]
            and 'tlimr' in e['summary'][t]['sched']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['sched']['tlimr']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_poll_att',
        'type': 'gauge',
        'help': 'file descriptors attached for polling',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'poll' in e['summary'][t] and 'att' in e['summary'][t]['poll']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%.6f', lambda t, d: d['summary'][t[0:3]]['poll']['att']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_poll_en',
        'type': 'counter',
        'help': 'poll enable ops',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'poll' in e['summary'][t] and 'en' in e['summary'][t]['poll']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['poll']['en']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_poll_ev',
        'type': 'counter',
        'help': 'polling events',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'poll' in e['summary'][t] and 'ev' in e['summary'][t]['poll']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['poll']['ev']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_poll_int',
        'type': 'counter',
        'help': 'unsolicited polling events',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'poll' in e['summary'][t] and 'int' in e['summary'][t]['poll']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['poll']['int']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_proc_sys',
        'unit': 'seconds',
        'type': 'counter',
        'help': 'system time',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'proc' in e['summary'][t] and 'sys' in e['summary'][t]['proc']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%.6f', lambda t, d: d['summary'][t[0:3]] \
                       ['proc']['sys']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_proc_usr',
        'unit': 'seconds',
        'type': 'counter',
        'help': 'user time',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'proc' in e['summary'][t] and 'usr' in e['summary'][t]['proc']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%.6f', lambda t, d: d['summary'][t[0:3]] \
                       ['proc']['usr']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_meta',
        'type': 'info',
        'help': 'reporter metadata',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'role' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'role': ('%s', lambda t, d: d['summary'][t[0:3]]['ofs']['role']),
        }
    },

    {
        'base': 'xrootd_cms_meta',
        'type': 'info',
        'help': 'CMS metadata',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'cms' in e['summary'][t] and 'role' in e['summary'][t]['cms']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'role': ('%s', lambda t, d: d['summary'][t[0:3]]['cms']['role']),
        }
    },

    {
        'base': 'xrootd_meta',
        'type': 'info',
        'help': 'server metadata',
        'select': lambda e: [
            t
            for t in e['summary']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: 1),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'site': ('%s', lambda t, d: d['summary'][t[0:3]].get('site')),
            'port': ('%s', lambda t, d: d['summary'][t[0:3]]['port']),
            'ver': ('%s', lambda t, d: d['summary'][t[0:3]]['ver']),
        }
    },

    {
        'base': 'xrootd_ofs_han',
        'type': 'gauge',
        'help': 'active file handles',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'han' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['han']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_opp',
        'type': 'gauge',
        'help': 'files open in read-write POSC mode',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'opp' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['opp']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_opw',
        'type': 'gauge',
        'help': 'files open in read-write mode',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'opw' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['opw']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_opr',
        'type': 'gauge',
        'help': 'files open in read mode',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'opr' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['opr']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_bxq',
        'type': 'counter',
        'help': 'user time',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'bxq' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['bxq']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_dly',
        'type': 'counter',
        'help': 'delays imposed',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'dly' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['dly']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_err',
        'type': 'counter',
        'help': 'errors encountered',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'err' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['err']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_rdr',
        'type': 'counter',
        'help': 'redirects processed',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'rdr' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['rdr']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_rep',
        'type': 'counter',
        'help': 'background replies processed',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'rep' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['rep']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_ser',
        'type': 'counter',
        'help': 'received events indicating failure',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'ser' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['ser']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_sok',
        'type': 'counter',
        'help': 'received events indicating success',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'sok' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['sok']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_ups',
        'type': 'counter',
        'help': 'POSC-mode file unpersisted occurrences',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t] and 'ups' in e['summary'][t]['ofs']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]]['ofs']['ups']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_tpc_grnt',
        'type': 'counter',
        'help': 'TPCs granted',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t]
            and 'tpc' in e['summary'][t]['ofs']
            and 'grnt' in e['summary'][t]['ofs']['tpc']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['ofs']['tpc']['grnt']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_tpc_deny',
        'type': 'counter',
        'help': 'TPCs denied',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t]
            and 'tpc' in e['summary'][t]['ofs']
            and 'deny' in e['summary'][t]['ofs']['tpc']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['ofs']['tpc']['deny']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_tpc_err',
        'type': 'counter',
        'help': 'TPCs failed',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t]
            and 'tpc' in e['summary'][t]['ofs']
            and 'err' in e['summary'][t]['ofs']['tpc']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['ofs']['tpc']['err']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_ofs_tpc_exp',
        'type': 'counter',
        'help': 'TPCs with expired auth',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'ofs' in e['summary'][t]
            and 'tpc' in e['summary'][t]['ofs']
            and 'exp' in e['summary'][t]['ofs']['tpc']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['ofs']['tpc']['exp']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_num',
        'type': 'counter',
        'help': 'xrootd protocol selections',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'num' in e['summary'][t]['xrootd']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['num']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_dly',
        'type': 'counter',
        'help': 'xrootd delayed requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'dly' in e['summary'][t]['xrootd']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['dly']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_err',
        'type': 'counter',
        'help': 'xrootd errors encountered',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'err' in e['summary'][t]['xrootd']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['err']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_rdr',
        'type': 'counter',
        'help': 'xrootd redirections',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'rdr' in e['summary'][t]['xrootd']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['rdr']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_aio_max',
        'type': 'counter',
        'help': 'xrootd maximum simultaneous asynchronous requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'aio' in e['summary'][t]['xrootd']
            and 'max' in e['summary'][t]['xrootd']['aio']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['aio']['max']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_aio_num',
        'type': 'counter',
        'help': 'xrootd asynchronous requests processed',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'aio' in e['summary'][t]['xrootd']
            and 'num' in e['summary'][t]['xrootd']['aio']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['aio']['num']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_aio_rej',
        'type': 'counter',
        'help': 'xrootd asynchronous requests converted to synchronous',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'aio' in e['summary'][t]['xrootd']
            and 'rej' in e['summary'][t]['xrootd']['aio']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['aio']['rej']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_getf',
        'type': 'counter',
        'help': 'xrootd getfile requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'getf' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['getf']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_misc',
        'type': 'counter',
        'help': 'xrootd other requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'misc' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['misc']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_open',
        'type': 'counter',
        'help': 'xrootd file-open requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'open' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['open']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_pr',
        'type': 'counter',
        'help': 'xrootd pre-read requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'pr' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['pr']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_putf',
        'type': 'counter',
        'help': 'xrootd putfile requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'putf' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['putf']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_rf',
        'type': 'counter',
        'help': 'xrootd cache-refresh requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'rf' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['rf']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_rd',
        'type': 'counter',
        'help': 'xrootd read requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'rd' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['rd']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_rs',
        'type': 'counter',
        'help': 'xrootd readv segments',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'rs' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['rs']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_rv',
        'type': 'counter',
        'help': 'xrootd readv requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'rv' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['rv']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_sync',
        'type': 'counter',
        'help': 'xrootd sync requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'sync' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['sync']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_ops_wr',
        'type': 'counter',
        'help': 'xrootd write requests',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'ops' in e['summary'][t]['xrootd']
            and 'wr' in e['summary'][t]['xrootd']['ops']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['ops']['wr']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_lgn_num',
        'type': 'counter',
        'help': 'xrootd login attempts',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'lgn' in e['summary'][t]['xrootd']
            and 'num' in e['summary'][t]['xrootd']['lgn']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['lgn']['num']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_lgn_af',
        'type': 'counter',
        'help': 'xrootd login failures',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'lgn' in e['summary'][t]['xrootd']
            and 'af' in e['summary'][t]['xrootd']['lgn']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['lgn']['af']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_lgn_au',
        'type': 'counter',
        'help': 'xrootd login authentications',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'lgn' in e['summary'][t]['xrootd']
            and 'au' in e['summary'][t]['xrootd']['lgn']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['lgn']['au']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_xrootd_lgn_ua',
        'type': 'counter',
        'help': 'xrootd unauthenticated logins',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'xrootd' in e['summary'][t]
            and 'lgn' in e['summary'][t]['xrootd']
            and 'ua' in e['summary'][t]['xrootd']['lgn']
        ] if 'summary' in e else list(),
        'samples': {
            '_total': ('%d', lambda t, d: d['summary'][t[0:3]] \
                       ['xrootd']['lgn']['ua']),
            '_created': ('%.3f', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_start_time',
        'unit': 'seconds',
        'type': 'gauge',
        'help': 'time XRootD started',
        'select': lambda e: [
            t
            for t in e['summary']
            if 'start' in e['summary'][t]
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]]['start']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
        }
    },

    {
        'base': 'xrootd_oss_paths_free',
        'unit': 'bytes',
        'type': 'gauge',
        'help': 'available capacity',
        'select': lambda e: [
            t + (lp,)
            for t in e['summary']
            if 'oss' in e['summary'][t]
            and 'paths' in e['summary'][t]['oss']
            for lp in e['summary'][t]['oss']['paths']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]] \
                 ['oss']['paths'][t[3]]['free']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'lp': ('%s', lambda t, d: t[3]),
            'rp': ('%s', lambda t, d: d['summary'][t[0:3]] \
                   ['oss']['paths'][t[3]]['rp']),
        }
    },

    {
        'base': 'xrootd_oss_paths_tot',
        'unit': 'bytes',
        'type': 'gauge',
        'help': 'total capacity',
        'select': lambda e: [
            t + (lp,)
            for t in e['summary']
            if 'oss' in e['summary'][t]
            and 'paths' in e['summary'][t]['oss']
            for lp in e['summary'][t]['oss']['paths']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]] \
                 ['oss']['paths'][t[3]]['tot']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'lp': ('%s', lambda t, d: t[3]),
            'rp': ('%s', lambda t, d: d['summary'][t[0:3]] \
                   ['oss']['paths'][t[3]]['rp']),
        }
    },

    {
        'base': 'xrootd_oss_paths_ifr',
        'unit': 'inodes',
        'type': 'gauge',
        'help': 'available capacity',
        'select': lambda e: [
            t + (lp,)
            for t in e['summary']
            if 'oss' in e['summary'][t]
            and 'paths' in e['summary'][t]['oss']
            for lp in e['summary'][t]['oss']['paths']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]] \
                 ['oss']['paths'][t[3]]['ifr']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'lp': ('%s', lambda t, d: t[3]),
            'rp': ('%s', lambda t, d: d['summary'][t[0:3]] \
                   ['oss']['paths'][t[3]]['rp']),
        }
    },

    {
        'base': 'xrootd_oss_paths_ino',
        'unit': 'inodes',
        'type': 'gauge',
        'help': 'total capacity',
        'select': lambda e: [
            t + (lp,)
            for t in e['summary']
            if 'oss' in e['summary'][t]
            and 'paths' in e['summary'][t]['oss']
            for lp in e['summary'][t]['oss']['paths']
        ] if 'summary' in e else list(),
        'samples': {
            '': ('%d', lambda t, d: d['summary'][t[0:3]] \
                 ['oss']['paths'][t[3]]['ino']),
        },
        'attrs': {
            # 'host': ('%s', lambda t, d: t[0]),
            # 'name': ('%s', lambda t, d: t[1]),
            'pgm': ('%s', lambda t, d: t[2]),
            'xrdid': ('%s@%s', lambda t, d: t[1], lambda t, d: t[0]),
            'lp': ('%s', lambda t, d: t[3]),
            'rp': ('%s', lambda t, d: d['summary'][t[0:3]] \
                   ['oss']['paths'][t[3]]['rp']),
        }
    },
]
