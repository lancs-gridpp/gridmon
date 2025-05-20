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

import json
import struct
import sys
import os
import re

_uriarg_fmt = re.compile(r'&([^=]+)=([^&]*)')

_mapping_kind = {
    '=': 'server',
    'd': 'user-path',
    'i': 'user-info',
    'u': 'log-auth',
    'p': 'file-purge',
    'U': 'expm',
    'T': 'token',
    'x': 'xfer',
}

_auth_fields = { 'g': 'grp', 'o': 'org', 'r': 'role' }

_xfr_ops = {
    0: 'unk_op',
    1: 'cli_cp_in',
    2: 'migr_cp_out',
    3: 'migr_cp_out_rm',
    4: 'cli_cp_out',
    5: 'cli_cp_out_rm',
    6: 'stg_cp_in',
}

_map_keys = {
    '=': dict(),
    'd': dict(),
    'i': dict(),
    'p': {
        'tod': 'ts',
        'sz': 'size',
        'at': 'tm_acc',
        'mt': 'tm_mod',
        'ct': 'tm_cre',
    },
    'T': {
        'Uc': 'user_dictid',
        's': 'subj',
        'n': 'mapped_user',
    },
    'u': {
        'p': 'proto',
        'h': 'host_addr',
        'x': 'exec',
        'y': 'cli_env',
        'I': 'ip_vers',
        'm': 'dn',
        'n': 'username',
    },
    'U': {
        'Uc': 'user_dictid',
        'Ec': 'expm_code',
        'Ac': 'actvt_code',
    },
    'x': {
        'tod': 'ts',
        'tm': 'migr_stg_dur',
        'rc': 'exit',
        'sz': 'size',
        'pd': 'mon_ext',
    },
}

_redir_ops = {
    0x01: 'chmod',
    0x02: 'locate',
    0x03: 'opendir',
    0x04: 'openc',
    0x05: 'openr',
    0x06: 'openw',
    0x07: 'mkdir',
    0x08: 'mv',
    0x09: 'prep',
    0x0a: 'query',
    0x0b: 'rm',
    0x0c: 'rmdir',
    0x0d: 'stat',
    0x0e: 'trunc',
}

def _u8(buf, off):
    return int(buf[off]) & 0xff

def _u16(buf, off):
    return struct.unpack('>H', buf[off:off+2])[0] & 0xffff

def _u32(buf, off):
    return struct.unpack('>I', buf[off:off+4])[0] & 0xffffffff

def _s32(buf, off):
    return struct.unpack('>i', buf[off:off+4])[0]

def _u64(buf, off):
    return struct.unpack('>Q', buf[off:off+8])[0] & 0xffffffffffffffff

def _ieee754(buf, off):
    return struct.unpack('>d', buf[off:off+8])[0]

def _get_stat_xfr(dat, buf, off):
    dat['read_bytes'] = _u64(buf, off)
    dat['readv_bytes'] = _u64(buf, off + 8)
    dat['write_bytes'] = _u64(buf, off + 16)
    pass

_redir_fmt = re.compile(r'^(\[[^]]+\]|[^:]+)?:(.*)$')

def _decompose_server_path(d, k):
    txt = d.get(k)
    if txt is None:
        return False
    m = _redir_fmt.match(txt)
    if not m:
        return False
    srv, pth = m.groups()
    if srv is not None:
        d[k + '_server'] = srv
        pass
    d[k + '_path'] = pth
    del d[k]
    return True

def _decode_null_term(buf):
    for i in range(0, len(buf)):
        if buf[i] == 0:
            break
        continue
    return buf[0:i].decode('ascii')

_swvers_fmt = re.compile(r'^([^/]+)/(.+)$')

def _software_version(txt):
    name, vers = _swvers_fmt.match(txt).groups()
    return { 'name': name, 'version': vers }

def _parse_field(d, k, f):
    v = d.get(k)
    if v is None:
        return False
    d[k] = f(v)
    return True

def _integrate_field(d, k):
    return _parse_field(d, k, int)

def _reify_field(d, k):
    return _parse_field(d, k, float)

def _expand_keys(d, spec):
    if spec is None:
        return
    for oldk, newk in spec.items():
        v = d.get(oldk)
        if v is None:
            continue
        d[newk] = v
        del d[oldk]
        continue
    pass

def _interleave_arrays(d, name, spec):
    n = 0
    for k in spec:
        a = d.get(k)
        if a is None:
            continue
        n = max(n, len(a))
        continue
    r = list()
    for i in range(0, n):
        e = dict()
        for k, k2 in spec.items():
            s = d.get(k)
            if s is None or i >= len(s):
                continue
            v = s[i]
            if v != 'NULL':
                e[k2] = s[i]
                pass
            continue
        r.append(e)
        continue
    if len(r) > 0:
        d[name] = r
        pass
    for k in spec:
        d.pop(k, None)
        continue
    pass

_userid_fmt = re.compile(r'^(?:([^/]+)/)?([^.]+)\.([^:]+):([^@]+)@(.*)')

def _decompose_userid(d, k, sid_name='sid', pid_name='sess'):
    v = d.get(k)
    if v is None:
        return False
    prot, user, pid, sid, host = _userid_fmt.match(v).groups()
    d[k] = {
        'user': user,
        pid_name: int(pid),
        sid_name: int(sid),
        'host': host,
        'orig': v,
    }
    if prot is not None:
        d[k]['prot'] = prot
        pass
    return True

def _decompose_array(d, k):
    v = d.get(k)
    if v is None:
        return False
    d[k] = v.split()
    return True

def _numeralize_time(d, k, fmt='%Y-%m-%dT%H:%M:%S.%fZ'):
    import datetime
    n = d.get(k)
    if n is None:
        return False
    dt = datetime.datetime.strptime(n, fmt).timestamp()
    d[k + '_unix'] = dt
    return True

def _humanize_timestamp(d, k, fmt='%Y-%m-%dT%H:%M:%S.%f%z'):
    import datetime
    n = d.get(k)
    if n is None:
        return False
    dt = datetime.datetime.utcfromtimestamp(n)
    d[k + '_human'] = dt.strftime(fmt)
    return True

def _humanize_buffer(d, k):
    v = d.get(k)
    if v is None:
        return False
    s1 = ''
    s2 = ''
    for c in v:
        s1 += ' %02X' % int(c)
        continue
    d[k + '_octets'] = s1[1:]
    d[k + '_escaped'] = v.decode('ascii', errors='replace')
    del d[k]
    return True

def decode_message(ts, addr, buf):
    result = {
        'ts': ts,
        'peer': {
            'host': addr[0],
            'port': addr[1],
        },
    }
    _humanize_timestamp(result, 'ts')

    if len(buf) < 8:
        result['error'] = 'too-short'
        result['remn'] = buf
        _humanize_buffer(result, 'remn')
    else:
        msg = result['message'] = dict()
        code = msg['code'] = buf[0:1].decode('ascii')
        msg['pseq'] = _u8(buf, 1)
        msg['plen'] = _u16(buf, 2)
        msg['stod'] = _u32(buf, 4)
        _humanize_timestamp(msg, 'stod', '%Y-%m-%dT%H:%M:%S%z')
        buf = buf[8:]

        if code == 'f':
            msg['type'] = 'file'
            fstr = msg['data'] = list()
            while len(buf) > 4:
                rent = dict()
                fstr.append(rent)
                rtype = rent['type'] = _u8(buf, 0)
                rflags = rent['flags'] = _u8(buf, 1)
                rlen = rent['len'] = _u16(buf, 2)
                rbuf = buf[4:rlen]
                buf = buf[rlen:]

                if rtype == 2: # time
                    dat = rent['time'] = dict()
                    dat['nxfr'] = _u16(rbuf, 0)
                    dat['ntot'] = _u16(rbuf, 2)
                    dat['tbeg'] = _u32(rbuf, 4)
                    dat['tend'] = _u32(rbuf, 8)
                    if rflags & 0x01:
                        ## The server fingerprint is only present if
                        ## this bit is set.
                        sid = _u64(rbuf, 12)
                        disused = sid >> 48
                        sid = sid & 0xffffffffffff
                        dat['sid'] = sid
                        dat['sid_unused'] = disused
                        rbuf = rbuf[20:]
                    else:
                        rbuf = rbuf[12:]
                        pass
                elif rtype == 4: # disc
                    dat = rent['disc'] = dict()
                    dat['user_dictid'] = _u32(rbuf, 0)
                    rbuf = rbuf[4:]
                elif rtype == 1: # open
                    dat = rent['open'] = dict()
                    dat['file_dictid'] = _u32(rbuf, 0)
                    dat['file_size'] = _u64(rbuf, 4)
                    dat['rw'] = (rflags & 0x02) != 0
                    if rflags & 0x01:
                        dat['user_dictid'] = _u32(rbuf, 12)
                        dat['lfn'] = _decode_null_term(rbuf[16:])
                        rbuf = b''
                    else:
                        rbuf = rbuf[12:]
                        pass
                elif rtype == 0: # close
                    dat = rent['close'] = dict()
                    dat['forced'] = rflags & 0x01 != 0
                    did = dat['file_dictid'] = _u32(rbuf, 0)
                    _get_stat_xfr(dat, rbuf, 4)
                    rbuf = rbuf[28:]

                    if rflags & 0x02:
                        dat['read_calls'] = _u32(rbuf, 0)
                        dat['readv_calls'] = _u32(rbuf, 4)
                        dat['write_calls'] = _u32(rbuf, 8)
                        dat['readv_segs_min'] = _u16(rbuf, 12)
                        dat['readv_segs_max'] = _u16(rbuf, 14)
                        dat['readv_segs'] = _u64(rbuf, 16)
                        dat['read_size_min'] = _u32(rbuf, 24)
                        dat['read_size_max'] = _u32(rbuf, 28)
                        dat['readv_size_min'] = _u32(rbuf, 32)
                        dat['readv_size_max'] = _u32(rbuf, 36)
                        dat['write_size_min'] = _u32(rbuf, 40)
                        dat['write_size_max'] = _u32(rbuf, 44)
                        rbuf = rbuf[48:]
                        pass

                    if rflags & 0x04:
                        dat['read_bytes_sq'] = _ieee754(rbuf, 0)
                        dat['readv_bytes_sq'] = _ieee754(rbuf, 8)
                        dat['read_count_sq'] = _ieee754(rbuf, 16)
                        dat['write_bytes_sq'] = _ieee754(rbuf, 24)
                        rbuf = rbuf[32:]
                        pass
                    pass
                elif rtype == 3: # xfr
                    dat = rent['close'] = dict()
                    did = dat['file_dictid'] = _u32(rbuf, 0)
                    _get_stat_xfr(dat, rbuf, 4)
                    rbuf = rbuf[28:]
                    pass

                if len(rbuf) > 0:
                    rent['remn'] = rbuf
                    _humanize_buffer(rent, 'remn')
                    pass

                continue
        elif code == 'g':
            msg['type'] = 'gstream'
            gstr = msg['data'] = dict()
            tbeg = gstr['time_begin'] = _u32(buf, 0)
            tend = gstr['time_end'] = _u32(buf, 4)
            sid = _u64(buf, 8)
            prov = gstr['provider'] = chr(sid >> 56)
            gstr['unused_byte'] = (sid >> 48) & 0xff
            gstr['sid'] = sid & 0xffffffffffff
            lines = buf[16:].decode('ascii').splitlines()

            if prov == 'C':
                badlines = list()
                gent = gstr['cache'] = list()
                for line in lines:
                    try:
                        dat = json.loads(line.rstrip('\0'))
                        gent.append(dat)
                    except json.JSONDecodeError as e:
                        gent.append(e)
                        badlines.append(line)
                        pass
                    continue
                lines = badlines
            elif prov == 'P':
                badlines = list()
                gent = gstr['tpc'] = list()
                for line in lines:
                    try:
                        dat = json.loads(line.rstrip('\0'))
                        xeq = dat.get('Xeq')
                        if xeq is not None:
                            _numeralize_time(xeq, 'Beg')
                            _numeralize_time(xeq, 'End')
                            pass
                        _decompose_userid(dat, 'Client', sid_name='socket',
                                         pid_name='process')
                        gent.append(dat)
                    except json.JSONDecodeError as e:
                        gent.append(e)
                        badlines.append(line)
                        pass
                    continue
                lines = badlines
                pass

            if len(lines) > 0:
                gstr['lines'] = lines
                pass
            buf = b''
        elif code == 't':
            msg['type'] = 'traces'
            trc = msg['data'] = list()
            while len(buf) >= 16:
                rbuf = buf[0:16]
                buf = buf[16:]

                rent = dict()
                trc.append(rent)
                typ = _u8(rbuf, 0)
                if typ == 0x80:
                    rent['type'] = 'open'
                    rent['len'] = _u64(rbuf, 0) & 0xffffffffffffff
                    rent['resv_8_12'] = rbuf[8:12]
                    _humanize_buffer(rent, 'resv_8_12')
                    rent['file_dictid'] = _u32(rbuf, 12)
                elif typ == 0x90 or typ == 0x91:
                    rent['type'] = 'readv' if typ == 0x90 else 'readu'
                    rent['reqid'] = _u8(rbuf, 1)
                    rent['nsegs'] = _u16(rbuf, 2)
                    rent['resv_4_8'] = rbuf[4:8]
                    _humanize_buffer(rent, 'resv_4_8')
                    rent['len'] = _s32(rbuf, 8)
                    rent['file_dictid'] = _s32(rbuf, 12)
                elif typ == 0xa0:
                    rent['type'] = 'appid'
                    rent['name'] = _decode_null_term(rbuf[4:16])
                    rent['resv_1_4'] = rbuf[1:4]
                    _humanize_buffer(rent, 'resv_1_4')
                elif typ == 0xc0:
                    rent['type'] = 'close'
                    rtotsh = _u8(rbuf, 1)
                    wtotsh = _u8(rbuf, 2)
                    rent['resv_3_4'] = rbuf[3:4]
                    _humanize_buffer(rent, 'resv_3_4')
                    rent['rtot'] = _u32(rbuf, 4) << rtotsh
                    rent['wtot'] = _u32(rbuf, 8) << wtotsh
                    rent['file_dictid'] = _s32(rbuf, 12)
                elif typ == 0xd0:
                    rent['type'] = 'disc'
                    rent['forced'] = (_u8(rbuf, 1) & 0x01) != 0
                    rent['boundp'] = (_u8(rbuf, 1) & 0x02) != 0
                    rent['resv_2_8'] = rbuf[2:8]
                    _humanize_buffer(rent, 'resv_2_8')
                    rent['dur'] = _s32(rbuf, 8)
                    rent['file_dictid'] = _u32(rbuf, 12)
                elif typ == 0xe0:
                    rent['type'] = 'window'
                    rent['sid'] =_u64(rbuf, 0) & 0xffffffffffff
                    rent['resv_1_2'] = rbuf[1:2]
                    _humanize_buffer(rent, 'resv_1_2')
                    rent['g0'] = _u32(rbuf, 8)
                    rent['g1'] = _u32(rbuf, 12)
                elif typ <= 0x7f:
                    rent['off'] = _u64(rbuf, 0) & 0xffffffffffffff
                    blen = rent['len'] = _s32(rbuf, 8)
                    if blen < 0:
                        rent['type'] = 'write_rq'
                        rent['len'] = -blen
                    else:
                        rent['type'] = 'read_rq'
                        rent['len'] = blen
                        pass
                    rent['file_dictid'] = _u32(rbuf, 12)
                else:
                    rent['type'] = 'unk_%02X' % typ
                    rent['resv_1_16'] = rbuf[1:]
                    _humanize_buffer(rent, 'resv_1_16')
                    pass
                # rent['resv_0_16'] = rbuf
                # _humanize_buffer(rent, 'resv_0_16')
                continue
            pass
        elif code == 'r':
            msg['type'] = 'redirect'
            red = msg['data'] = dict()
            red['sid'] = _u64(buf, 0) & 0xffffffffffff
            rlst = red['items'] = list()
            buf = buf[8:]
            while len(buf) >= 8:
                typ = _u8(buf, 0)
                subtyp = typ & 0x0f
                typ &= 0xf0
                dlen = _u8(buf, 1) * 8
                rbuf = buf[0:dlen]
                buf = buf[dlen:]

                rent = dict()
                rlist.append(rent)
                if typ == 0x00:
                    rent['type'] = 'redtime'
                    rent['size'] = _u32(rbuf, 0) & 0xffffff
                    rent['time'] = _u32(rbuf, 1)
                elif typ == 0xf0:
                    rent['type'] = 'redsid'
                    rent['sid'] = _u64(rbuf, 0) & 0xffffffffffff
                elif typ == 0x80 or typ == 0x90:
                    rent['type'] = 'redirect' if typ == 0x80 else 'redlocal'
                    rent['port'] = _u16(rbuf, 2)
                    rent['user_dictid'] = _u32(rbuf, 4)
                    rent['referent'] = _decode_null_term(rbuf[8:])
                    _decompose_server_path(rent, 'referent')
                    rent['op'] = _redir_ops.get(subtyp, 'unk_%02X' % subtyp)
                else:
                    rent['type'] = 'unk_%02X' % (typ | subtyp)
                    rent['resv_1_%d' % dlen] = rbuf[1:]
                    _humanize_buffer(rent, 'resv_1_%d' % dlen)
                    pass
                continue
            pass
        elif code in _mapping_kind:
            msg['type'] = 'mapping'
            mpg = msg['data'] = dict()
            mpg['dictid'] = struct.unpack('>I', buf[0:4])[0]
            mpg['kind'] = _mapping_kind.get(code, None)
            buf = buf[4:]
            lines = buf.decode('ascii').splitlines()
            mpg['info'] = lines[0] ; lines = lines[1:]
            _decompose_userid(mpg, 'info')
            info = mpg['info']
            mpgdat = info['args'] = dict()
            if code == 'p':
                info['xfn'] = lines[0]
                lines = lines[1:]
            elif code == 'x':
                info['lfn'] = lines[0]
                lines = lines[1:]
            elif code == 'i':
                info['app_info'] = [ _software_version(i) \
                                     for i in lines[0].split() ]
                lines = list()
            elif code == 'd':
                info['path'] = lines[0]
                lines = list()
                pass
            if len(lines) > 0:
                for it in _uriarg_fmt.finditer(lines[0]):
                    mpgdat[it.group(1)] = it.group(2)
                    continue
                lines = lines[1:]
                _decompose_array(mpgdat, 'g')
                _decompose_array(mpgdat, 'o')
                _decompose_array(mpgdat, 'r')
                _interleave_arrays(mpgdat, 'auth', _auth_fields)
                _integrate_field(mpgdat, 'I')
                if code == 'x':
                    mpgdat['op'] = _xfr_ops.get(mpgdat['op'],
                                                'unk_' + mpgdat['op'])
                    _integrate_field(mpgdat, 'sz')
                    _integrate_field(mpgdat, 'rc')
                    _reify_field(mpgdat, 'tm')
                    _reify_field(mpgdat, 'tod')
                elif code == 'p':
                    mpgdat[('pfn' if mpgdat.get('f') == 'p'
                            else 'lfn')] = mpgdat['xfn']
                    del mpgdat['xfn']
                    pass
                _expand_keys(mpgdat, _map_keys[code])
                pass
            if len(lines) > 0:
                mpg['lines'] = lines
                pass
            
            buf = b''
            pass
        pass

    if len(buf) > 0:
        result['remn'] = buf
        _humanize_buffer(result, 'remn')
        pass

    return result


if __name__ == '__main__':
    ## Pipe a PCAP file to this module using "tshark -r foo.pcap -t u
    ## -Tfields -e frame.time_epoch -e ip.src -e udp.srcport -e data".
    ## Each line needs to be a timestamp in seconds, the source IP
    ## address, the source port, and a hex string of the payload
    ## bytes.
    import yaml
    for line in sys.stdin:
        words = line.split()
        ts = float(words[0])
        addr = (words[1], int(words[2]))
        buf = bytearray.fromhex(words[3])
        decoded = decode_message(ts, addr, buf)
        yaml.dump(decoded, sys.stdout, explicit_end=True)
        continue
    pass
