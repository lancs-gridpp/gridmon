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

import subprocess
import os

class PCAPSource:
    def __init__(self, src, limit=None):
        self._src = src
        self._lim = limit
        pass

    def __open(self):
        cmd = [ 'tshark', '-r', self._src, '-t', 'u', '-Tfields',
                '-e', 'frame.time_epoch', '-e', 'ip.src',
                '-e', 'udp.srcport', '-e', 'data' ]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                universal_newlines=True).stdout

    def get_start(self):
        with self.__open() as fin:
            for line in fin:
                words = line.split()
                return float(words[0])
            pass
        pass

    def set_action(self, proc):
        self._proc = proc
        pass

    def serve_forever(self):
        with self.__open() as fin:
            c = 0
            for line in fin:
                words = line.split()
                ts = float(words[0])
                addr = (words[1], int(words[2]))
                buf = bytearray.fromhex(words[3])
                self._proc(ts, addr, buf)
                if self._lim is not None:
                    c += 1
                    if c >= self._lim:
                        break
                    pass
                continue
            pass
        pass

    pass

if __name__ == '__main__':
    import sys
    src = PCAPSource(sys.argv[1])
    print('start: %d' % src.get_start())
    def action(ts, addr, buf):
        print('%d %s %s' % (ts, addr, buf))
        pass
    src.set_action(action)
    src.serve_forever()
    pass
