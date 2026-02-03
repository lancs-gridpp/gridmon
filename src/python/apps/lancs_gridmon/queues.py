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

import time
import re
import os
import filelock
import threading
import logging
from pathlib import Path

_fnfmt = re.compile('^queue-([0-9a-fA-F]+).chk$')

class _Chunk:
    def __init__(self, stamp, path, new=False, name='unk'):
        self._name = name
        self._stamp = stamp
        self._path = path
        if new:
            self._handle = open(self._path, 'wb+')
            self._size = 0
            self._count = 0
            self._handle.write(self._size.to_bytes(4, byteorder='big'))
            self._handle.write(self._count.to_bytes(2, byteorder='big'))
            logging.debug('%s:%s new open wb+' % (self._name, self._path))
        else:
            self._handle = None
            with open(self._path, 'rb') as fh:
                buf = fh.read(6)
                self._size = int.from_bytes(buf[0:4], byteorder='big')
                self._count = int.from_bytes(buf[4:6], byteorder='big')
                pass
            logging.debug('%s:%s counted %d:%d' % \
                          (self._name, self._path, self._count, self._size))
        pass

    def __iter__(self):
        self.complete()
        with open(self._path, 'rb') as fh:
            buf = fh.read(6)
            while self._count > 0:
                buf = fh.read(6)
                hsz = int.from_bytes(buf[0:2], byteorder='big')
                bsz = int.from_bytes(buf[2:6], byteorder='big')
                header = fh.read(hsz)
                if len(header) < hsz:
                    logging.warning(('discarding incomplete header' + \
                                     ' exp=%d got=%d f=%s') % \
                                    (hsz, len(header), self._path))
                    break
                body = fh.read(bsz)
                if len(body) < bsz:
                    logging.warning(('discarding incomplete body' + \
                                     ' exp=%d got=%d f=%s') % \
                                    (bsz, len(body), self._path))
                    break
                self._count -= 1
                self._size -= len(body)
                logging.debug('%s:%s loaded elem %d:%d' % \
                              (self._name, self._path, len(header), len(body)))
                yield (header, body)
                continue
            if self._size > 0 or self._count > 0:
                logging.warning('excess %d/%d on %s' % \
                                (self._size, self._count, self._path))
                pass
            pass
        pass

    def name(self):
        return '%s:%s' % (self._name, self._path)

    def next_time(self, stamp):
        return self._stamp + 1 if stamp <= self._stamp else stamp

    def prev_time(self):
        return self._stamp - 1

    def unlink(self):
        self.complete()
        self._path.unlink()
        logging.debug('%s:%s unlinked' % (self._name, self._path))
        pass

    def too_much(self, sz, lim):
        return self._size + sz > lim

    def append(self, header, body):
        assert len(header) <= 0xffff
        assert len(body) <= 0xffffffff
        if self._handle is None:
            ## rb+ allows us to seek back to the start to overwrite
            ## the chunk header inside complete().  ab+ fails to allow
            ## this, even though the result is seekable() and the
            ## position has been set to 0.
            self._handle = open(self._path, 'rb+')
            logging.debug('%s:%s opened rb+' % (self._name, self._path))
            self._handle.seek(0, os.SEEK_END)
            pass
        self._handle.write(len(header).to_bytes(2, byteorder='big'))
        self._handle.write(len(body).to_bytes(4, byteorder='big'))
        self._handle.write(header)
        self._handle.write(body)
        self._size += len(body)
        self._count += 1
        logging.debug('%s:%s append %d:%d' % \
                      (self._name, self._path, len(header), len(body)))
        pass

    def complete(self):
        if self._handle is None:
            return
        assert self._handle.seekable()
        self._handle.seek(0, os.SEEK_SET)
        assert self._handle.tell() == 0
        self._handle.write(self._size.to_bytes(4, byteorder='big'))
        self._handle.write(self._count.to_bytes(2, byteorder='big'))
        self._handle.close()
        self._handle = None
        logging.debug('%s:%s completed %d:%d' % \
                      (self._name, self._path, self._count, self._size))
        return

    pass

class Shutdown(Exception):
    def __init__(self):
        super().__init__()
        pass

    def __str__(self):
        return 'shutdown'

    pass

class PersistentQueue:
    def __init__(self, path, encoder=lambda x: x, decoder=lambda x: x,
                 chunk_size=1024*1024, ram_size=1024*1024, name='queue'):
        self._name = name
        self._encoder = encoder
        self._decoder = decoder
        self._dir = Path(path)
        self._dir.mkdir(parents=False, exist_ok=True, mode=0o700)
        self._file_lock = filelock.FileLock(self._dir / "queue.lock")
        self._file_lock.acquire(timeout=0)
        self._chunk_size = chunk_size
        self._ram_size = ram_size
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

        self._complete = False

        self._mem_elems = list()
        self._mem_size = 0
        self._disk_size = 0
        self._disk_count = 0

        ## Load in chunks, and sort them by their timestamp.
        chunks = dict()
        for fp in self._dir.iterdir():
            if not fp.is_file():
                continue
            mt = _fnfmt.match(fp.name)
            if mt is None:
                continue
            ts = int(mt.group(1), 16)
            chunks[ts] = _Chunk(ts, fp, name=self._name)
            continue
        self._chunks = [ chunks[k] for k in sorted(chunks) ]

        self.__repop()
        pass

    def __chunk_path(self, stamp):
        return self._dir / ('queue-%016x.chk' % stamp)

    def __repop(self):
        while len(self._mem_elems) == 0:
            ## Populate the in-memory queue from the first chunk.
            if len(self._chunks) == 0:
                return
            for header, body in self._chunks[0]:
                try:
                    header = self._decoder(header)
                except:
                    logging.error('%s broken' % self._chunks[0].name())
                    break
                self._mem_elems.append((header, body))
                self._mem_size += len(body)
                self._disk_size -= len(body)
                self._disk_count -= 1
                continue
            self._chunks[0].unlink()

            ## If there's any truncation, the chunk's counters will be
            ## non-zero.
            self._disk_size -= self._chunks[0]._size
            self._disk_count -= self._chunks[0]._count

            del self._chunks[0]
            continue
        pass

    def shutdown(self):
        with self._cond:
            if self._complete:
                return
            self._complete = True
            if len(self._chunks) > 0:
                self._chunks[-1].complete()
            elif len(self._mem_elems) == 0:
                self._cond.notify_all()
                pass
            pass
        pass

    def stats(self):
        with self._cond:
            return {
                'mem_count': len(self._mem_elems),
                'mem_size': self._mem_size,
                'disk_count': self._disk_count,
                'disc_size': self._disk_size,
            }

    def push(self, header, body):
        if len(body) > 0xffffffff:
            raise ValueError('body %d too big' % len(body))
            
        with self._cond:
            if self._complete:
                raise Shutdown()

            bsz = len(body)
            nmsz = self._mem_size + bsz
            if len(self._chunks) > 0 or nmsz > self._ram_size:
                ## We have to write to a file.  Ensure that there is
                ## at least one chunk.
                stamp = None
                if len(self._chunks) == 0:
                    ## We have no chunks, so we definitely need a new
                    ## one.  Use the current time as a timestamp.
                    stamp = int(time.time() * 1000)
                elif self._chunks[-1].too_much(bsz, self._chunk_size):
                    ## The last chunk is full, so use the current time
                    ## as the timestamp, or one more than the last
                    ## chunk's stamp.  Also, the last chunk should be
                    ## completed before moving on.
                    self._chunks[-1].complete()
                    stamp = self._chunks[-1].next_time(int(time.time() * 1000))
                    pass
                if stamp is not None:
                    ## Make a new chunk.
                    stamp = int(time.time() * 1000)
                    path = self.__chunk_path(stamp)
                    self._chunks.append(_Chunk(stamp, path, name=self._name,
                                               new=True))
                    pass

                ## Add to the last chunk.
                ehdr = self._encoder(header)
                if len(ehdr) > 0xffff:
                    raise ValueError('header %d too big' % len(ehdr))
                self._chunks[-1].append(ehdr, body)
                self._disk_count += 1
                self._disk_size += len(body)
            else:
                ## Add to the in-memory queue.
                self._mem_elems.append((header, body))
                self._mem_size = nmsz
                self._cond.notify()
                logging.debug('%s mem %d:%d' % \
                              (self._name, len(header), len(body)))
                pass
            pass
        pass

    def pop(self):
        with self._cond:
            while not self._complete and len(self._mem_elems) == 0:
                self._cond.wait()
                continue
            if self._complete:
                raise Shutdown()
            header, body = self._mem_elems[0]
            del self._mem_elems[0]
            self._mem_size -= len(body)
            if len(self._mem_elems) == 0:
                self.__repop()
                pass
            if len(self._mem_elems) > 0:
                self._cond.notify()
                pass
            return (header, body)
        pass

    def close(self):
        self.shutdown()
        with self._cond:
            if len(self._mem_elems) == 0:
                return

            ## For a new leading chunk, choose either the current
            ## time, or a moment before the current leading chunk.
            stamp = self._chunks[0].prev_time() \
                if len(self._chunks) > 0 \
                else int(time.time() * 1000)

            ## Create the chunk, and save the in-memory elements to
            ## it.
            path = self.__chunk_path(stamp)
            ch0 = _Chunk(stamp, path, name=self._name, new=True)
            for header, body in self._mem_elems:
                ch0.append(self._encoder(header), body)
                continue

            ## Clear and release resources.
            self._mem_elems = list()
            self._mem_size = 0
            self._mem_count = 0
            self._disk_count = 0
            self._disk_size = 0
            self._chunks = list()
            ch0.complete()
            self._file_lock.release()
            pass
        pass

    pass

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%SZ')
    import sys
    from getopt import gnu_getopt
    q = PersistentQueue('/tmp/test.queue', chunk_size=32)
    try:
        opts, args = gnu_getopt(sys.argv[1:], 'a:r')
        for opt, val in opts:
            if opt == '-a':
                q.push(b'', val.encode('utf-8'))
            elif opt == '-r':
                _, v = q.pop()
                v = v.decode('utf-8')
                print(v)
                pass
            continue
        q.shutdown()
    except KeyboardInterrupt:
        print('stopping')
    finally:
        q.close()
        pass
            
    pass
