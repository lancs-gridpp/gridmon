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

from socketserver import DatagramRequestHandler
import time
import functools
import threading
import pickle
import os
import re
import filelock
import logging
from pathlib import Path

_fnfmt = re.compile('^queue-([0-9a-fA-F]+).chunk$')

class Shutdown(Exception):
    def __init__(self):
        super().__init__()
        pass

    def __str__(self):
        return 'shutdown'

    pass

class FileQueue:
    def __init__(self, path, chunk_size=1024*1024, ram_size=1024*1024):
        self._dir = Path(path)
        self._dir.mkdir(parents=False, exist_ok=True, mode=0o700)
        self._file_lock = filelock.FileLock(self._dir / "queue.lock")
        self._file_lock.acquire(timeout=0)

        self._ram_size = ram_size
        self._chunk_size = chunk_size
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._mem = list()
        self._mem_size = 0
        self._tasks = 0
        self._file = None
        self._file_size = 0

        self.__repop()
        self._up = True
        pass

    def __new_chunk(self):
        ## Open a new chunk, using the time in milliseconds to
        ## identify and sequence it.
        assert self._file is None or self._file.closed
        ts = int(time.time() * 1000)
        fn = self._dir / ('queue-%016x.chunk' % ts)
        self._file = open(fn, "wb")
        self._file_size = 0
        logging.debug('new chunk %s' % fn)
        return

    def put(self, ent):
        with self._cond:
            if not self._up:
                raise Shutdown()

            dat = pickle.dumps(ent)

            ## Make sure we're not writing to a chunk file
            ## unnecessarily.  This does nothing if we already have
            ## something in memory.
            note = self.__repop()

            ## Append to any existing chunk file.  Close the file and
            ## open a new one if the limit is reached.
            if self._file is not None:
                self._file.write(dat)
                self._file_size += len(dat)
                logging.debug('to chunk: %d/%d' % \
                              (self._mem_size, self._file_size))
                if self._file_size >= self._chunk_size:
                    ## The chunk has exceeded its limit, so close it
                    ## and start a new one.
                    self._file.close()
                    self.__new_chunk()
                    pass
                return False

            ## Append in-memory, and notify if the queue was empty.
            try:
                self._mem.append({ 'pkl': dat, 'ent': ent})
                self._mem_size += len(dat)
                logging.debug('to mem: %d/%d' % \
                              (self._mem_size, self._file_size))
                note = note or len(self._mem) == 1

                if self._mem_size >= self._ram_size:
                    self.__new_chunk()
                    pass
            finally:
                if note:
                    self._cond.notify_all()
                    return True
                return False
            pass
        pass

    def __get_chunks(self):
        ## Scan the directory for chunk files, and create a sequence
        ## of tuples (timestamp, pathname) ordered by timestamp.
        seq = dict()
        best = None
        for fp in self._dir.iterdir():
            if not fp.is_file():
                continue
            mt = _fnfmt.match(fp.name)
            if mt is None:
                continue
            ts = int(mt.group(1), 16)
            seq[ts] = fp
            continue
        return [ (k, v) for k, v in sorted(seq.items()) ]

    def __repop(self):
        ## Do nothing if we already have a chunk in memory.
        if len(self._mem) > 0:
            return False
        assert self._mem_size == 0

        ## Load in the earliest chunk, then delete it.  If it's empty,
        ## delete it anyway, and try the next one.
        seq = self.__get_chunks()
        if len(seq) == 1 and self._file is not None:
            ## There's only one chunk, and we seem to be open on it.
            ## Close it before loading in.
            self._file.close()
            self._file = None
            pass
        notify = False
        while not notify and len(seq) > 0:
            expect = seq[0][1].stat().st_size
            logging.debug('loading %d from %s' % (expect, seq[0][1]))
            with open(seq[0][1], 'rb') as fh:
                pos = 0
                while expect > 0:
                    dat = pickle.load(fh)
                    np = fh.tell()
                    self._mem.append({ 'ent': dat })
                    z = np - pos
                    self._mem_size += z
                    expect -= z
                    pos = np
                    notify = True
                    continue
                pass
            seq[0][1].unlink()
            del seq[0]
            continue

        ## We should be writing to a file next.
        if len(seq) > 0 and self._file is None:
            self._file_size = seq[-1][1].stat().st_size
            if self._file_size >= self._chunk_size:
                ## Append to the last file.
                self._file = open(seq[-1][1], "ab")
            else:
                ## Start a new file.
                self.__new_chunk()
                pass
            pass

        logging.debug('repop: %d/%d' % \
                      (self._mem_size, self._file_size))
        return notify

    def shutdown(self):
        with self._cond:
            if not self._up:
                return
            try:
                self._up = False
                if len(self._mem) == 0:
                    return
                if self._file is not None:
                    self._file.close()
                    self._file = None
                    pass

                ## Determine the name of the chunk to write by
                ## choosing a name that appears before other chunks.
                seq = self.__get_chunks()
                ts = int(time.time() * 1000) \
                    if len(seq) == 0 else (seq[0][0] - 1)
                fn = self._dir / ('queue-%016x.chunk' % ts)

                ## Write each entry to the chunk file, using its
                ## pre-pickled form if present.
                with open(fn, "wb") as fh:
                    for memb in self._mem:
                        if 'dat' in memb:
                            fh.write(memb['dat'])
                        else:
                            pickle.dump(memb['ent'], fh)
                            pass
                        continue
                    pass

                ## Clear the RAM list.
                self._mem = list()
                self._mem_size = 0
            finally:
                self._cond.notify_all()
                self._file_lock.release()
                pass
            pass
        pass

    def get(self):
        with self._cond:
            while len(self._mem) == 0 and self._up:
                self._cond.wait()
                continue
            if not self._up:
                raise Shutdown()
            r = self._mem.pop(0)
            if 'pkl' not in r:
                r['pkl'] = pickle.dumps(r['ent'])
                pass
            self._mem_size -= len(r['pkl'])
            self.__repop()
            return r['ent']
        pass

    pass

class UDPQueuer:
    def __init__(self, dirpath, dest=None,
                 ram_size=1024*1024, chunk_size=1024*1024):
        self._dest = dest
        if self._dest is not None:
            self._q = FileQueue(dirpath,
                                ram_size=ram_size,
                                chunk_size=chunk_size)
            self._hdlr = functools.partial(self.Handler, self)
            self._thrd = threading.Thread(target=self._serve_forever)
            pass
        pass

    def handler(self):
        return self._hdlr

    def start(self):
        if self._dest is None:
            return
        self._thrd.start()
        pass

    def halt(self):
        if self._dest is None:
            return
        self._q.shutdown()
        pass

    def _serve_forever(self):
        try:
            while True:
                rec = self._q.get()
                self._dest(rec['ts'], rec['peer'], rec['payload'])
                continue
        except Shutdown:
            pass
        pass

    def _push(self, ts, peer, payload):
        rec = {
            'peer': peer,
            'payload': payload,
            'ts': ts,
        }
        return self._q.put(rec)

    class Handler(DatagramRequestHandler):
        def __init__(self, rcvr, *args, **kwargs):
            self._rcvr = rcvr
            super().__init__(*args, **kwargs)
            pass

        def handle(self):
            rec = {
                'peer': self.client_address,
                'payload': self.request[0],
                'ts': time.time(),
            }
            self._rcvr._push(time.time(), self.client_address, self.request[0])
            pass

        pass

    pass
