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

import threading
import logging
from lancs_gridmon.xrootd.detail.peers import Peer, Stats as PeerStats
from lancs_gridmon.xrootd.detail.recordings import Recorder as XRootDRecorder

class PeerManager:
    def __init__(self, now, evrec, adv, domains=None, id_to=120*60,
                 seq_to=2, epoch=0, fake_port=None, seq_win=128,
                 vo_issuers=dict()):
        self._evrec = evrec
        self._adv = adv
        self._domains = domains
        self._epoch = epoch
        self._fake_port = fake_port
        self._seq_win = seq_win
        self._vo_issuers = vo_issuers

        ## We map from client host/port to Peer.
        self._peers = { }

        ## When we get an identity, we map it to the client host/port
        ## here.  If the old value is different, we purge the old
        ## value from self._peers.
        self._names = { }

        ## Set the timeout for ids.  Remember when we last purged
        ## them.
        self._id_to = id_to
        self._id_ts = now

        ## Set the timeout for missing sequence numbers.
        self._seq_to = seq_to

        self._out_on = True

        self._stats = {
            "mapping": PeerStats(),
            "file": PeerStats(),
            "gstream": PeerStats(),
        }
        self._fake_port_count = 0

        self._lock = threading.Lock()
        pass

    def aggregate(self, dest):
        with self._lock:
            for k, v in self._peers.items():
                v.aggregate(self._stats)
                self._fake_port_count += v.get_fake_port_count()
                continue
            pass
        dest['sequencing'] = { k: v.as_dict() for k, v in self._stats.items() }
        dest['fake_port_overrides'] = self._fake_port_count
        pass

    def _identify(self, pgm, host, inst, peer):
        ## Check to see if anything has changed.
        key = (host, inst, pgm)
        old_addr = self._names.get(key)
        if old_addr is not None:
            old = self._peers.get(old_addr)
            if old is not None and old == peer:
                return

        ## Replace the old entry.
        self._names[key] = peer
        peer.set_identity(host, inst, pgm)
        old = self._peers.pop(old_addr, None)
        if old is not None:
            old.discard()
            pass
        pass

    def check_identity(self):
        ## Go through all peers.  If any are unidentified, halt all
        ## output.
        for addr, peer in self._peers.items():
            if not peer.is_identified():
                self._out_on = False
                return
            continue
        self._out_on = True
        for addr, peer in self._peers.items():
            peer.continue_output()
            continue
        return

    def process(self, dgram):
        with self._lock:
            return self.__process(dgram)
        pass

    ## Return true if the supplied datagram was not fully accepted and
    ## not logged.
    def __process(self, dgram):
        try:
            now = dgram['ts']
            addr = (dgram['peer']['host'], dgram['peer']['port'])
            if 'error' in dgram:
                logging.error('from %s:%d at %.3f %s remnant[%d] %s' % \
                              (addr[0], addr[1], now, dgram['error'],
                               dgram['remn_len'], dgram['remn_octets']))
                return True

            msg = dgram['message']
            stod = msg['stod']
            pseq = msg['pseq']
            data = msg['data']
            typ = msg['type']

            if 'remn_octets' in msg:
                logging.warning('from %s:%d at %.3f remnant %s' % \
                                (addr[0], addr[1], now, msg['remn_octets']))
                pass

            ## Locate the peer record.  Replace with a new one if the
            ## start time has increased.
            peer = self._peers.get(addr)
            if peer is None or stod > peer.stod:
                self._peers[addr] = peer = \
                    Peer(stod, addr, self._identify, self._evrec,
                         id_timeout=self._id_to,
                         seq_timeout=self._seq_to,
                         seq_window=self._seq_win,
                         domains=self._domains,
                         epoch=self._epoch,
                         vo_issuers=self._vo_issuers,
                         fake_port=self._fake_port)
                self.check_identity()
            elif stod < peer.stod:
                ## Ignore messages from old instances.
                return True

            ## Submit the message to be incorporated into the peer
            ## record.
            return peer.process(now, pseq, typ, data)
        except Exception as e:
            logging.error('error processing %s' % dgram)
            raise e
        finally:
            if now - self._id_ts > self._id_to:
                for addr, peer in self._peers.items():
                    peer.id_clear(now)
                    continue
                self._id_ts = now
                pass
            self._adv(now)
            pass
        pass

    pass
