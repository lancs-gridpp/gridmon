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
import functools
import time
import xml
from defusedxml import ElementTree
from lancs_gridmon.xrootd.detail.parsing import decode_message \
    as decode_detailed_message

class XRootDFilter:
    def __init__(self, proc_sum, proc_det):
        """Invokes proc_sum(timestamp, address, xml_doc_tree) or
        proc_det(timestamp, address, dict_tree).

        """
        self._proc_sum = proc_sum
        self._proc_det = proc_det
        pass

    class Handler(DatagramRequestHandler):
        def __init__(self, receiver, *args, **kwargs):
            self.xrootd_receiver = receiver
            super().__init__(*args, **kwargs)
            pass

        def handle(self):
            self.xrootd_receiver.__process(self.client_address, self.request[0])
            pass

        pass

    def datagram_handler(self):
        return functools.partial(self.Handler, self)

    def __process(self, addr, dgram):
        ## Attempt to parse the data as XML.  If it fails to parse,
        ## let it be interpreted as a detailed message.  Pass the
        ## parsed data on to the right function, along with a
        ## timestamp and the source address.
        now = time.time()
        try:
            tree = ElementTree.fromstring(dgram)
            self._proc_sum(now, addr, tree)
        except xml.etree.ElementTree.ParseError:
            self._proc_det(now, addr, decode_detailed_message(dgram))
            pass
        pass

    pass

