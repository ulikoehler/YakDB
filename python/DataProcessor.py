#!/usr/bin/env python

class ClientSidePassiveJob:
    """
    A server job that waits for data chunk request from
    arbitrary clients. The server does neither spawn
    any workers nor actively send data somewhere.
    """
    def __init__(self,  connection,  apid):
        self.connection = connection
        self.apid = apid
    def requestDataChunk(self):
        """
        Request a single data chunk from the server.
        The size of the data chunk is set on job initialization.
        If the data block returned is empty, the caller shall
        not request any more data blocks (they will always be empty).
        """
        return self.connection._requestJobDataChunk(self.apid)
