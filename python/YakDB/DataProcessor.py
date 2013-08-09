#!/usr/bin/env python
# -*- coding: utf8 -*-

class ClientSidePassiveJob:
    """
    A server job that waits for data chunk request from
    arbitrary clients. The server does neither spawn
    any workers nor actively send data somewhere.
    """
    def __init__(self,  connection,  apid):
        """
        Create a new clientside passive job to request data
        from a given DB connection and an APID
        """
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
    def getAPID(self):
        """
        Getter for the job-specific APID that is guaranteed to be unique
        """
        return self.apid