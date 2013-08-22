#!/usr/bin/env python
# -*- coding: utf8 -*-

class KeyValueIterators:
    """
    An iterator that iterates over nodes in a graph.
    """
    def __init__(self, conn, startKey=None, endKey=None, chunkSize=1000):
        """
        Initialize a new key-value iterator
        @param startKey the first node to scan
        @param limit The number of nodes to load at once
        """
        self.conn = conn
        self.limit = limit
        self.nextStartKey = startKey
        self.endKey = endKey
        self.chunkSize = chunkSize
        self.buf = deque()
    def __iter__(self):
        return self
    def __loadNextNodes(self):
        """
        Load the next set of nodes into the buffer
        """
        scanRes = self.graph.scanNodes(startKey=self.nextStartKey, endKey=self.endKey, limit=self.chunkSize)
        #Stop if there's nothing left to scan
        if len(scanRes) is 0:
            raise StopIteration
        for node in scanRes:
            self.buf.append(node)
        #Get the key to use as start key next time
        lastIdentifier = (scanRes[-1])
        print "Last identifier: %s" % lastIdentifier
        self.nextStartKey= Identifier.incrementKey(lastIdentifier)
    def next(self):
        """
        Get the next node
        """
        if len(self.buf) == 0:
            self.__loadNextNodes()
        return self.buf.popleft()