#!/usr/bin/env python
# -*- coding: utf8 -*-

from collections import deque

from YakDB.Utils import YakDBUtils

class KeyValueIterator:
    """
    An iterator that iterates over nodes in a graph.
    """
    def __init__(self, conn, tableNo=1, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, chunkSize=1000):
        """
        Initialize a new key-value iterator
        @param startKey the first node to scan
        @param limit The number of nodes to load at once
        """
        self.conn = conn
        self.tableNo = tableNo
        self.limit = limit
        self.nextStartKey = startKey
        self.endKey = endKey
        self.limit = limit
        self.keyFilter = keyFilter
        self.valueFilter = valueFilter
        self.chunkSize = chunkSize
        self.buf = deque()
    def __iter__(self):
        return self
    def __loadNextNodes(self):
        """
        Load the next set of nodes into the buffer
        """
        scanRes = self.conn.scan(self.tableNo, startKey=self.nextStartKey, endKey=self.endKey, limit=self.chunkSize, keyFilter=self.keyFilter, valueFilter=self.valueFilter)
        #Stop if there's nothing left to scan
        if len(scanRes) is 0:
            raise StopIteration
        for node in scanRes:
            self.buf.append(node)
        #Get the key to use as start key on chunk load
        lastIdentifier = (scanRes.keys()[-1])
        self.nextStartKey= YakDBUtils.incrementKey(lastIdentifier)
    def next(self):
        """
        Get the next node
        """
        if len(self.buf) == 0:
            self.__loadNextNodes() #raises StopIteration if needed
        return self.buf.popleft()