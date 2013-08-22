#!/usr/bin/env python
# -*- coding: utf8 -*-

from collections import deque
from YakDB.Graph.Identifier import Identifier

class NodeIterator:
    """
    An iterator that iterates over nodes in a graph.
    """
    def __init__(self, graph, startKey=None, endKey=None, limit=1000):
        """
        @param graph The graph to use
        @param startKey the first node to scan
        @param limit The number of nodes to load at once
        """
        self.graph = graph
        self.limit = limit
        self.nextStartKey = startKey
        self.endKey = endKey
        self.limit = limit
        self.buf = deque()
    def __iter__(self):
        return self
    def __loadNextNodes(self):
        """
        Load the next set of nodes into the buffer
        """
        scanRes = self.graph.scanNodes(startKey=self.nextStartKey, endKey=self.endKey, limit=self.limit)
        #Stop if there's nothing left to scan
        if len(scanRes) is None:
            raise StopIteration
        for key, value in scanRes.iteritems():
            dataTuple = (key,value)
            self.buf.append(dataTuple)
        #Get the key to use as start key next time
        lastIdentifier = sorted(scanRes.iterkeys())[-1]
        self.nextStartKey(Identifier.incrementKey(lastIdentifier))
    def next(self):
        """
        Get the next node
        """
        if len(self.buf) == 0:
            self.__loadNextNodes()
        return self.buf.popleft()