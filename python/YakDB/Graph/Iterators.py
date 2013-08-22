#!/usr/bin/env python
# -*- coding: utf8 -*-

from collections import deque
from YakDB.Graph.Identifier import Identifier
from YakDB.Graph.Node import Node

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
        if len(scanRes) is 0:
            raise StopIteration
        for node in scanRes:
            self.buf.append(node)
        #Get the key to use as start key next time
        lastIdentifier = (scanRes[-1]).id
        print "Last identifier: %s" % lastIdentifier
        self.nextStartKey= Identifier.incrementKey(lastIdentifier)
    def next(self):
        """
        Get the next node
        """
        if len(self.buf) == 0:
            self.__loadNextNodes()
        return self.buf.popleft()