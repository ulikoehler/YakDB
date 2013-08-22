#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph.Node import Node

class NodeIterator:
    """
    An iterator that iterates over nodes in a graph.
    """
    def __init__(self, graph, backingIterator):
        """
        Initialize a NodeIterator from a backing KeyValueIterator
        """
        self.graph = graph
        self.backingIterator = backingIterator
    def __iter__(self):
        return self
    def next(self):
        """
        Get the next node
        """
        (nodeId, basicAttrs) = self.backingIterator.next()
        return Node(nodeId, self.graph, basicAttrs)