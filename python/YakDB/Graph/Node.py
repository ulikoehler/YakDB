#!/usr/bin/env python
# -*- coding: utf8 -*-

from BasicAttributes import BasicAttributes

class Node(object):
    """
    An instance of this class represents a
    single node within a graph.
    """
    def __init__(self, nodeId,  graph,  basicAttrString=None):
        self.id = nodeId
        self.graph = graph
        self.basicAttrs = BasicAttributes(self, basicAttrString)
    def getID(self): return self.id
    def getBasicAttributes(self):
        return self.basicAttrs
