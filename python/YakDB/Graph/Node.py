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
        if basicAttrString == None:
            self.basicAttributes = BasicAttributes(self, basicAttrString)
        else:
            self.basicAttributes = None
    @property
    def id(self):
        """
        Get the ID that is used as key in the database
        """
        return self.id
    @property
    def basicAttributes(self):
        """
        The basic attributes for the current node
        """
        if self.basicAttrs == None:
            pass #TODO: load basic attributes
        return self.basicAttributes
