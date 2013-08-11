#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph.Exceptions import ConsistencyException

class Edge(object):
    """
    Represents a directed edge in a graph.
    """
    def __init__(self, sourceNodeId, targetNodeId, graph, type="", basicAttrs=None):
        """
        Create a new edge instance.
        Usually this constructor shall not be used directly
        
        @param sourceNodeId The node instance that acts as the source node.
        @param targetNode The node instance that acts as the target node
        @param The edge type
        @param basicAttrs The basic attributes, or None to use empty set.
        """
        self.sourceNodeId = sourceNodeId
        self.targetNodeId = targetNodeId
        self.graphAttr = graph
        #Serialize the edge database keys
        self.activeKey = "%s\x1F%s\x0E%s" % (type, self.sourceNodeId, self.targetNodeId)
        self.passiveKey = "%s\x1F%s\x0F%s" % (type, self.targetNodeId, self.sourceNodeId)
        #Initialize basic and extended attributes
        self.basicAttrs = BasicAttributes(self, basicAttrs)
        self.extendedAttrs = ExtendedAttributes(self)
    @property
    def basicAttributes(self):
        """
        The basic attributes for the current node
        """
        return self.basicAttrs
    @property
    def extendedAttributes(self):
        """
        Get the extended attributes. Note that extended attributes
        are generally lazy-loaded and not persistently stored in API classes
        """
        return self.extendedAttrs
    @property
    def graph(self):
        """
        The graph this node relates to
        """
        return self.graphAttr
    @property
    def id(self):
        """
        This method is used by the extended attributes
        to get the database key. The database key for extended attributes
        is the active edge, so we return that here.
        """
        return self.activeKey
    @property
    def source(self):
        """
        The ID of the source node
        """
        return self.sourceNodeId
    @property
    def target(self):
        """
        The ID of the target node
        """
        return self.targetNodeId
    def _save():
        """
        Writes the edge (both passive and active versions)
        to the database
        """
        self.graph._writeEdge(self.activeKey,
                              self.passiveKey,
                              self.basicAttrs.serialize())
