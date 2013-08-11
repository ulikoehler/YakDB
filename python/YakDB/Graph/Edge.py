#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph.Exceptions import ConsistencyException

class Edge(object):
    """
    Represents a directed edge in a graph.
    """
    def __init__(self, sourceNode, targetNode, type="", basicAttrs=None):
        """
        Create a new edge instance.
        @param sourceNode The node instance that acts as the source node.
        @param targetNode The node instance that acts as the target node
        @param The edge type
        @param basicAttrs The basic attributes, or None to use empty set.
        """
        if sourceNode.graph() != targetNode.graph():
            raise ConsistencyException("Source and target node are not from the same graph")
        self.sourceNode = sourceNode
        self.targetNode = targetNode
        self.graph = sourceNode.graph()
        #Serialize the edge versions
        sourceNodeId = sourceNode.id()
        targetNodeId = targetNode.id()
        self.activeKey = "%s\x1F%s\x0E%s" % (type, sourceNodeId, targetNodeId)
        self.passiveKey = "%s\x1F%s\x0F%s" % (type, targetNodeId, sourceNodeId)
        #Initialize basic and extended attributes
        self.basicAttributes = BasicAttributes(self, basicAttrs)
        self.extendedAttributes = ExtendedAttributes(self)
    @property
    def basicAttributes(self):
        """
        The basic attributes for the current node
        """
        return self.basicAttributes
    @property
    def extendedAttributes(self):
        """
        Get the extended attributes. Note that extended attributes
        are generally lazy-loaded and not persistently stored in API classes
        """
        return self.extendedAttributes
    @property
    def graph(self):
        """
        The graph this node relates to
        """
        return self.graph
    @property
    def id(self):
        """
        This method is used by the extended attributes
        to get the database key. The database key for extended attributes
        is the active edge, so we return that here.
        """
        return self.activeKey
    def _save():
        """
        Writes the edge (both passive and active versions)
        to the database
        """
        self.graph._writeEdge(self.activeKey,
                              self.passiveKey,
                              self.basicAttrs._serialize())
