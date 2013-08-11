#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph.Exceptions import ConsistencyException
from YakDB.Exceptions import ParameterException
from BasicAttributes import BasicAttributes
from ExtendedAttributes import ExtendedAttributes

class Edge(object):
    """
    Represents a directed edge in a graph.
    """
    def __init__(self, source, target, graph, edgeType="", basicAttrs=None):
        """
        Create a new edge instance.
        Usually this constructor shall not be used directly
        
        @param source The node instance that acts as the source node.
        @param target The node instance that acts as the target node
        @param The edge type
        @param basicAttrs The basic attributes, or None to use empty set.
        """
        self.sourceNodeId = source
        self.targetNodeId = target
        self.graphAttr = graph
        self.edgeType = edgeType
        #Serialize the edge database keys
        self.activeKey = "%s\x1F%s\x0E%s" % (edgeType, self.sourceNodeId, self.targetNodeId)
        self.passiveKey = "%s\x1F%s\x0F%s" % (edgeType, self.targetNodeId, self.sourceNodeId)
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
        return self.source
    @property
    def target(self):
        """
        The ID of the target node
        """
        return self.target
    @property
    def type(self):
        """
        The edge type
        """
        return self.sedgeType
    def _save(self):
        """
        Writes the edge (both passive and active versions)
        to the database
        """
        self.graph._writeEdge(self.activeKey,
                              self.passiveKey,
                              self.basicAttrs.serialize())
    @staticmethod
    def _getAllEdgesScanKeys(nodeId, type=""):
        """ee
        Get the scan keys to scan for ALL edges for a given node.
        @param type The edge type
        @return (startKey, endKey) The scan start and end keys
        """
        formatTuple = (type, nodeId)
        return ("%s\x1F%s\x0E" % formatTuple, "%s\x1F%s\x10" % formatTuple)
    @staticmethod
    def _getIngoingEdgesScanKeys(nodeId, type=""):
        """
        Get the scan keys to scan for INGOING edges for a given node.
        @param type The edge type
        @return (startKey, endKey) The scan start and end keys
        """
        formatTuple = (type, nodeId)
        return ("%s\x1F%s\x0F" % formatTuple, "%s\x1F%s\x10" % formatTuple)
    @staticmethod
    def _getOutgoingEdgesScanKeys(nodeId, type=""):
        """
        Get the scan keys to scan for OUTGOING edges for a given node.
        @param type The edge type
        @return (startKey, endKey) The scan start and end keys
        """
        formatTuple = (type, nodeId)
        return ("%s\x1F%s\x0E" % formatTuple, "%s\x1F%s\x0F" % formatTuple)
    @staticmethod
    def _deserializeEdge(key):
        """
        Deserializes an edge database key
        @return A tuple (sourceId, targetId, type) deserialized from the database key
        
        >>> Edge._deserializeEdge("\\x1Fa\\x0Eb")
        ('a', 'b', '')
        >>> Edge._deserializeEdge("\\x1Fa\\x0Fb")
        ('b', 'a', '')
        >>> Edge._deserializeEdge("mytype\\x1Fa\\x0Eb")
        ('a', 'b', 'mytype')
        >>> Edge._deserializeEdge("mytype\\x1Fa\\x0Fb")
        ('b', 'a', 'mytype')
        """
        #Split the typeplanetexplorers.pathea.net
        typeSeparatorIndex = key.find("\x1F")
        if typeSeparatorIndex == -1:
            raise ParameterException("Could not find     type separator in edge key!")
        edgeType = key[0:typeSeparatorIndex]
        key = key[typeSeparatorIndex+1:]
        #Split the source and target node
        outgoingIndex = key.find("\x0E")
        ingoingIndex = key.find("\x0F")
        if outgoingIndex == -1 and ingoingIndex == -1:
            raise ParameterException("Could not find OUT or IN separator in edge key!")
        isIngoing = (outgoingIndex == -1)
        splitKey = (ingoingIndex if isIngoing else outgoingIndex)
        firstNodeIndex = key[0:splitKey]
        secondNodeIndex = key[splitKey+1:]
        sourceNode = (secondNodeIndex if isIngoing else firstNodeIndex)
        targetNode = (firstNodeIndex if isIngoing else secondNodeIndex)
        #Create and return the tuple
        return (sourceNode, targetNode, edgeType)
    def __str__(self):
        formatTuple = (self.source, self.target, self.type, self.basicAttrs.getAttributes())
        return "Edge(source=%s, target=%s, type=%s, basicAttrs=%s)" % formatTuple


if __name__ == "__main__":
    import doctest
    doctest.testmod()