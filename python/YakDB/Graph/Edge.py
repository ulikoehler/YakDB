#!/usr/bin/env python
# -*- coding: utf8 -*-

import Node
from YakDB.Graph.Exceptions import ConsistencyException
from YakDB.Exceptions import ParameterException
from BasicAttributes import BasicAttributes
from ExtendedAttributes import ExtendedAttributes
from Identifier import Identifier
from Entity import Entity

class Edge(Entity):
    """
    Represents a directed edge in a graph.
    """
    def __init__(self, source, target, graph, edgeType="", basicAttrs=None):
        """
        Create a new edge instance.
        Usually this constructor shall not be used directly
        
        @param source A node instance
        @param target The node instance that acts as the target node
        @param graph The graph this edge belongs to
        @param The edge type
        @param basicAttrs The basic attributes, or None to use empty set.
        """
        #Ensure we have valid ID strings
        if isinstance(source, str):
            Identifier.checkIdentifier(source)
        elif isinstance(source, Node):
            target = source.id
        if isinstance(target, str):
            Identifier.checkIdentifier(target)
        elif isinstance(source, Node):
            source = target.id
        self.source = source
        self.target = target
        self.graph = graph
        self.type = edgeType
        #Serialize the edge database keys
        self.activeKey = "%s\x1F%s\x0E%s" % (edgeType, self.source, self.target)
        self.passiveKey = "%s\x1F%s\x0F%s" % (edgeType, self.target, self.source)
        #Initialize basic and extended attributes
        self.initializeAttributes(basicAttrs)
    @property
    def id(self):
        """
        This method is used by the extended attributes
        to get the database key. The database key for extended attributes
        is the active edge, so we return that here.
        """
        return self.activeKey
    def save(self):
        """
        Writes the edge (both passive and active versions)
        to the database
        """
        self.graph.saveEdge(self)
    def delete(self, deleteExtendedAttributes=True):
        """
        Deletes the current edge in the database.
        @param deleteExtendedAttributes Whether to delete the extended attributes
        """
        self._deleteEdgeKeys([self.activeKey, self.passiveKey])
    @staticmethod
    def _getAllEdgesScanKeys(nodeId, edgeType=""):
        """
        Get the scan keys to scan for ALL edges for a given node.
        @param edgeType The edge type
        @return (startKey, endKey) The scan start and end keys
        """
        formatTuple = (edgeType, nodeId)
        return ("%s\x1F%s\x0E" % formatTuple, "%s\x1F%s\x10" % formatTuple)
    @staticmethod
    def _getIncomingEdgesScanKeys(nodeId, edgeType=""):
        """
        Get the scan keys to scan for Incoming edges for a given node.
        @param edgeType The edge type
        @return (startKey, endKey) The scan start and end keys
        """
        formatTuple = (edgeType, nodeId)
        return ("%s\x1F%s\x0F" % formatTuple, "%s\x1F%s\x10" % formatTuple)
    @staticmethod
    def _getOutgoingEdgesScanKeys(nodeId, edgeType=""):
        """
        Get the scan keys to scan for OUtgoing edges for a given node.
        @param edgeType The edge type
        @return (startKey, endKey) The scan start and end keys
        """
        formatTuple = (edgeType, nodeId)
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
        incomingIndex = key.find("\x0F")
        if outgoingIndex == -1 and incomingIndex == -1:
            raise ParameterException("Could not find OUT or IN separator in edge key!")
        isIncoming = (outgoingIndex == -1)
        splitKey = (incomingIndex if isIncoming else outgoingIndex)
        firstNodeIndex = key[0:splitKey]
        secondNodeIndex = key[splitKey+1:]
        sourceNode = (secondNodeIndex if isIncoming else firstNodeIndex)
        targetNode = (firstNodeIndex if isIncoming else secondNodeIndex)
        #Create and return the tuple
        return (sourceNode, targetNode, edgeType)
    def __str__(self):
        """
        Creates a printable repr-ish string from the edge
        that allows the user to identify source, target and type.
        The Graph is not included.
        """
        formatTuple = (self.source, self.target, self.type, self.basicAttributes.getAttributes())
        return "Edge(source='%s', target='%s', type='%s', basicAttrs=%s)" % formatTuple
    def __repr__(self): return self.__str__()

if __name__ == "__main__":
    import doctest
    doctest.testmod()
