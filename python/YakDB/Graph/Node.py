#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph.Edge import Edge
from YakDB.Graph.Identifier import Identifier
from YakDB.Graph.Entity import Entity
from YakDB.Exceptions import ParameterException

class Node(Entity):
    """
    An instance of this class represents a
    single node within a graph.
    """
    def __init__(self, nodeId, graph, basicAttrs=None):
        """
        Create a new node instance.
        
        @param nodeId The ID of the node (must be an identifier)
        @param graph The graph 
        @param basicAttrs The set of basic attributes or None to use empty set.
            Any binary string passed here is automatically deserialized
        """
        Identifier.checkIdentifier(nodeId)
        #if not isinstance(graph, Graph):
        #   raise ParameterException("The graph argument is no Graph object")
        self.id = nodeId
        self.graph = graph
        #Initialize basic and extended attributes
        self.initializeAttributes(basicAttrs)
    def save(self):
        """
        Save the current entity in the database
        """
        self.graph.saveNode(self)
    def getEdges(self, limit=None):
        """
        Get a list all edges for the current node
        @return A list of Edge instances.
        """
        (startKey, endKey) = Edge._getAllEdgesScanKeys(self.id)
        return self.graph._scanEdges(startKey, endKey, limit)
    def getIncomingEdges(self, limit=None):
        """
        Get a list all edges for the current node
        @return A list of Edge instances.
        """
        (startKey, endKey) = Edge._getIncomingEdgesScanKeys(self.id)
        return self.graph._scanEdges(startKey, endKey, limit)
    def getOutgoingEdges(self, limit=None):
        """
        Get a list all edges for the current node
        @return A list of Edge instances.
        """
        (startKey, endKey) = Edge._getOutgoingEdgesScanKeys(self.id)
        return self.graph._scanEdges(startKey, endKey, limit)
    @property
    def indegree(self):
        """
        @return The indegree (as int) of the node
        """
        (startKey, endKey) = Edge._getIncomingEdgesScanKeys(self.id)
        return self.graph._countEdges(startkey, endKey)
    @property
    def outdegree(self):
        """
        @return The indegree (as int) of the node
        """
        (startKey, endKey) = Edge._getOutgoingEdgesScanKeys(self.id)
        return self.graph._countEdges(startkey, endKey)
    def delete(self):
        """
        Deletes the current node.
        """
        self.graph.deleteNode(self.id)
    def __str__(self):
        return "Node(id='%s')" % self.id
    def __repr__(self):
        return self.__str__()
