#!/usr/bin/env python
# -*- coding: utf8 -*-

from BasicAttributes import BasicAttributes
from ExtendedAttributes import ExtendedAttributes
from Identifier import Identifier
from YakDB.Graph import Graph
from YakDB.Exceptions import ParameterException

class Node(object):
    """
    An instance of this class represents a
    single node within a graph.
    """
    def __init__(self, nodeId,  graph,  basicAttrs=None):
        """
        Create a new node instance.
        
        @param nodeId The ID of the node (must be an identifier)
        @param graph The graph 
        @param basicAttrs The set of basic attributes or None to use empty set
        """
        Identifier.checkIdentifier(nodeId)
        if not isinstance(graph, Graph.Graph):
            raise ParameterException("The graph argument is no Graph object")
        self.nodeId = nodeId
        self.graphAttr = graph
        #Initialize the basic and extended attributes
        self.basicAttr = BasicAttributes(self, basicAttrs)
        self.extendedAttr = ExtendedAttributes(self)
    def _save(self):
        """
        Save the current entity in the database
        """
        self.graph._saveNode(self)
    @property
    def id(self):
        """
        Get the ID that is used as key in the database.
        The ID of a node must not be changed.
        """
        return self.nodeId
    @property
    def basicAttributes(self):
        """
        The basic attributes for the current node
        """
        return self.basicAttr
    @property
    def extendedAttributes(self):
        """
        Get the extended attributes. Note that extended attributes
        are generally lazy-loaded and not persistently stored in API classes
        """
        return self.extendedAttr
    @property
    def graph(self):
        """
        The graph this node relates to
        """
        return self.graphAttr
