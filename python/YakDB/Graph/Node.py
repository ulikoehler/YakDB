#!/usr/bin/env python
# -*- coding: utf8 -*-

from BasicAttributes import BasicAttributes
from ExtendedAttributes import ExtendedAttributes

class Node(object):
    """
    An instance of this class represents a
    single node within a graph.
    """
    def __init__(self, nodeId,  graph,  serializedBasicAttrs=None):
        self.id = nodeId
        self.graph = graph
        #Initialize the basic attributes
        if serializedBasicAttrs == None: #Node was created by reading DB entry
            self.basicAttributes = BasicAttributes(self, serializedBasicAttrs)
        else: #Node was created de-novo without a related DB entry
            self.basicAttributes = BasicAttributes(self)
        #Initialize the extended attributes (lazy-loaded
        self.extendedAttributes = ExtendedAttributes(self)
    def _save(self):
        """
        Save the current entity in the database
        """
        self.graph._saveNode(self,  self.basicAttributes)
    @property
    def id(self):
        """
        Get the ID that is used as key in the database.
        The ID of a node must not be changed.
        """
        return self.id
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
    def _getExtendedAttributes(startKey,  limit):
        """
        Internal function, should only be called by ExtendedAttributes instances
        """
        pass
    def _getExtendedAttribute(startKey,  limit):
        """
        Internal function, should only be called by ExtendedAttributes instances
        """
        pass
    def _deleteExtendedAttributes(startKey,  limit):
        """
        Internal function, should only be called by ExtendedAttributes instances
        """
        pass
    def _saveExtendedAttributes(startKey,  limit):
        """
        Internal function, should only be called by ExtendedAttributes instances
        """
        pass
