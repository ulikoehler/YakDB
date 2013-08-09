#!/usr/bin/env python
# -*- coding: utf8 -*-

from ExtendedAttributes import ExtendedAttributes

class Graph:
    """
    A directed graph represented 
    as adjacency list in multiple tables in a YakDB database.
    
    """
    def __init__(self,  conn, nodeTableId=2,  edgeTableId=3,  nodeExtAttrTableId=4,  edgeExtAttrId=5):
        """
        Create a new graph from a YakDB connection.
        """
        self.conn = conn
        self.nodeTableId = nodeTableId
        self.edgeTableId = edgeTableId,
        self.nodeExtAttrTableId = nodeExtAttrTableId
        self.edgeExtAttrId = edgeExtAttrId
    def createNode(self,  nodeId,  basicAttributes):
        """
        Add a node to the graph.
        If a node with the same ID already exists in the graph,
        the already-existing node will be ignored
        (its existing basic attributes will be overwritten),
        but its extended attributes won't be modified
        """
        pass
    def deleteNode(self, nodeId,  deleteExtAttrs=True):
        """
        Delete a node.
        If the node does not exist, the request will be ignored.
        @param deleteExtAttrs If this is set to true, all node extended attributes will be deleted as well.
            Otherwise, when creating a node with the same ID, it will automatically inherit the extended attributes left in the database.
        """
        pass
    def nodeExists(self, nodeId):
        """
        Check if a node exists within the database
        @return True if and only if a node with the given ID exists within the database
        """
        pass
    def _loadNodeBasicAttributes(self, node):
        """
        Load a node entry from the database (by node ID) and return
        the value (= basic attributes)
        """
        return self.conn.read(self.nodeTableId,  node)
    def _saveNode(self, node):
        """
        Save a node and its basic attribute set into the database.
        """
        dbValue = node.basicAttributes().__serialize()
        self.conn.put(self.nodeTableId,  {node.id() : dbValue})
    def _loadNodeExtendedAttributes(self,  node, keys):
        """
        Load the extended attribute for a node.
        @param keys A single key identifier or a list of identifiers.
        @return An array of values, in the same order as the keys.
        """
        dbKeys = []
        if type(keys) is list:
            for key in keys:
                dbKeys.append(ExtendedAttributes._serializeKey(node.id(),  key))
        else: #Single key
            dbKeys.append(ExtendedAttributes._serializeKey(node.id(),  keys))
        #Write
        return self.conn.read(self.nodeExtAttrTableId,  dbKeys)
    def _saveNodeExtendedAttribute(self,  node,  key,  value):
        """
        Save a single extended attribute for a node.
        @param node The node to serialize for
        @param key The attribute key to write
        @param value The attribute value to write
        """
        dbKey = ExtendedAttributes._serializeKey(node.id(),  key)
        self.conn.put(self.nodeTableId,  {dbKey : value})
