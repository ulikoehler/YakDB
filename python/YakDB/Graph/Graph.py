#!/usr/bin/env python
# -*- coding: utf8 -*-

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
    def __saveNode(self, node):
        """
        Save a node and its basic attribute set into the database
        """
        dbKey = node.id()
        #TODO
    def __saveNodeExtendedAttribute(self,  node,  key,  value):
        pass
        #TODO
