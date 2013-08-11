#!/usr/bin/env python
# -*- coding: utf8 -*-

from ExtendedAttributes import ExtendedAttributes
import Node
import Edge
from Identifier import Identifier

class Graph:
    """
    A directed graph represented 
    as adjacency list in multiple tables in a YakDB database.
    
    """
    def __init__(self,  conn, nodeTableId=2,  edgeTableId=3, extendedAttributesTable=4):
        """
        Create a new graph from a YakDB connection.
        """
        self.conn = conn
        self.nodeTableId = nodeTableId
        self.edgeTableId = edgeTableId
        self.extendedAttributesTable = extendedAttributesTable
    def createNode(self,  nodeId,  basicAttributes=None, save=True):
        """
        Add a node to the graph.
        If a node with the same ID already exists in the graph,
        the already-existing node will be ignored wären auch noch stromsparender.

        (its existing basic attributes will be overwritten),
        but its extended attributes won't be modified
        
        @param basicAttributes A dictionary of basic attributes or None to use empty set
        @param save Set this to false to avoid writing the node to the database.
        @return the node object.
        """
        node = Node.Node(nodeId, self, basicAttributes)
        if save: node._save()
        return node
    def createEdge(self, sourceNode, targetNode, type="", basicAttributes=None, save=True):
        """
        Create a directed edge between two nodes.
        
        If you use node IDs instead of nodes as parameters,
        the nodes will not be saved automatically and will not contain
        any basic attributes.
        You can save the nodes
        
        @param sourceNode A source node identifier or a Node instance that acts as source
        @param targetNode A target node identifier or a Node instance that acts as target
        @param type The type of the edge
        @return The edge instance
        """
        #Check if both nodes are from the same graph.
        if isinstance(sourceNode, Node.Node) and isinstance(targetNode, Node.Node):
            if sourceNode.graph() != targetNode.graph():
                raise ConsistencyException("Source and target node are not from the same graph")
        #If the arguments are strings, ensure they're proper identifiers
        if isinstance(sourceNode, str):
            Identifier.checkIdentifier(sourceNode)
        if isinstance(targetNode, str):
            Identifier.checkIdentifier(targetNode)
        #Ensure we have strings because the edge constructor takes them
        if isinstance(sourceNode, Node.Node):
            sourceNode = sourceNode.id
        if isinstance(targetNode, Node.Node):
            targetNode = targetNode.id
        #Create the edge and save it to the database
        edge = Edge.Edge(sourceNode, targetNode, self, basicAttributes)
        if save: edge._save()
        return edge
    def deleteNode(self, nodeId,  deleteExtAttrs=True):
        """
        Delete a node.
        If the node does not exist, the request will be ignored.
        @param deleteExtAttrs If this is set to true, all node extended attributes will be deleted as well.
            Otherwise, when creating a node with the same ID, it will automatically inherit the extended attributes left in the database.
        """
        pass
    def __convertToNode(obj):
        """
        
        """
    def nodeExists(self, nodeId):
        """
        Check if a node exists within the database
        @return True if and only if a node with the given ID exists within the database
        """
        pass
    def _writeEdge(self, activeKey, passiveKey, serializedBasicAttrs):
        """
        Write both versions of an edge to the database.
        @param activeKey The active-version serialized database key
        @param passiveKey The passive-version serialized database key
        @param serializedBasicAttrs The serialized basic attributes
        """
        putDict = {activeKey: serializedBasicAttrs, passiveKey: serializedBasicAttrs}
        self.conn.put(self.edgeTableId)
    def _saveNode(self, node):
        """
        Save a node and its basic attribute set into the database.
        """
        dbValue = node.basicAttributes.serialize()
        self.conn.put(self.nodeTableId,  {node.id : dbValue})
    def _loadNodeBasicAttributes(self, node):
        """
        Load a node entry from the database (by node ID) and return
        the value (= basic attributes)
        """
        return self.conn.read(self.nodeTableId,  node)
    def _loadExtendedAttributeSet(self,  entityIdkeys):
        """
        Load a list of extended attributes by key set
        @param keys An array of database keys to read
        @return An array of values, in the same order as the keys.
        """
        #Write
        return self.conn.read(self.extendedAttributesTable, dbKeys)
    def _loadExtendedAttributes(self,  startKey, endKey, limit=None):
        """
        Load a set of extended attributes by scanning the extended attribute range.
        The end key is automatically determined by the entity ID
        
        @param limit Set this to a integer to impose a limit on the number of keys. None means no limit.
        @return A dictionary of the extended attributes (key = extended attribute key)
        """
        #Do the scan
        scanResult = self.conn.scan(self.extendedAttributesTable, startKey,  endKey,  limit)
        #Strip the entity name and separator from the scanned keys
        ret = {}
        for key in scanResult.iterkeys():
            attrKey = ExtendedAttributes._getAttributeKeyFromDBKey(key)
            attrValue = scanResult[key]
            ret[attrKey] = attrValue
        return ret
    def _saveExtendedAttribute(self,  entity,  key,  value):
        """
        Save a single extended attribute for a node.
        @param node The node to serialize for
        @param key The attribute key to write
        @param value The attribute value to write
        """
        dbKey = ExtendedAttributes._serializeKey(entity.id(),  key)
        self.conn.put(self.extendedAttributesTable,  {dbKey : value})
