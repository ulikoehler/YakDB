#!/usr/bin/env python
# -*- coding: utf8 -*-

from BasicAttributes import BasicAttributes
from ExtendedAttributes import ExtendedAttributes
import Node as Node
import Edge as Edge
from Identifier import Identifier

class Graph:
    """
    A directed graph represented 
    as adjacency list in multiple tables in a YakDB database.
    
    """
    def __init__(self,  conn, nodeTableId=2,  edgeTableId=3, extendedAttributesTable=4, partsync=True):
        """
        Create a new graph from a YakDB connection.
        @param partsync Set this to false if you do not need to assume any write
            guarantees subsequent reads to return the written value.
            A False value guarantees better write performance.
        """
        self.conn = conn
        self.nodeTableId = nodeTableId
        self.edgeTableId = edgeTableId
        self.extendedAttributesTable = extendedAttributesTable
        self.partsync = partsync
    def createNode(self,  nodeId, basicAttrs=None, save=True):
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
        node = Node.Node(nodeId, self, basicAttrs)
        if save: node._save()
        return node
    def createEdge(self, sourceNode, targetNode, type="", basicAttrs=None, save=True):
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
        edge = Edge.Edge(sourceNode, targetNode, self, type, basicAttrs)
        if save: edge._save()
        return edge
    def deleteNode(self, nodeId,  deleteExtAttrs=True):
        """
        Delete a node.
        If the node does not exist, the request will be ignored.
        @param deleteExtAttrs If this is set to true, all node extended attributes will be deleted as well.
            Otherwise, when creating a node with the same ID, it will automatically inherit the extended attributes left in the database.
        """
        node = Node.Node(nodeId, self)
        node.delete() #TODO implement
        #TODO delete extattrs
    def nodes(self):
        """
        Get a list of all nodes in the graph
        """
        return self._scanNodes()
    def _scanNodes(self, startKey="", endKey="", limit=None):
        """
        Do a scan over the node table.
        @return A list of Node objects
        """
        nodes = []
        scanResult = self.conn.scan(self.nodeTableId, startKey, endKey, limit)
        for (key, value) in scanResult.iteritems():
            #If the edge and node table are the same, we need to skip edges
            if key.find("\x1F") != -1: #Type separator
                continue
            basicAttrs = BasicAttributes._parseAttributeSet(value)
            node = Node.Node(key, self, basicAttrs)
            nodes.append(node)
        return nodes
    def _scanEdges(self, startKey, endKey, limit=None):
        """
        Do a scan over the edge table.
        Internally used by the node class.
        @param startKey The edge table start key
        @param endKey The edge table start key
        @return a list of edge objects
        """
        scanResult = self.conn.scan(self.edgeTableId, startKey, endKey, limit)
        edges = []
        for (key, value) in scanResult.iteritems():
            #Deserialize key and value
            edgeTuple = Edge.Edge._deserializeEdge(key)
            basicAttrs = BasicAttributes._parseAttributeSet(value)
            edge = Edge.Edge(edgeTuple[0], edgeTuple[1], self, edgeTuple[2], basicAttrs)
            edges.append(edge)
        return edges
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
        self.conn.put(self.edgeTableId, putDict, partsync=self.partsync)
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
    def _loadExtendedAttributes(self,  dbKeys):
        """
        Load a list of extended attributes by key set
        @param dbKeys An array of database keys to read
        @return An array of values, in the same order as the keys. A single string is also allowed.
        """
        #Write
        return self.conn.read(self.extendedAttributesTable, dbKeys)
    def _loadExtendedAttributeRange(self,  startKey, endKey, limit=None):
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
    def _saveExtendedAttributes(self,  kvDict):
        """
        Save a single extended attribute for a node.
        @param node The node to serialize for
        @param kvDict A dictionary from database key to value
        """
        self.conn.put(self.extendedAttributesTable, kvDict, partsync=self.partsync)
    def _deleteExtendedAttributes(self,  keyList):
        """
        Executes a deletion on the extended attribute tables
        @param keyList A list of database keys to delete
        """
        self.conn.delete(self.extendedAttributesTable, keyList, partsync=self.partsync)
