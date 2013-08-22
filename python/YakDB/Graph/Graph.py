#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph.BasicAttributes import BasicAttributes
from YakDB.Graph.ExtendedAttributes import ExtendedAttributes
from YakDB.Graph.Node import Node
from YakDB.Graph.Edge import Edge
from YakDB.Exceptions import ParameterException
from YakDB.Graph.Exceptions import ConsistencyException
from YakDB.Graph.Identifier import Identifier
from YakDB.Iterators import KeyValueIterator
from YakDB.Graph.Iterators import NodeIterator

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
        self.partsyncFlag = partsync
    @property
    def partsync(self):
        """
        Whether the graph guarantees reads directly following writes
        always return the written value.
        If this is set to true (default), it has a negative effect
        on write latency and therefore throughput.
        """
        return self.partsyncFlag
    @partsync.setter
    def partsync(self, flag):
        """
        Set or clear the partsync flag.
        """
        if type(flag) is not bool:
            raise ParameterException("flag value must be a bool!")
        self.partsyncFlag = flag
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
        node = Node(nodeId, self, basicAttrs)
        if save: node.save()
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
        if isinstance(sourceNode, Node) and isinstance(targetNode, Node):
            if sourceNode.graph() != targetNode.graph():
                raise ConsistencyException("Source and target node are not from the same graph")
        #If the arguments are strings, ensure they're proper identifiers
        
        #Ensure we have strings because the edge constructor takes them
        if isinstance(sourceNode, Node):
            sourceNode = sourceNode.id
        if isinstance(targetNode, Node):
            targetNode = targetNode.id
        #Create the edge and save it to the database
        edge = Edge(sourceNode, targetNode, self, type, basicAttrs)
        if save: edge.save()
        return edge
    def deleteNode(self, nodeId,  deleteExtAttrs=True):
        """
        Delete a node.
        If the node does not exist, the request will be ignored.
        @param deleteExtAttrs If this is set to true, all node extended attributes will be deleted as well.
            Otherwise, when creating a node with the same ID, it will automatically inherit the extended attributes left in the database.
        """
        node = Node(nodeId, self)
        node.delete() #TODO implement
        #TODO delete extattrs
    @property
    def nodes(self):
        """
        Get a list of all nodes in the graph
        """
        return self.scanNodes()
    def iternodes(self, startKey=None, endKey=None, limit=None, chunkSize=1000, keyFilter=None, valueFilter=None):
        """
        Get a node iterator
        """
        kvIt = KeyValueIterator(self.conn, tableNo=self.nodeTableId, startKey=startKey, endKey=endKey, chunkSize=chunkSize, limit=limit, keyFilter=keyFilter, valueFilter=valueFilter)
        return NodeIterator(self, kvIt)
    def scanNodes(self, startKey=None, endKey=None, limit=None):
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
            node = Node(key, self, basicAttrs)
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
            edgeTuple = Edge._deserializeEdge(key)
            basicAttrs = BasicAttributes._parseAttributeSet(value)
            edge = Edge(edgeTuple[0], edgeTuple[1], self, edgeTuple[2], basicAttrs)
            edges.append(edge)
        return edges
    def nodeExists(self, nodeId):
        """
        Check if a node (or a list of nodes exists within the database
        @param 
        @return A list of bools, in the same order as the argument array, True if the node exists.
        """
        return self.exists(self.nodeTableId, nodeId)
    def saveEdge(self, edge):
        """
        Write an edge to the database.
        """
        if not isinstance(edge, Edge):
            raise ParameterException("Edge parameter must by an Edge instance!")
        serializedBasicAttrs = edge.basicAttributes.serialize()
        putDict = {edge.activeKey: serializedBasicAttrs,
                   edge.passiveKey: serializedBasicAttrs}
        self.conn.put(self.edgeTableId, putDict, partsync=self.partsyncFlag)
    def saveNode(self, node):
        """
        Save a node and its basic attribute set into the database.
        """
        if not isinstance(node, Node):
            raise ParameterException("Node parameter must by a Node instance!")
        self.conn.put(self.nodeTableId,
                      {node.id : node.basicAttributes.serialize()},
                      partsync=self.partsyncFlag)
    def getNode(self, nodeId, loadBasicAttributes=True):
        """
        Read a node from the database (by ID).
        @param nodeId The ID to load
        @param loadBasicAttributes Set this to false if you want to ignore basic attributes.
            If this parameter is set to False, no database operations are involved.
            A common usecase is when you know you don't have
        @return The node instance
        """
        basicAttributes = {}
        if loadBasicAttributes:
            basicAttributes = _readNodeBasicAttributes(self, nodeId)
        return Node(nodeId, self, basicAttributes)
    def readExtendedAttributes(self, entityId, keys):
        """
        Read a list of extended attributes by key set
        @param dbKeys An array of database keys to read, or a single string to read
        @return An array of values, in the same order as the keys.
        """
        if type(keys) is str: keys = [keys]
        dbKeys = [ExtendedAttributes._serializeKey(entityId, key) for key in keys]
        return self.conn.read(self.extendedAttributesTable, dbKeys)  
    def scanExtendedAttributes(self, entityId, startAttribute=None, endAttribute=None, limit=None):
        """
        Load a set of extended attributes by scanning the extended attribute range.
        
        @param limit Set this to a integer to impose a limit on the number of keys. None means no limit.
        @return A dictionary of the extended attributes (key = extended attribute key)
        """
        #Calculate the start and end database keys
        dbStartKey, dbEndKey = ExtendedAttributes._getEntityScanKeys(entityId)
        if startAttribute is not None:
            dbStartKey = ExtendedAttributes._serializeKey(entityId, startAttribute)
        if endAttribute is not None:
            dbEndKey = ExtendedAttributes._serializeKey(entityId, endAttribute)
        #Do the scan
        scanResult = self.conn.scan(self.extendedAttributesTable, dbStartKey,  dbEndKey,  limit)
        #Strip the entity name and separator from the scanned keys
        ret = {}
        for key,value in scanResult.iteritems():
            attrKey = ExtendedAttributes._getAttributeKeyFromDBKey(key)
            ret[attrKey] = value
        return ret
    def deleteExtendedAttributes(self, entityId, keyList):
        """
        Executes a deletion on the extended attribute tables
        @param keyList A list of database keys to delete (single string is also allowed
        """
        if type(keyList) is str: keyList = []
        dbKeys = [ExtendedAttributes._serializeKey(entityId, key) for key in keyList]
        self.conn.delete(self.extendedAttributesTable, keyList, partsync=self.partsyncFlag)
    def deleteExtendedAttributeRange(self, entityId, startAttribute=None, endAttribute=None, limit=None):
        """
        Delete a range of extended attributes.
        @param entityId The entity Id to delete attributes for
        """
        dbStartKey, dbEndKey = ExtendedAttributes._getEntityScanKeys(entityId)
        if startKey is not None:
            dbStartKey = ExtendedAttributes._serializeKey(entityId, startKey)
        if endKey is not None:
            dbEndKey = ExtendedAttributes._serializeKey(entityId, endKey)
        self.conn.deleteRange(self.extendedAttributesTable, dbStartKey,  dbEndKey,  limit)
    def saveExtendedAttributes(self, entityId, attrDict):
        """
        Save one or multiple extended attributes to the database
        @param node The node to serialize for
        @param attrDict A dictionary from key to value
        """
        if type(attrDict) is not dict:
            raise ParameterException("attrDict parameter must be a Dictionary!")
        dbDict = {}
        for key, value in attrDict.iteritems():
            dbKey = ExtendedAttributes._serializeKey(entityId, key)
            dbDict[dbKey] = value
        self.conn.put(self.extendedAttributesTable, dbDict, partsync=self.partsyncFlag)
    def _readNodeBasicAttributes(self, node):
        """
        Load a node entry from the database (by node ID) and return
        the value (= basic attributes)
        """
        return self.conn.read(self.nodeTableId,  node)