#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph import Graph
import YakDB

#Initialize graph
conn = YakDB.Connection("tcp://localhost:7100")
graph = Graph.Graph(conn)
#Create some basic nodes
nodeX = graph.createNode("x")
nodeY = graph.createNode("y")
nodeZ = graph.createNode("z")
edgeXY = graph.createEdge("x","y")
graph.createEdge("y","z")
graph.createEdge("z","x")
#Print some node info
print "Node 'x': %s" % nodeX
print "Edge 'x->y': %s" % edgeXY
print "Node 'x' edges: %s" % nodeX.getEdges()
print "Node 'x' incoming edges: %s" % nodeX.getIncomingEdges()
print "Node 'x' outgoing edges: %s" % nodeX.getOutgoingEdges()
#Create some nodes with basic attributes
nodeA = graph.createNode("a", basicAttrs={"nodeText":"This is node a"})
nodeB = graph.createNode("b", basicAttrs={"nodeText":"This is node b"})
nodeC = graph.createNode("c", basicAttrs={"nodeText":"This is node c"})
graph.createEdge("a", "b", basicAttrs={"text":"This is an edge from a to b"})
graph.createEdge("b", "c", basicAttrs={"text":"This is an edge from b to c"})
graph.createEdge("c", "a", basicAttrs={"text":"This is an edge from c to a"})
graph.createEdge("x", "b", basicAttrs={"text":"This is an edge from a to b"})
#Set some additional attributes
nodeA.basicAttributes["myattr"] = "myvalue"
#Print the node list
print "Nodes in the graph (all at once): %s" % graph.nodes
#Use the node iterator to do the same thing
print "Nodes in graph (using iterator):"
for node in graph.iternodes():
    print "\t%s - Indegree %d - Outdegree %d" % (node.id, node.indegree, node.outdegree)
#Create some extended attributes
extAttrsX = nodeX.extendedAttributes
extAttrsX["foo"] = "bar"
assert extAttrsX["foo"] == "bar"
extAttrsX.setAttributes({"a":"b","c":"d","e":"f"})
del extAttrsX["foo"]
print "Node X extended attributes: %s" % str(extAttrsX.getAllAttributes())
