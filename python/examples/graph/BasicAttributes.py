#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph import Graph
import YakDB

#Initialize graph
conn = YakDB.Connection("tcp://localhost:7100")
graph = Graph.Graph(conn)
#Create the graph
graph.createNode("a", basicAttrs={"nodeText":"This is node a"})
graph.createNode("b", basicAttrs={"nodeText":"This is node b"})
graph.createNode("c", basicAttrs={"nodeText":"This is node c"})
graph.createEdge("a", "b", basicAttrs={"text":"This is an edge from a to b"})
graph.createEdge("b", "c", basicAttrs={"text":"This is an edge from b to c"})
graph.createEdge("c", "a", basicAttrs={"text":"This is an edge from c to a"})