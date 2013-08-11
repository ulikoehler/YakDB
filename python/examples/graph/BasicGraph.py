#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph import Graph
import YakDB

#Initialize graph
conn = YakDB.Connection("tcp://localhost:7100")
graph = Graph.Graph(conn)
#Create the graph
graph.createNode("a")
graph.createNode("b")