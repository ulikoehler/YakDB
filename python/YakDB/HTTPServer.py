#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
The YakDB development HTTP server.

Originally this was implemented in C++ but it didn't prove to be
worth the significant complexity.

This server is not built for speed, but for simplicity,
so for applications requiring high performance,
using ZMQ directly should be preferred.

Being a stateless server, this implementation supports
multiple instances 
"""
from bottle import run, Bottle, static_file

class YakHTTPServer(Bottle):
    def __init__(self, staticFilePath):
        self.staticFilePath = staticFilePath
        Bottle.__init__(self)
        self.initRoutes()
    def initRoutes(self):
        ### Initialize static routes
        @self.route('/')
        def indexPage():
            return static_file("index.html", root=self.staticFilePath)
        @self.route('/<path:path>')
        def staticFile(path):
            return static_file(path, root=self.staticFilePath)
    def run(self, port=7110):
        run(self, host='localhost', port=port)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()  
    parser.add_argument("--static", help="The path where the static web UI components are stored")
    parser.add_argument("-p", "--port", default=7110, help="The port to listen on")
    args = parser.parse_args()

    server = YakHTTPServer(args.static)
    server.run()