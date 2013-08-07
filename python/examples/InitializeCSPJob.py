#!/usr/bin/env python
import zerodb

conn = zerodb.Connection()
conn.connect("tcp://localhost:7100")