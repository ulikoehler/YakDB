#!/usr/bin/env python
# Job initi
import YakDB

conn = YakDB.Connection()
conn.connect("tcp://localhost:7100")