#!/usr/bin/env python
#This tester storms a server with request that cause table opens requests
#Each table is opened more than once.
import YakDB.Connection

conn = YakDB.Connection()
#Use push mode to
conn.usePushMode()
#conn.useRequestReply
conn.connect("tcp://localhost:7101")
#For table no in [1..100], issue 5 identical requests
for i in range(1,100):
    for j in range(0,5):
        conn.put(i, {"0":"1"})
#Delete what we just wrote
for i in range(1,100):
    conn.delete(i, "0")