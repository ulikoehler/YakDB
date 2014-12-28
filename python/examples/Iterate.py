#!/usr/bin/env python3
"""
An example of how to use YakDB iterators.
These iterators use batched scan requests with local buffers,
therefore you can iterate petabytes of YakDB data without
special constructs while using only kilobytes of RAM.
"""
import YakDB
from YakDB.Iterators import KeyValueIterator

#Initialize connection (in default=req/rep mode)
conn = YakDB.Connection()
#Connect to default setting for request-reply
conn.connect("tcp://localhost:7100")
#Initialize iterator. Optional arguments omitted
it = KeyValueIterator(conn, tableNo=1)
for key, value in it:
    #Write data in CSV-ish format
    print("{0},{1}".format(key, value))
