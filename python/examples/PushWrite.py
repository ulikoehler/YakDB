import YakDB.Connection

conn = YakDB.Connection()
conn.usePushMode()
conn.connect("tcp://localhost:7101")
print "Connected"
conn.put(1, {"a":"b"})
