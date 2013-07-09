import zerodb

conn = zerodb.ZeroDBConnection()
conn.connect("tcp://localhost:7100")

print conn.serverInfo()