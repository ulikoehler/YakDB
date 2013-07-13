import zerodb

conn = zerodb.Connection()
conn.connect("tcp://localhost:7100")

print conn.serverInfo()