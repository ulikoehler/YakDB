import zerodb

conn = zerodb.ZeroDBConnection()
conn.connect("tcp://localhost:7100")

raw_input('Key to read ')
