import zerodb
import random

#Create a dataset of 1k string KV pairs
data = {}
for i in range(1,1000):
    data[str(i)] = str(random.random())

conn = zerodb.ZeroDBConnection()
conn.connect("tcp://localhost:7100")
conn.writeDict(0, data)

print conn.serverInfo()