import zerodb
import random
import zmq

#Create a dataset of 1k string KV pairs
data = {}
for i in range(1,1000):
    data[str(i)] = str(random.random())

conn = zerodb.Connection()
conn.connect("tcp://localhost:7100")

conn.put(1, data)
