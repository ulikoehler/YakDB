import zerodb
import random
import sys

#Create a dataset of 1k string KV pairs
data = {}
for i in range(1,10000):
    data[str(i)] = str(random.random())

conn = zerodb.Connection()
conn.connect("tcp://localhost:7100")

sys.stdout.write("Key to retrieve: ")
key = sys.stdin.readline()

print(conn.read(1, key))
