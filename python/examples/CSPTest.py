import zerodb
import random

#Create a dataset of 25k string KV pairs
data = {}
for i in range(1,25000):
    data[str(i)] = str(random.random())

conn = zerodb.Connection()
conn.connect("tcp://localhost:7100")

conn.put(1, data)

#Initialize job
job = conn.initializePassiveDataJob(1)

print "Got APID %d" % job.getAPID()

print job.requestDataChunk()

