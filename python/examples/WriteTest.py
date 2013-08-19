import YakDB.Connection

conn = YakDB.Connection()
conn.usePushMode()
#conn.useRequestReply
conn.connect("tcp://localhost:7101")
#Write 100 msgs with 1000 keys each
for i in range(1,100):
    data = {}
    for j in range(1,1000):
        data["Key"+str(j)] = "value"+str(j)
    print "Put " + str(i)
    conn.put(1, data)
