import optparse
import sys
import zerodb

class ConnectMode:
    requestReply = 0
    pushPull = 1
    pubSub = 2

def exists(db, tableNo, keys):
    output = db.exists(tableNo, keys)
    #Convert value-only to key-->value map
    outMap = {}
    for index, inval in enumerate(keys):
        outMap[inval] = output[index]
    print outMap

def read(db, tableNo, keys):
    output = db.read(tableNo, keys)
    #Convert value-only to key-->value map
    outMap = {}
    for index, inval in enumerate(keys):
        outMap[inval] = output[index]
    print outMap

def put(db, tableNo, key, value):
    db.put(tableNo, {key: value})
    #Convert value-only to key-->value map
    print "Put '%s' --> '%s'" % (key, value)

def delete(db, tableNo, keys):
    db.delete(tableNo, keys)
    print "Deleted [%s]" % ", ".join(keys)

def scan(db, tableNo, fromKey, toKey):
    #Data is remapped in ZeroDB class
    print db.scan(tableNo, fromKey, toKey)
    
def count(db, tableNo, fromKey, toKey):
    print db.count(tableNo, fromKey, toKey)

def deleteRange(db, tableNo, fromKey, toKey):
    db.deleteRange(tableNo, fromKey, toKey)

def info(db):
    print db.serverInfo()

if __name__ == "__main__":
    parser = optparse.OptionParser()
    #Server options
    serverOptsGroup = optparse.OptionGroup(parser, "Server options")
    serverOptsGroup.add_option("-s","--server", help="Specifies server URL to connect to", default="tcp://localhost:7100", action='store', dest="serverURL")
    serverOptsGroup.add_option("-g","--server-group", help="Specifies the server group to connect to for PUB/SUB connections", action='store', dest="serverGroup")
    #Connect mode
    connectModeGroup = optparse.OptionGroup(parser, "Connection modes")
    connectModeGroup.add_option("-e","--request-reply", help="Use request-reply connect mode", action="store_const", const=ConnectMode.requestReply, dest="connectMode")
    connectModeGroup.add_option("-p","--push-pull", help="Use push-pull connect mode", action="store_const", const=ConnectMode.pushPull, dest="connectMode")
    connectModeGroup.add_option("-b","--pub-sub", help="Use public-subscribe connect mode", action="store_const", const=ConnectMode.pubSub, dest="connectMode")
    #Database options
    dbOptsGroup = optparse.OptionGroup(parser, "Database options")
    dbOptsGroup.add_option("-t","--table", help="Set table number to use", action="store", default=1, type="int", dest="tableNo")
    #CLI options
    cliOptsGroup = optparse.OptionGroup(parser, "CLI options")
    cliOptsGroup.add_option("-q","--quiet", help="Don't print connection info", action="store_true", dest="quiet", default=False)
    cliOptsGroup.add_option("-r","--repl", help="Start into a REPL where db is the database connection", action="store_true", dest="repl")
    #Add all opt groups
    parser.add_option_group(serverOptsGroup)
    parser.add_option_group(connectModeGroup)
    parser.add_option_group(dbOptsGroup)
    parser.add_option_group(cliOptsGroup)
    #Parse
    (opts, args) = parser.parse_args()
    #
    #Setup connection
    #
    if not opts.quiet:
        sys.stderr.write("Connecting to %s\n" % opts.serverURL)
    db = zerodb.Connection()
    #Default is req/rep
    if opts.connectMode is ConnectMode.pushPull:
        db.usePushMode()
    elif opts.connectMode is ConnectMode.pubSub:
        db.usePubMode()
    else:
        db.useRequestReplyMode()
    #Connect
    db.connect(opts.serverURL)
    #s
    if opts.repl:
        import code
        code.InteractiveConsole(locals=globals()).interact("ZeroDB REPL -- Raw connection is available in 'db' variable")
    else: #Single-command mode
        tableNo = opts.tableNo
        cmd = args[0]
        #Extract other args
        arg1 = None
        arg2 = None
        if len(args) >= 2: arg1 = args[1]
        if len(args) >= 3: arg2 = args[2]
        commands = ["open","read","exists","put","delete","deleterange","scan","count","info"]
        if cmd not in commands:
            print "Command '%s' not available - available commands: %s" % (cmd, ", ".join(commands))
            sys.exit(1)
        elif cmd == "info": info(db)
        elif cmd == "exists": exists(db, tableNo, args[1:])
        elif cmd == "read": read(db, tableNo, args[1:])
        elif cmd == "put": put(db, tableNo, arg1, arg2)
        elif cmd == "delete": delete(db, tableNo, args[1:])
        elif cmd == "scan": scan(db, tableNo, arg1, arg2)
        elif cmd == "deleterange": deleteRange(db, tableNo, arg1, arg2)
        elif cmd == "count": count(db, tableNo, arg1, arg2)
            