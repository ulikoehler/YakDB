import optparse
import sys
import zerodb

class ConnectMode:
    requestReply = 0
    pushPull = 1
    pubSub = 2

if __name__ == "__main__":
    parser = optparse.OptionParser()
    #Server options
    serverOptsGroup = optparse.OptionGroup(parser, "Server options")
    serverOptsGroup.add_option("-s","--server", help="Specifies server URL to connect to", default="tcp://localhost:7100", action='store', dest="serverURL")
    serverOptsGroup.add_option("-g","--server-group", help="Specifies the server group to connect to for PUB/SUB connections", action='store', dest="serverGroup")
    #Connect mode
    connectModeGroup = optparse.OptionGroup(parser, "Connection modes")
    connectModeGroup.add_option("-q","--request-reply", help="Use request-reply connect mode", action="store_const", const=ConnectMode.requestReply, dest="connectMode")
    connectModeGroup.add_option("-p","--push-pull", help="Use push-pull connect mode", action="store_const", const=ConnectMode.pushPull, dest="connectMode")
    connectModeGroup.add_option("-b","--pub-sub", help="Use public-subscribe connect mode", action="store_const", const=ConnectMode.pubSub, dest="connectMode")
    #Database options
    dbOptsGroup = optparse.OptionGroup(parser, "Database options")
    dbOptsGroup.add_option("-t","--table", help="Set table number to use", action="store", default=1, type="int", dest="tableNo")
    dbOptsGroup.add_option("-r","--repl", help="Start into a REPL where db is the database connection", action="store_true", dest="repl")
    #Add all opt groups
    parser.add_option_group(serverOptsGroup)
    parser.add_option_group(connectModeGroup)
    parser.add_option_group(dbOptsGroup)
    #Parse
    (opts, args) = parser.parse_args()
    #
    #Setup connection
    #
    sys.stderr.write("Connecting to %s\n" % opts.serverURL)
    db = zerodb.ZeroDBConnection()
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
        code.InteractiveConsole(locals=globals()).interact("ZeroDB REPL -- db is the ZeroDB connection")
    else: #Single-command mode
        tableNo = opts.tableNo
        cmd = args[0]
        commands = ["read","exists","put"]
        if cmd not in commands:
            print "Command '%s' not available - available commands: %s" % (cmd, ", ".join(commands))
            sys.exit(1)
        elif cmd == "exists":
            existsInput = args[1:]
            output = db.exists(tableNo, existsInput)
            #Convert value-only to key-->value map
            outMap = {}
            for index, inval in enumerate(existsInput):
                outMap[inval] = output[index]
            print outMap
        elif cmd == "read":
            readInput = args[1:]
            output = db.read(tableNo, readInput)
            #Convert value-only to key-->value map
            outMap = {}
            for index, inval in enumerate(readInput):
                outMap[inval] = output[index]
            print outMap
        elif cmd == "put":
            key = args[1]
            value = args[2]
            output = db.put(tableNo, {key: value})
            #Convert value-only to key-->value map
            print "Put '%s' --> '%s'" % (key, value)