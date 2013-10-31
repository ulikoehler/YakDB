/*
 * Server.hpp
 *
 *  Created on: 23.04.2013
 *      Author: uli
 */

#ifndef SERVER_HPP_
#define SERVER_HPP_
#include "UpdateWorker.hpp"
#include "ReadWorker.hpp"
#include "ConfigParser.hpp"
#include "AsyncJobRouter.hpp"
#include "Logger.hpp"
#include "LogServer.hpp"
#include "HTTPServer.hpp"

class KeyValueServer {
public:
    KeyValueServer(ConfigParser& configParser);
    void start();
    ~KeyValueServer();
    /**
     * The ZeroMQ context.
     * 
     */
    void* ctx;
    LogServer logServer;
    Tablespace tables;
    //External sockets
    void* externalRepSocket; //ROUTER socket that receives remote req/rep READ requests can only use this socket
    void* externalSubSocket; //SUB socket that subscribes to UPDATE requests (For mirroring etc)
    void* externalPullSocket; //PULL socket for UPDATE load balancinge
    void* responseProxySocket; //Worker threads connect to this PULL socket -- messages need to contain envelopes and area automatically proxied to the main router socket
    TableOpenServer tableOpenServer;
    UpdateWorkerController updateWorkerController;
    ReadWorkerController readWorkerController;
    AsyncJobRouterController asyncJobRouterController;
    YakHTTPServer httpServer;
    Logger logger; //The log source of the server itself, only to be used from the main thread
    ConfigParser& configParser;
private:
    void handleRequestResponse();
    void handlePushPull();
};


#endif /* SERVER_HPP_ */
