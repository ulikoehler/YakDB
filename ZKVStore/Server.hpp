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

const char* const mainRequestProxyEndpoint = "inproc://mainRequestProxy";

class KeyValueServer {
public:
    KeyValueServer(zctx_t* ctx, bool dbCompressionEnabled = true);

    /**
     * Cleanup. This must be called before the ZMQ context is destroyed
     */
    void cleanup() {
        tables.cleanup();
        //Destroy the sockets
    }

    void start();

    ~KeyValueServer();
    Tablespace tables;
    //External sockets
    void* externalRepSocket; //ROUTER socket that receives remote req/rep READ requests can only use this socket
    void* externalSubSocket; //SUB socket that subscribes to UPDATE requests (For mirroring etc)
    void* externalPullSocket; //PULL socket for UPDATE load balancinge
    void* responseProxySocket; //Worker threads connect to this PULL socket -- messages need to contain envelopes and area automatically proxied to the main router socket
    TableOpenServer tableOpenServer;
    UpdateWorkerController updateWorkerController;
    ReadWorkerController readWorkerController;
    //Other stuff
    zctx_t* ctx;
};


#endif /* SERVER_HPP_ */
