
#include <zmq.h>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <czmq.h>
#include <string>
#include <cstdio>
#include <thread>
#include <iostream>
#include <vector>
#include <sys/stat.h>
#include <mutex>
#include "TableOpenHelper.hpp"
#include "protocol.hpp"
#include "zutil.hpp"

//Feature toggle
#define BATCH_UPDATES
#define DEBUG_UPDATES
#define DEBUG_READ

const char* readWorkerThreadAddr = "inproc://readWorkerThreads";

using namespace std;

/**
 */
struct KVServer {

    KVServer(zctx_t* ctx, bool dbCompressionEnabled) : ctx(ctx), tables(), tableOpenServer(ctx, tables.getDatabases(), dbCompressionEnabled) {
    }

    /**
     * Cleanup. This must be called before the ZMQ context is destroyed
     */
    void cleanup() {
        tables.cleanup();
        //Destroy the sockets
    }

    ~KVServer() {

        printf("Gracefully closed tables, exiting...\n");
    }
    Tablespace tables;
    //External sockets
    void* externalRepSocket; //ROUTER socket that receives remote req/rep READ requests can only use this socket
    void* externalSubSocket; //SUB socket that subscribes to UPDATE requests (For mirroring etc)
    void* externalPullSocket; //PULL socket for UPDATE load balancinge
    void* responseProxySocket; //Worker threads connect to this PULL socket -- messages need to contain envelopes and area automatically proxied to the main router socket
    //Internal sockets
    void* readWorkerThreadSocket; //PUSH socket that distributes update requests among read worker threads
    TableOpenServer tableOpenServer;
    //Other stuff
    zctx_t* ctx;
};

/*
 * Request/response codes are determined by the first byte
 * Request codes (from client):
 *      1<Read request>: Read. Response: Serialized ReadResponse
 *      2<Update request>: Update. Response: 0 --> Acknowledge
 * Response codes (to client):
 *      0: Acknowledge
 */
int handleRequestResponse(zloop_t *loop, zmq_pollitem_t *poller, void *arg) {
    KVServer* server = (KVServer*) arg;
    //Initialize a scoped lock
    zmsg_t *msg = zmsg_recv(server->externalRepSocket);
    if (msg) {
        //The message consists of four frames: Client addr, empty delimiter, msg type (1 byte) and data
        assert(zmsg_size(msg) >= 3); //return addr + empty delimiter + protocol header [+ data frames]
        //Extract the frames
        zframe_t* addrFrame = zmsg_first(msg);
        zframe_t* delimiterFrame = zmsg_next(msg);
        zframe_t* headerFrame = zmsg_next(msg);
        //Check the header -- send error message if invalid
        string errorString;
        char* headerData = (char*) zframe_data(headerFrame);
        if (!checkProtocolVersion(headerData, zframe_size(headerFrame), errorString)) {
            fprintf(stderr, "Got illegal message from client\n");
            return 1;
        }
        RequestType requestType = (RequestType) (uint8_t) headerData[2];
        //        fprintf(stderr, "Got message of type %d from client\n", (int) msgType);
        if (requestType == RequestType::ReadRequest) {
            handleReadRequest(server->tables, msg, *(server->tableOpenHelper));
            zmsg_send(&msg, server->externalRepSocket);
        } else if (requestType == RequestType::PutRequest || requestType == RequestType::DeleteRequest) {
            //We reuse the header frame and the routing information to send the acknowledge message
            //The rest of the msg (the table id + data) is directly forwarded to one of the handler threads
            //Remove the routing information
            zframe_t* routingFrame = zmsg_unwrap(msg);
            //Remove the header frame
            headerFrame = zmsg_pop(msg);
            uint8_t writeFlags = getWriteFlags(headerFrame);
            zframe_destroy(&headerFrame);
            //Send the message to the update worker (--> processed async)
            zmsg_send(&msg, server->updateWorkerThreadSocket);
            //Send acknowledge message unless PARTSYNC is set (in which case it is sent in the update worker thread)
            if (!isPartsync(writeFlags)) {
                msg = zmsg_new();
                zmsg_wrap(msg, routingFrame); //Send response code 0x00 (ack)
                zmsg_addmem(msg, "\x31\x01\x20\x00", 4);
                zmsg_send(&msg, server->externalRepSocket);
            }
        } else if (requestType == RequestType::ServerInfoRequest) {
            //Server info requests are answered in the main thread
            const uint64_t serverFlags = SupportOnTheFlyTableOpen | SupportPARTSYNC | SupportFULLSYNC;
            const size_t responseSize = 3/*Metadata*/ + sizeof (uint64_t)/*Flags*/;
            char serverInfoData[responseSize]; //Allocate on stack
            serverInfoData[0] = magicByte;
            serverInfoData[1] = protocolVersion;
            serverInfoData[2] = ServerInfoResponse;
            memcpy(serverInfoData + 3, &serverFlags, sizeof (uint64_t));
            //Re-use the header frame and the original message and send the response
            zframe_reset(headerFrame, serverInfoData, responseSize);
            zmsg_send(&msg, server->externalRepSocket);
        } else {
            fprintf(stderr, "Unknown message type %d from client\n", (int) requestType);
        }
        cout << "Sending reply to" << (requestType == RequestType::PutRequest ? " put request " : " read request ") << endl;
    }
    return 0;
}

void initializeDirectoryStructure() {
    mkdir("tables", S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP);
}

/**
 * This function is called by the reactor loop to process responses being sent from the worker threads.
 * 
 * The worker threads can't directly use the main ROUTER socket because sockets may
 * only be used by one thread.
 * 
 * By proxying the responses (non-PARTSYNC responses are sent directly by the main
 * thread before the request has been processed by the worker thread) the main ROUTER
 * socket is only be used by the main thread,
 * @param loop
 * @param poller
 * @param arg A pointer to the current KVServer instance
 * @return 
 */
int proxyWorkerThreadResponse(zloop_t *loop, zmq_pollitem_t *poller, void *arg) {
    KVServer* server = (KVServer*) arg;
    zmsg_t* msg = zmsg_recv(server->externalRepSocket);
    //We assume the message contains a valid envelope.
    //Just proxy it. Nothing special here.
    //zmq_proxy is a loop and therefore can't be used here.
    int rc = zmsg_send(&msg, server->externalRepSocket);
    if (rc == -1) {
        debugZMQError("Proxy worker thread response", errno);
    }
    return 0;
}

/**
 * This data structure is used in the update worker thread to share info with
 * the update poll handler
 */
struct UpdateWorkerInfo {
    KVServer* server;
    void* pullSocket;
};

void initializeReadWorkers(zctx_t* ctx, KVServer* serverInfo) {
    //Initialize the push socket
    serverInfo->readWorkerThreadSocket = zsocket_new(ctx, ZMQ_PUSH);
    zsocket_bind(serverInfo->updateWorkerThreadSocket, updateWorkerThreadAddr);
    //Start the threads
    const int numThreads = 3;
    serverInfo->numReadThreads = numThreads;
    serverInfo->readWorkerThreads = new std::thread*[numThreads];
    for (int i = 0; i < numThreads; i++) {
        serverInfo->updateWorkerThreads[i] = new std::thread(updateWorkerThreadFunction, ctx, serverInfo);
    }
}

/**
 * ZDB KeyValueServer
 * Port 
 */
int main() {
    const char* reqRepUrl = "tcp://*:7100";
    const char* writeSubscriptionUrl = "tcp://*:7101";
    const char* errorPubUrl = "tcp://*:7102";
    //Ensure the tables directory exists
    initializeDirectoryStructure();
    //Create the ZMQ context
    zctx_t* ctx = zctx_new();
    //Create the object that will be shared between the threadsloop
    KVServer server(ctx);
    //Start the table opener thread (constructor returns after thread has been started. Cleanup on scope exit.)
    //Initialize all worker threads
    initializeUpdateWorkers(ctx, &server);
    //Initialize the sockets that run on the main thread
    server.externalRepSocket = zsocket_new(ctx, ZMQ_ROUTER);
    server.responseProxySocket = zsocket_new(ctx, ZMQ_PULL);
    zsocket_bind(server.responseProxySocket, "inproc://mainRequestProxy");
    zsocket_bind(server.externalRepSocket, reqRepUrl);
    //Start the loop
    printf("Starting server...\n");
    zloop_t *reactor = zloop_new();
    //The main thread listens to the external sockets and proxies responses from the worker thread
    zmq_pollitem_t externalPoller = {server.externalRepSocket, 0, ZMQ_POLLIN};
    zloop_poller(reactor, &externalPoller, handleRequestResponse, &server);
    zmq_pollitem_t responsePoller = {server.responseProxySocket, 0, ZMQ_POLLIN};
    zloop_poller(reactor, &responsePoller, proxyWorkerThreadResponse, &server);
    //Start the reactor loop
    zloop_start(reactor);
    //Cleanup (called when finished)
    zloop_destroy(&reactor);
    server.cleanup(); //Before the context is destroyed
    zctx_destroy(&ctx);
    //All tables are closed at scope exit.
}