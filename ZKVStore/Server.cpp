/*
 * Server.cpp
 *
 *  Created on: 23.04.2013
 *      Author: uli
 */


#include <czmq.h>
#include <string>
#include <iostream> 
#include "Server.hpp"
#include "zutil.hpp"
#include "protocol.hpp"
#include "endpoints.hpp"

using namespace std;

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
    KeyValueServer* server = (KeyValueServer*) arg;
    zmsg_t* msg = zmsg_recv(server->responseProxySocket);
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
 * Poll handler for the main request/response socket
 */
static int handleRequestResponse(zloop_t *loop, zmq_pollitem_t *poller, void *arg) {
    KeyValueServer* server = (KeyValueServer*) arg;
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
            //Forward the message to the read worker controller, the response is sent asynchronously
            server->readWorkerController.send(&msg);
        } else if (requestType == RequestType::PutRequest || requestType == RequestType::DeleteRequest) {
            //We reuse the header frame and the routing information to send the acknowledge message
            //The rest of the msg (the table id + data) is directly forwarded to one of the handler threads
            //Get the routing information
            zframe_t* routingFrame = zmsg_first(msg);
            zframe_t* delimiterFrame = zmsg_next(msg);
            zframe_t* headerFrame = zmsg_next(msg);
            uint8_t writeFlags = getWriteFlags(headerFrame);
            //Duplicate the routing frame if we need it late
            if (!isPartsync(writeFlags)) {
                routingFrame = zframe_dup(routingFrame);
            }
            //Send the message to the update worker (--> processed async)
            server->updateWorkerController.send(&msg);
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

KeyValueServer::KeyValueServer(bool dbCompressionEnabled) : ctx(zctx_new()),
tables(),
tableOpenServer(ctx, tables.getDatabases(), dbCompressionEnabled),
externalPullSocket(NULL),
externalSubSocket(NULL),
updateWorkerController(ctx, tables),
readWorkerController(ctx, tables) {
    const char* reqRepUrl = "tcp://*:7100";
    const char* writeSubscriptionUrl = "tcp://*:7101";
    const char* errorPubUrl = "tcp://*:7102";
    //Initialize the sockets that run on the main thread
    externalRepSocket = zsocket_new(ctx, ZMQ_ROUTER);
    zsocket_bind(externalRepSocket, reqRepUrl);
    responseProxySocket = zsocket_new(ctx, ZMQ_PULL);
    zsocket_bind(responseProxySocket, externalRequestProxyEndpoint);
    //Now start the update and read workers
    //(before starting the worker threads the response sockets need to be bound)
    updateWorkerController.start();
    readWorkerController.start();
}

KeyValueServer::~KeyValueServer() {
    tables.cleanup();
    //Destroy the sockets
    if (externalRepSocket != nullptr) {
        zsocket_destroy(ctx, &externalRepSocket);
    }
    if (externalSubSocket != nullptr) {
        zsocket_destroy(ctx, &externalSubSocket);
    }
    if (externalPullSocket != nullptr) {
        zsocket_destroy(ctx, &externalPullSocket);
    }
    zsocket_destroy(ctx, &responseProxySocket);
    printf("Gracefully closed tables, exiting...\n");
    zctx_destroy(&ctx);
}

void KeyValueServer::start() {
    zloop_t *reactor = zloop_new();
    //The main thread listens to the external sockets and proxies responses from the worker thread
    zmq_pollitem_t externalPoller = {externalRepSocket, 0, ZMQ_POLLIN};
    if (zloop_poller(reactor, &externalPoller, handleRequestResponse, this)) {
        debugZMQError("Add external poller to reactor", errno);
    }
    zmq_pollitem_t responsePoller = {responseProxySocket, 0, ZMQ_POLLIN};
    if (zloop_poller(reactor, &responsePoller, proxyWorkerThreadResponse, this)) {
        debugZMQError("Add response poller to reactor", errno);
    }
    //Start the reactor loop
    zloop_start(reactor);
    //Cleanup (called when finished, e.g. by interrupt)
    zloop_destroy(&reactor);
}