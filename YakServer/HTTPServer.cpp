#include "HTTPServer.hpp"
#include <czmq.h>
#include <iostream>
#include "zutil.hpp"

#define controlEndpoint "inproc://http/control"

using namespace std;

YakHTTPServer::YakHTTPServer(zctx_t* ctxParam, const std::string& endpoint) : ctx(ctxParam), logger(ctx, "HTTP Server") {
    logger.debug("Binding HTTP server to " + endpoint);
    routerSocket = zsocket_new(ctx, ZMQ_ROUTER);
    controlSocket = zsocket_new_bind(ctx, ZMQ_PAIR, controlEndpoint);
    zsocket_set_router_raw(routerSocket, 1);
    //TODO proper error handling
    assert(zsocket_bind(routerSocket, endpoint.c_str()) != -1);
    //Start thread
    thread = new std::thread(std::mem_fun(&YakHTTPServer::workerMain), this);
}

void YakHTTPServer::workerMain() {
    logger.trace("HTTP Server starting");
    zmq_msg_t replyAddr;
    zmq_msg_init(&replyAddr);
    void* controlRecvSocket = zsocket_new_connect(ctx, ZMQ_PAIR, controlEndpoint);
    zmq_pollitem_t items[2];
    items[0].socket = routerSocket;
    items[0].events = ZMQ_POLLIN;
    items[1].socket = controlRecvSocket;
    items[1].events = ZMQ_POLLIN;
    while(true) {
         //  Get HTTP request
        assert(zmq_poll(items, 2, -1) != -1);
        //Check if we received a control msg
        if(items[1].revents) {
            //It must be a stop message
            zmq_recv(controlRecvSocket, nullptr, 0, 0);
            break;
        }
        int rc = zmq_msg_recv(&replyAddr, routerSocket, 0);
        assert(rc != -1);
        assert(zmq_msg_more(&replyAddr));
        char *request = zstr_recv (routerSocket);
        puts (request);     //  Professional Logging(TM)
        free (request);     //  We throw this away

        //Send main response
        zmq_send (routerSocket, zmq_msg_data(&replyAddr), zmq_msg_size(&replyAddr), ZMQ_MORE);
        zstr_send (routerSocket,
            "HTTP/1.0 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "\r\n"
            "Hello, World!");

        //Close TCP connection
        zmq_msg_send (&replyAddr, routerSocket, ZMQ_MORE);
        zmq_send (routerSocket, NULL, 0, 0);
    }
    logger.debug("HTTP Server terminating...");
    zsocket_destroy(ctx, controlRecvSocket);
    zsocket_destroy(ctx, routerSocket);
    routerSocket = nullptr;
}

void YakHTTPServer::terminate() {
    if(thread != nullptr) {
        //Send control msg
        zmq_send(controlSocket, NULL, 0, 0);
        zsocket_destroy(ctx, controlSocket);
        //Wait until the thread exists
        thread->join();
        delete thread;
        thread = nullptr;
    }
    logger.terminate();
}

YakHTTPServer::~YakHTTPServer() {
    terminate();
}