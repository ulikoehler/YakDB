#include "HTTPServer.hpp"
#include <czmq.h>
#include <iostream>
#include <sstream>
#include "zutil.hpp"

#define controlEndpoint "inproc://http/control"

using namespace std;

YakHTTPServer::YakHTTPServer(zctx_t* ctxParam, const std::string& endpointParam) : endpoint(endpointParam), ctx(ctxParam), thread(nullptr), logger(ctx, "HTTP Server") {
    controlSocket = zsocket_new_bind(ctx, ZMQ_PAIR, controlEndpoint);
    //Start thread
    thread = new std::thread(std::mem_fun(&YakHTTPServer::workerMain), this);
}

void YakHTTPServer::workerMain() {
    //TODO proper error handling
    logger.trace("HTTP Server starting");
    //Initialize router socket
    void* routerSocket = zsocket_new(ctx, ZMQ_STREAM);
    assert(zsocket_bind(routerSocket, endpoint.c_str()) != -1);
    //Initialize other stuff
    zmq_msg_t replyAddr;
    zmq_msg_init(&replyAddr);
    //Initialize control socket (receives STOP cmd etc.)
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
        assert(items[0].revents);
        assert(zmq_msg_recv(&replyAddr, routerSocket, 0) != -1);
        char *request = zstr_recv (routerSocket);
        cout << "R: " << request;
        free(request);
        assert(zsocket_rcvmore(routerSocket));
        //Receive the request
        /*ostringstream requestBuffer;
        while(zsocket_rcvmore(routerSocket)) {
            //cout << "RP " << endl;
            //requestBuffer << std::string(request);
        }
        cout << requestBuffer.str() << endl;*/
        //Send main response
        assert(zmq_send (routerSocket, zmq_msg_data(&replyAddr), zmq_msg_size(&replyAddr), ZMQ_SNDMORE) != -1);
        assert(zstr_send (routerSocket,
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "\r\n"
            "Hello, World!") != -1);

        //Close TCP connection
        assert(zmq_msg_send (&replyAddr, routerSocket, ZMQ_SNDMORE) != -1);
        sendEmptyFrameMessage(routerSocket);
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