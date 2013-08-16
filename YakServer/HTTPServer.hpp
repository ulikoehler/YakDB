#ifndef HTTPSERVER_HPP
#define HTTPSERVER_HPP
#include <czmq.h>
#include <thread>
#include "Logger.hpp"

/**
 * A minimalistic ZMQ-based HTTP server that
 * does not even attempt to be fully compatible
 * to any standard, but only supports the required
 * features over ZMQ raw sockets without
 * introducing additional dependencies.
 * 
 * It also provides static file support, but does not
 * attempt to be a fast multithreaded
 * 
 * For production multi-user environments, it's recommended
 * to reverse-proxy this server behind NGinx (with nginx setup to serve
 * the static files).
 */
class YakHTTPServer {
public:
    /**
     * Create a new HTTP server instance and start the worker thread
     */
    YakHTTPServer(zctx_t* ctx, const std::string& endpoint);
    void terminate();
    ~YakHTTPServer();
private:
    void workerMain();
    std::string endpoint;
    /**
     * This is used to send control messages to the HTTP server
     * (currently STOP command
     */
    void* controlSocket;
    zctx_t* ctx;
    std::thread* thread;
    Logger logger;
};

#endif //HTTPSERVER_HPP