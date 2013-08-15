#ifndef HTTPSERVER_HPP
#define HTTPSERVER_HPP
#include <czmq.h>
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
    YakHTTPServer(zctx_t* ctx, const std::string& endpoint);
    void terminate();
    ~YakHTTPServer();
private:
    void* routerSocket;
    zctx_t* ctx;
    Logger logger;
};

#endif //HTTPSERVER_HPP