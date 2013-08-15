#include "HTTPServer.hpp"
#include <czmq.h>
#include "zutil.hpp"

YakHTTPServer::YakHTTPServer(zctx_t* ctxParam, const std::string& endpoint) : ctx(ctxParam), logger(ctx, "HTTP Server") {
    routerSocket = zsocket_new(ctx, ZMQ_ROUTER);
    zsocket_set_router_raw(routerSocket, 1);
    //TODO proper error handling
    assert(zsocket_bind(routerSocket, endpoint.c_str()) != -1);
}

void YakHTTPServer::terminate() {
    if(routerSocket != nullptr) {
        zsocket_destroy(ctx, routerSocket);
        routerSocket = nullptr;
    }
}

YakHTTPServer::~YakHTTPServer() {
    terminate();
}