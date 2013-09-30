#ifndef HTTPSERVER_HPP
#define HTTPSERVER_HPP
#include <czmq.h>
#include <thread>
#include <unordered_map>
#include "Logger.hpp"

class MMappedStaticFile;

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
    YakHTTPServer(zctx_t* ctx, const std::string& endpoint, const std::string& staticFileRoot);
    void terminate();
    ~YakHTTPServer();
private:
    void workerMain();
    /**
     * Serve a static, mmapped file.
     * mmaps the file if neccessary
     */
    void serveStaticFile(const char* filename);
    
    void serveAPI(char* requestPath);
    /**
     * Close the current TCP connection, identified by
     * this->replyAddr
     */
    void closeTCPConnection();
    
    /**
     * Send this->replyAddr over this->httpSocket.
     */
    void sendReplyIdentity();
    std::string endpoint;
    /**
     * This is used to send control messages to the HTTP server
     * (currently STOP command
     */
    void* controlSocket;
    void* httpSocket; //Socket to the outer world, used by the worker thread
    void* mainRouterSocket; //Socket to the main router (inproc)
    zctx_t* ctx;
    std::thread* thread;
    Logger logger;
    const char* replyAddr;
    size_t replyAddrSize;
    std::string staticFileRoot;
    /**
     * Static files mmapped into memory
     */
    std::unordered_map<std::string, MMappedStaticFile*> mappedFiles;
};

#endif //HTTPSERVER_HPP