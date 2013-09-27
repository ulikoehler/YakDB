#include "HTTPServer.hpp"
#include <czmq.h>
#include <iostream>
#include <sstream>
#include <cstring>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
#include "zutil.hpp"

#define controlEndpoint "inproc://http/control"

static size_t getFilesize(const char* filename) {
    struct stat st;
    stat(filename, &st);
    return st.st_size;   
}

static bool fileExists(const char* file) {
    struct stat buf;
    return (stat(file, &buf) == 0);
}

/**
 * Represents a readonly static file instance which has been mmapped
 * into vmem and possible pre-cached by the kernel.
 */
class MMappedStaticFile {
public:
    /**
     * mmap a new static file.
     */
    MMappedStaticFile(const char* filename) {
        this->size = getFilesize(filename);
        fd = open(filename, O_RDONLY, 0);
        mem = mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        assert(mem != nullptr);
        //File will always be sent sequentially
        madvise(mem, size, MADV_SEQUENTIAL);
    }
    ~MMappedStaticFile() {
        assert(munmap(mem, size) == 0);
        close(fd);
    }
    //The mmapped memory
    void* mem;
    size_t size;
private:
    //The file descriptor that is mmapped
    int fd;
};

using namespace std;

YakHTTPServer::YakHTTPServer(zctx_t* ctxParam, const std::string& endpointParam, const std::string& staticFileRoot) : endpoint(endpointParam), ctx(ctxParam), thread(nullptr), logger(ctx, "HTTP Server"), staticFileRoot(staticFileRoot) {
    controlSocket = zsocket_new_bind(ctx, ZMQ_PAIR, controlEndpoint);
    //Start thread
    thread = new std::thread(std::mem_fun(&YakHTTPServer::workerMain), this);
}

//Static HTTP error msgs
static const char securityErrorMessage[] = "HTTP/1.1 403 Forbidden\r\nContent-type: text/plain\r\n\r\nSecurity error: Path must not contain ..";
static const char notFoundError[] = "HTTP/1.1 404 Not Found\r\nContent-type: text/plain\r\n\r\nFile not found";


/**
 * @return A constant string representing the MIME type, e.g. "text/plain")
 */
static const char* getMIMEType(const char* buffer) {
    //Find the beginning of the file ext === the last '.' occurrence -> strRchr
    const char* fileExtensionPtr = strrchr(buffer, '.');
    if(fileExtensionPtr == nullptr) {
        return "text/plain";
    }

    //We found an extension
    if(strcmp(".html", fileExtensionPtr) == 0) {
        return "text/html";
    } else if(strcmp(".js", fileExtensionPtr) == 0) {
        return "text/javascript";
    } else if(strcmp(".css", fileExtensionPtr) == 0) {
        return "text/css";
    } else if(strcmp(".jpg", fileExtensionPtr) == 0
        || strcmp(".jpeg", fileExtensionPtr) == 0) {
        return "image/jpeg";
    } else if(strcmp(".png", fileExtensionPtr) == 0) {
        return "image/png";
    } else if(strcmp(".ico", fileExtensionPtr) == 0) {
        return "image/x-icon";
    } else {
        return "text/plain";
    }
}

void YakHTTPServer::serveStaticFile(const char* fileURL) {
    sendReplyIdentity();
    //Security: Check if the file contains relative paths
    if(strstr(fileURL, "..") != nullptr) {
        zmq_send_const(httpSocket, securityErrorMessage, sizeof(securityErrorMessage), 0);
        return;
    }
    //No security violation, check if the file exists
    if(fileURL[0] == '/') {
        fileURL++;
    }
    if(fileURL[0] == '\0') {
        fileURL = "index.html";
    }
    string absoluteFilePath = staticFileRoot + string(fileURL);
    if(!fileExists(absoluteFilePath.c_str())) {
        zmq_send_const(httpSocket, notFoundError, sizeof(notFoundError), 0);
        return;
    }
    //File exists, mmap if neccessary
    if(mappedFiles.count(absoluteFilePath) == 0) {
        mappedFiles[absoluteFilePath] = new MMappedStaticFile(absoluteFilePath.c_str());
    }
    MMappedStaticFile* file = mappedFiles[absoluteFilePath];
    const char* mimeType = getMIMEType(fileURL);
    //Send the header
    string header("HTTP/1.1 200 OK\r\nContent-type: " + string(mimeType) 
                    + "\r\nContent-Length: " + std::to_string(file->size) +  "\r\n\r\n");
    zmq_send_const(httpSocket, header.data(), header.size(), 0);
    //Send data
    sendReplyIdentity();
    zmq_send_const(httpSocket, file->mem, file->size, 0);
}

void YakHTTPServer::sendReplyIdentity() {
    if(unlikely(zmq_send_const (httpSocket, replyAddr, replyAddrSize, ZMQ_SNDMORE) == -1)) {
        logger.error("Error while sending stream reply adress: " + string(zmq_strerror(errno)));
    }
}

void YakHTTPServer::workerMain() {
    //TODO proper error handling
    logger.trace("HTTP Server starting");
    //Initialize router socket
    httpSocket = zsocket_new(ctx, ZMQ_STREAM);
    assert(zsocket_bind(httpSocket, endpoint.c_str()) != -1);
    //Initialize other stuff
    zmq_msg_t replyAddrFrame;
    zmq_msg_init(&replyAddrFrame);
    zmq_msg_t request;
    zmq_msg_init(&request);
    zmq_msg_t response;
    zmq_msg_init(&response);
    //Initialize control socket (receives STOP cmd etc.)
    void* controlRecvSocket = zsocket_new_connect(ctx, ZMQ_PAIR, controlEndpoint);
    zmq_pollitem_t items[2];
    items[0].socket = httpSocket;
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
        //Receive the reply adress
        zmq_msg_init(&replyAddrFrame);
        if(unlikely(zmq_msg_recv(&replyAddrFrame, httpSocket, 0) == -1)) {
            logger.error("HTTP critical error: Reply address could not be received correctly: " + string(zmq_strerror(errno)));
            continue;
        }
        //Receive the request itself (might be multi-part but we're ATM not interested in all the header
        if(unlikely(zmq_msg_recv(&request, httpSocket, 0) == -1)) {
            logger.error("Error while receiving HTTP request: " + std::string(zmq_strerror(errno)));
            continue;
        }
        //Extract the request type from the request
        char* requestData = (char*)zmq_msg_data(&request);
        bool isGETRequest = (memcmp(requestData, "GET ", 4) == 0);
        assert(isGETRequest);
        //Ensure there are no frames left to receive!
        recvAndIgnore(httpSocket);
        //Make a NUL-delimited string from the request path
        char* requestPath = strchr(requestData, ' ') + 1;
        *(strchr(requestPath, ' ')) = '\0';
        /**
         * NOTE: Even if the reply adress is non-const data,
         * it is deallocated by the IO thread when the connection is closed
         * and certainly won't change, so we can treat it as constant
         * and therefore zero-copy data while the HTTP connection is open
         */
        replyAddr = (char*) zmq_msg_data(&replyAddrFrame);
        replyAddrSize = zmq_msg_size(&replyAddrFrame);
        //TODO Check routes
        serveStaticFile(requestPath);
        //Cleanup
        closeTCPConnection();
        zmq_msg_close(&replyAddrFrame);
    }
    logger.debug("HTTP Server terminating...");
    zsocket_destroy(ctx, controlRecvSocket);
    zsocket_destroy(ctx, httpSocket);
    httpSocket = nullptr;
}

void YakHTTPServer::closeTCPConnection() {
    sendReplyIdentity();
    sendEmptyFrameMessage(httpSocket);
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
    //munmap all mmapped files
    for(auto pair : mappedFiles) {
        delete pair.second;
    }
}