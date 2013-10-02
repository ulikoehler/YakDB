#include "HTTPServer.hpp"
#include <czmq.h>
#include <iostream>
#include <sstream>
#include <limits>
#include <cstring>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
#include "yakclient/ReadRequests.hpp"
#include "yakclient/WriteRequests.hpp"
#include "http/URLParser.hpp"
#include "endpoints.hpp"
#include "BoyerMoore.hpp"
#include "zutil.hpp"

#define controlEndpoint "inproc://http/control"

using std::string;
using std::map;

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
    } else if(strcmp(".woff", fileExtensionPtr) == 0) {
        return "application/font-woff";
    } else {
        return "text/plain";
    }
}

void YakHTTPServer::serveStaticFile(const char* fileURL) {
    sendReplyIdentity();
    //Security: Check if the file contains relative paths
    if(strstr(fileURL, "..") != nullptr) {
        if(zmq_send_const(httpSocket, securityErrorMessage, sizeof(securityErrorMessage), 0) == -1) {
            logger.warn("Sending security violation error to HTTP client failed: " + string(zmq_strerror(errno)));
        }
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
        if(zmq_send_const(httpSocket, notFoundError, sizeof(notFoundError), 0) == -1) {
            logger.warn("Sending HTTP 404 to client failed: " + string(zmq_strerror(errno)));
        }
        return;
    }
    //File exists, mmap if neccessary
    if(mappedFiles.count(absoluteFilePath) == 0 || true) {
        logger.trace("mmap'ing static file " + absoluteFilePath);
        mappedFiles[absoluteFilePath] = new MMappedStaticFile(absoluteFilePath.c_str());
    }
    MMappedStaticFile* file = mappedFiles[absoluteFilePath];
    const char* mimeType = getMIMEType(fileURL);
    //Send the header
    string header("HTTP/1.1 200 OK\r\nContent-type: " + string(mimeType) 
                    + "\r\nContent-Length: " + std::to_string(file->size) +  "\r\n\r\n");
    if(zmq_send_const(httpSocket, header.data(), header.size(), 0) == -1) {
        logger.warn("Sending HTTP header to client failed: " + string(zmq_strerror(errno)));   
    }
    //Send data
    sendReplyIdentity();
    if(zmq_send_const(httpSocket, file->mem, file->size, 0) == -1) {
        logger.warn("Sending HTTP body to client failed: " + string(zmq_strerror(errno)));
    }
    //DEBUG CODE: Unmap immediately, to reload files
}

void YakHTTPServer::sendReplyIdentity() {
    if(unlikely(zmq_send (httpSocket, replyAddr, replyAddrSize, ZMQ_SNDMORE) == -1)) {
        logger.error("Error while sending stream reply adress: " + string(zmq_strerror(errno)));
    }
}

static bool startsWith(const string& corpus, const string& pattern) {
    return pattern.length() <= corpus.length() 
        && equal(pattern.begin(), pattern.end(), corpus.begin());
}

static const char *hexLUT = "0123456789ABCDEF";

std::string escapeJSON(const std::string& in) {
    string out;
    const char* data = in.data();
    for(size_t i = 0; i < in.size(); i++) {
        if(data[i] < 0x20) {
            //Ignore unicode, just serialize the hex value
            char temp[] = "\\u0000";
            temp[4] = hexLUT[(data[i] & 0xF0) >> 4];
            temp[5] = hexLUT[data[i] & 0x0F];
            out += string(temp, 6);
        } else if(data[i] == '\\') {
            out += "\\\\";
        } else if(data[i] == '\"') {
            out += "\\\"";
        } else {
            out += data[i];
        }
    }
    return out;
    //in.replace("\\", "\\\\");
    //in.replace("\"", "\\\"");
}

void YakHTTPServer::serveAPI(char* requestPathCstr) {
    /*
     * NOTE: All URLs are relative to /api/v1 !
    */
    map<string, string> queryArgs;
    //Overwritable defaults
    queryArgs["table"] = "1";
    queryArgs["limit"] = "10";
    //Parse query arguments, if any
    char* queryBegin = strchr(requestPathCstr, '?');
    if(queryBegin != nullptr) {
        //--> There's a query part
        *queryBegin = '\x0'; //query part doesn't belong to the URL itself
        parseQueryPart(queryBegin + 1, queryArgs);
    }
    string requestPath(requestPathCstr);
    if(startsWith(requestPath, "/scan")) {
        /*
         * Scan keys, with optional value size limiting
         * Query arguments:
         *   startKey       -- The start key, inclusive
         *   endKey         -- The stop key, not inclusive
         *   limit          -- The numeric limit, default 10
         *   table          -- The table no, default 1
         *   keyFilter      -- Optional key substring filter
         *   valueFilter    -- Optional value substring filter
         *   valueSizeLimit -- Option
         */
        //Set default arguments
        //cout << "prefix=" << queryArgs["prefix"] << endl;
        //cout << "prefix=" << queryArgs["prefix"] << endl;
        int rc = ScanRequest::sendRequest(mainRouterSocket,
                                 std::stol(queryArgs["table"]),
                                 std::stol(queryArgs["limit"]),
                                 queryArgs["startKey"],
                                 queryArgs["endKey"],
                                 queryArgs["keyFilter"],
                                 queryArgs["valueFilter"]
                     );
        size_t valueSizeLimit = std::numeric_limits<size_t>::max();
        if(queryArgs.count("valueSizeLimit") > 0) {
            valueSizeLimit = std::stol(queryArgs["valueSizeLimit"]);
        }
        if(rc == -1) {
            //TODO handle error
        }
        std::string errorMessage;
        rc = ScanRequest::receiveResponseHeader(mainRouterSocket, errorMessage);
        if(rc == -1) {
            //TODO handle error
        }
        //No error, send HTTP header
        sendReplyIdentity();
        string header("HTTP/1.1 200 OK\r\nContent-type: text/json\r\n\r\n{");
        if(zmq_send_const(httpSocket, header.data(), header.size(), 0) == -1) {
            logger.warn("Sending HTTP header to client failed: " + string(zmq_strerror(errno)));   
        }
        bool firstObject = true; //Used to determine whether to write a comma separator
        string key, value;
        while(true) {
            rc = ScanRequest::receiveResponseValue(mainRouterSocket, key, value);
            if(rc == -1) {
                //TODO handle error
                break;
            }
            //Limit the value size
            if(value.size() > valueSizeLimit) {
                value = value.substr(0, valueSizeLimit);
            }
            //Escape (almost) everything according to RFC4627 (ignore unicode)
            key = escapeJSON(key);
            value = escapeJSON(value);
            string jsonObj = ",\"" + key + "\":\"" + value + "\"";
            if(firstObject) {
                //Remove the comma
                jsonObj = jsonObj.substr(1);
            }
            firstObject = false;
            //Send the JSON data
            sendReplyIdentity();
            zmq_send(httpSocket, jsonObj.data(), jsonObj.size(), 0);
            //Stop if there are no more frames to be processed
            if(rc == 0) {
                break;
            }
        }
        if(rc == -1) {
            //TODO handle error
        }
        sendReplyIdentity();
        zmq_send(httpSocket, "}", 1, 0);
    } else if(startsWith(requestPath, "/delete/")) {
        /*
         * Single key delete
         * Path arguments:
         *   /delete/<key>
         * Query arguments:
         *  table -- Table number to delete in, default 1
         */
        //Extract the key to delete
        string key = decodeURLEntities(requestPath.substr(8));
        //Send the delete request
        int rc = DeleteRequest::sendHeader(mainRouterSocket, std::stol(queryArgs["table"]));
        if(rc == -1) {
            //TODO handle error
        }
        //Send the one and only key
        rc = DeleteRequest::sendKey(mainRouterSocket, key, true);
        if(rc == -1) {
            //TODO handle error
        }
        //Receive the response
        string errorMessage;
        rc = DeleteRequest::receiveResponse(mainRouterSocket, errorMessage);
        if(rc == -1) {
            //TODO handle error
        }
        sendReplyIdentity();
        if(rc == 1) {
            //Server error, but not a communication error
            string reply = "{\"status\":\"error\",\"error\":\"" + errorMessage + "\"}";
            zmq_send(httpSocket, reply.data(), reply.size(), 0);
        } else {
            //No error
            string reply = "{\"status\":\"ok\"}";
            zmq_send(httpSocket, reply.data(), reply.size(), 0);
        }
    } else if(startsWith(requestPath, "/put")) {
        /*
         * Single key put
         * Path arguments:
         *   /delete/<key>
         * Query arguments:
         *  table -- Table number to delete in, default 1
         */
        //Extract the key to delete
        string key = queryArgs["key"];
        string value = queryArgs["value"];
        //Send the delete request
        int rc = PutRequest::sendHeader(mainRouterSocket, std::stol(queryArgs["table"]));
        if(rc == -1) {
            //TODO handle error
        }
        //Send the one and only key
        rc = PutRequest::sendKeyValue(mainRouterSocket, key, value, true);
        if(rc == -1) {
            //TODO handle error
        }
        //Receive the response
        string errorMessage;
        rc = PutRequest::receiveResponse(mainRouterSocket, errorMessage);
        if(rc == -1) {
            //TODO handle error
        }
        sendReplyIdentity();
        if(rc == 1) {
            //Server error, but not a communication error
            string reply = "{\"status\":\"error\",\"error\":\"" + errorMessage + "\"}";
            zmq_send(httpSocket, reply.data(), reply.size(), 0);
        } else {
            //No error
            string reply = "{\"status\":\"ok\"}";
            zmq_send(httpSocket, reply.data(), reply.size(), 0);
        }
    }
}

void YakHTTPServer::workerMain() {
    //TODO proper error handling
    logger.trace("HTTP Server starting on " + endpoint);
    //Initialize HTTP socket
    httpSocket = zsocket_new(ctx, ZMQ_STREAM);
    assert(zsocket_bind(httpSocket, endpoint.c_str()) != -1);
    //Connect to main Yak router
    mainRouterSocket = zsocket_new_connect(ctx, ZMQ_REQ, mainRouterAddr);
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
            //Valid control msgs: STOP, HUP
            char* controlMsg = zstr_recv(controlRecvSocket);
            if(strcmp(controlMsg, "HUP") == 0) {
                //HUP: munmap all static files --> 'reload' static files
                logger.trace("HUP received, reloading all mmap'ed files");
                for(auto pair : mappedFiles) {
                    delete pair.second;
                }
                free(controlMsg);
                continue;
            } else if(strcmp(controlMsg, "STOP") == 0) {
                free(controlMsg);
                break;
            } else {
                logger.warn("Received unknown control message: '" + string(controlMsg) + "'");
                continue;
            }
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
        char* requestPathEnd = strchr(requestPath, ' ');
        size_t requestPathLength = requestPathEnd - requestPath;
        string requestPathString(requestPath, requestPathLength);
        *requestPathEnd = '\0';
        /**
         * NOTE: Even if the reply adress is non-const data,
         * it is deallocated by the IO thread when the connection is closed
         * and certainly won't change, so we can treat it as constant
         * and therefore zero-copy data while the HTTP connection is open
         */
        replyAddr = (char*) zmq_msg_data(&replyAddrFrame);
        replyAddrSize = zmq_msg_size(&replyAddrFrame);
        //Check whether a static file or an API route is requested
        if(strncmp(requestPath, "/api/v1", 7) == 0) {
            serveAPI(requestPath+7);
        } else {
            serveStaticFile(requestPath);
        }
        //Cleanup
        closeTCPConnection();
        zmq_msg_close(&replyAddrFrame);
    }
    logger.debug("HTTP Server terminating...");
    zsocket_destroy(ctx, controlRecvSocket);
    zsocket_destroy(ctx, httpSocket);
    zsocket_destroy(ctx, mainRouterSocket);
    httpSocket = nullptr;
}

void YakHTTPServer::closeTCPConnection() {
    sendReplyIdentity();
    sendEmptyFrameMessage(httpSocket);
}

void YakHTTPServer::terminate() {
    if(thread != nullptr) {
        //Send control msg
        zstr_send(controlSocket, "STOP");
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