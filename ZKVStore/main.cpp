
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

const char* updateWorkerThreadAddr = "inproc://updateWorkerThreads";
const char* readWorkerThreadAddr = "inproc://readWorkerThreads";

using namespace std;

/**
 * Encapsulates multiple key-value tables in one interface.
 * The tables are addressed by number and 
 */
class KeyValueMultiTable {
public:
    typedef uint32_t IndexType;

    KeyValueMultiTable(IndexType defaultTablespaceSize = 16) : databases(32) {
        //Initialize the table array with 16 tables.
        //This avoids early re-allocation
        databasesSize = 16;
        //Use malloc here to allow usage of realloc
        //Initialize all pointers to zero
        for (int i = 0; i < databases.size(); i++) {
            databases[i] = NULL;
        }
    }

    /**
     * Close all tables and stop the table open server.
     * 
     * This can't be done in the destructor because it needs to be done before
     * the context is deallocated
     */
    void cleanup() {
        fprintf(stderr, "Flushing and closing tables...\n");
        //Flush & delete all databases
        for (int i = 0; i < databasesSize; i++) {
            if (databases[i] != NULL) {
                delete databases[i];
            }
        }
    }

    leveldb::DB* getTable(IndexType index, TableOpenHelper& openHelper) {
        //Check if the database has already been opened
        if (databases[index] == NULL || index >= databases.size()) {
            openHelper.openTable(index);
        }
        return databases[index];
    }

    void closeTable(IndexType index) {
        if (databases[index] != NULL) {
            delete databases[index];
            databases[index] = NULL;
        }
    }

    leveldb::DB* getExistingTable(IndexType index) {
        return databases[index];
    }

    vector<leveldb::DB*>& getDatabases() {
        return databases;
    }
private:
    /**
     * The databases vector.
     * Any method in this class has read-only access (tables may be closed by this class however)
     * 
     * Write acess is serialized using the TableOpenHelper class.
     * 
     * Therefore locks and double initialization are avoided.
     */
    std::vector<leveldb::DB*> databases; //Indexed by table num
    uint32_t databasesSize;
};

/**
 * This struct is used to shared common variables across a server thread
 */
struct KVServer {

    KVServer(zctx_t* ctx) : ctx(ctx), tables(), numUpdateThreads(0) {

    }

    void initializeTableOpenHelper() {
        tableOpenHelper = new TableOpenHelper(ctx);
    }

    /**
     * Cleanup. This must be called before the ZMQ context is destroyed
     */
    void cleanup() {
        delete tableOpenHelper;
        tables.cleanup();
    }

    ~KVServer() {
        if (numUpdateThreads > 0) {
            delete[] updateWorkerThreads;
        }
        printf("Gracefully closed tables, exiting...\n");
    }
    KeyValueMultiTable tables;
    //External sockets
    void* externalRepSocket; //ROUTER socket that receives remote req/rep READ requests can only use this socket
    void* externalSubSocket; //SUB socket that subscribes to UPDATE requests (For mirroring etc)
    void* externalPullSocket; //PULL socket for UPDATE load balancinge
    void* responseProxySocket; //Worker threads connect to this PULL socket -- messages need to contain envelopes and area automatically proxied to the main router socket
    //Internal sockets
    void* updateWorkerThreadSocket; //PUSH socket that distributes update requests among update worker threads
    void* readWorkerThreadSocket; //PUSH socket that distributes update requests among read worker threads
    TableOpenHelper* tableOpenHelper; //Only for use in the main thread
    //Thread info
    uint16_t numUpdateThreads;
    uint16_t numReadThreads;
    std::thread** updateWorkerThreads;
    std::thread** readWorkerThreads;
    //Other stuff
    zctx_t* ctx;
};

void handleReadRequest(KeyValueMultiTable& tables, zmsg_t* msg, TableOpenHelper& openHelper) {
#ifdef DEBUG_READ
    printf("Starting to handle read request of size %d\n", (uint32_t) zmsg_size(msg) - 1);
    fflush(stdout);
#endif
    //Parse the table id
    zframe_t* tableIdFrame = zmsg_next(msg);
    assert(zframe_size(tableIdFrame) == sizeof (uint32_t));
    uint32_t tableId = *((uint32_t*) zframe_data(tableIdFrame));
    cout << "RM Resized to size " << zmsg_size(msg) << endl;
    //The response has doesn't have the table frame, so
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    string value; //Where the value will be placed
    leveldb::Status status;
    //Read each read request
    zframe_t* keyFrame = NULL;
    while ((keyFrame = zmsg_next(msg)) != NULL) {
        //Build a slice of the key (zero-copy)
        string keystr((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        leveldb::Slice key((char*) zframe_data(keyFrame), zframe_size(keyFrame));
#ifdef DEBUG_READ
        printf("Reading key %s from table %d\n", key.ToString().c_str(), tableId);
#endif
        status = db->Get(readOptions, key, &value);
        if (status.IsNotFound()) {
#ifdef DEBUG_READ
            cout << "Could not find value for key " << key.ToString() << "in table " << tableId << endl;
#endif
            //Empty value
            zframe_reset(keyFrame, "", 0);
        } else {
#ifdef DEBUG_READ
            cout << "Read " << value << " (key = " << key.ToString() << ") --> " << value << endl;
#endif
            zframe_reset(keyFrame, value.c_str(), value.length());
        }
    }
    //Now we can remove the table ID frame from the message (doing so before would confuse zmsg_next())
    zmsg_remove(msg, tableIdFrame);
    zframe_destroy(&tableIdFrame);
#ifdef DEBUG_READ
    cout << "Final reply msg size: " << zmsg_size(msg) << endl;
#endif
}

void handleUpdateRequest(KeyValueMultiTable& tables, zmsg_t* msg, TableOpenHelper& helper, bool synchronousWrite) {
    leveldb::Status status;
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = synchronousWrite;
    //Parse the table id
    //This function requires that the given message has the table id in its first frame
    zframe_t* tableIdFrame = zmsg_next(msg);
    assert(zframe_size(tableIdFrame) == sizeof (uint32_t));
    uint32_t tableId = *((uint32_t*) zframe_data(tableIdFrame));
    zmsg_remove(msg, tableIdFrame);
    //Get the table
    leveldb::DB* db = tables.getTable(tableId, helper);
    //The entire update is processed in one batch
#ifdef BATCH_UPDATES
    leveldb::WriteBatch batch;
    while (true) {
        //The next two frames contain 
        zframe_t* keyFrame = zmsg_next(msg);
        if (!keyFrame) {
            break;
        }
        zframe_t* valueFrame = zmsg_next(msg);
        assert(valueFrame); //if this fails there is an odd number of data frames --> illegal (see protocol spec)

        leveldb::Slice keySlice((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        leveldb::Slice valueSlice((char*) zframe_data(valueFrame), zframe_size(valueFrame));

#ifdef DEBUG_UPDATES
        printf("Insert '%s' = '%s'\n", keySlice.ToString().c_str(), valueSlice.ToString().c_str());
        fflush(stdout);
#endif

        batch.Put(keySlice, valueSlice);
    }
    //Commit the batch
#ifdef DEBUG_UPDATES
    printf("Commit update batch\n");
#endif
    db->Write(writeOptions, &batch);
#else //No batch updates
    for (int i = 0; i < request.write_requests_size(); i++) {
        const KeyValue& kv = request.write_requests(i);
        status = db->Put(writeOptions, kv.key(), kv.value());
    }
    for (int i = 0; i < request.delete_requests_size(); i++) {
        status = db->Delete(writeOptions, request.delete_requests(i));
    }
#endif
    //The memory occupied by the message is free'd in the thread loop
}

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

/**
 * The main function for the update worker thread.
 * 
 * This function parses the header, calls the appropriate handler function
 * and sends the response for PARTSYNC requests
 */
void updateWorkerThreadFunction(zctx_t* ctx, KVServer* serverInfo) {
    void* workPullSocket = zsocket_new(ctx, ZMQ_PULL);
    zsocket_connect(workPullSocket, updateWorkerThreadAddr);
    //Create the table open helper (creates a socket that sends table open requests)
    TableOpenHelper tableOpenHelper(ctx);
    //Create the data structure with all info for the poll handler
    while (true) {
        zmsg_t* msg = zmsg_recv(workPullSocket);
        assert(zmsg_size(msg) >= 1);
        //Parse the header
        //At this point it is unknown if
        // 1) the msg contains an envelope (--> received from the main ROUTER) or
        // 2) the msg does not contain an envelope (--> received from PULL, SUB etc.
        //As the frame after the header may not be empty under any circumstances for
        // request types processed by the update worker threads, we can distiguish these cases
        zframe_t* firstFrame = zmsg_first(msg);
        zframe_t* secondFrame = zmsg_next(msg);
        zframe_t* headerFrame;
        zframe_t* routingFrame = NULL; //Set to non-null if there is an envelope
        if (zframe_size(secondFrame) == 0) { //Msg contains envelope
            headerFrame = zmsg_next(msg);
            routingFrame = zmsg_unwrap(msg);
        } else {
            //We need to reset the current frame ptr
            // (because the handlers may call zmsg_next()),
            // so we can't just use firstFrame as header
            headerFrame = zmsg_first(msg);
        }
        assert(isHeaderFrame(headerFrame));
        //Get the request type
        RequestType requestType = getRequestType(headerFrame);
        //Process the flags
        uint8_t flags = getWriteFlags(headerFrame);
        bool partsync = isPartsync(flags); //= Send reply after written to backend
        bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
        //Process the rest of the frame
        if (requestType == PutRequest) {
            handleUpdateRequest(serverInfo->tables, msg, tableOpenHelper, fullsync);
        } else if (requestType == DeleteRequest) {
            cerr << "Delete request TBD - WIP!" << endl;
        } else {
            cerr << "Internal routing error: request type " << requestType << " routed to update worker thread!" << endl;
        }
        //Cleanup
        zmsg_destroy(&msg);
        //If partsync is disabled, the main thread already sent the response.
        //Else, we need to create & send the response now.
        //Routing 
        if (partsync) {
            assert(routingFrame); //If this fails, someone disobeyed the protocol specs and sent PARTSYNC over a non-REQ-REP-cominbation.
            //Send acknowledge message
            msg = zmsg_new();
            zmsg_wrap(msg, routingFrame);
            zmsg_addmem(msg, "\x31\x01\x20\x00", 4); //Send response code 0x00 (ack)
            zmsg_send(&msg, serverInfo->externalRepSocket);
        }
    }
    printf("Stopping update processor\n");
    zsocket_destroy(ctx, workPullSocket);
}

void initializeUpdateWorkers(zctx_t* ctx, KVServer* serverInfo) {
    //Initialize the push socket
    serverInfo->updateWorkerThreadSocket = zsocket_new(ctx, ZMQ_PUSH);
    zsocket_bind(serverInfo->updateWorkerThreadSocket, updateWorkerThreadAddr);
    //Start the threads
    const int numThreads = 3;
    serverInfo->numUpdateThreads = numThreads;
    serverInfo->updateWorkerThreads = new std::thread*[numThreads];
    for (int i = 0; i < numThreads; i++) {
        serverInfo->updateWorkerThreads[i] = new std::thread(updateWorkerThreadFunction, ctx, serverInfo);
    }
}


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
    bool dbCompressionEnabled = true;
    TableOpenServer tableOpenServer(ctx, server.tables.getDatabases(), dbCompressionEnabled);
    server.initializeTableOpenHelper();
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