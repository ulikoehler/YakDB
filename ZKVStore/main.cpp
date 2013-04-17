
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
#include "../protobuf/KVDB.pb.h"
#include "TableOpenHelper.hpp"
#include "protocol.hpp"

//Feature toggle
#define BATCH_UPDATES
#define DEBUG_UPDATES
#define DEBUG_READ

const char* updateWorkerThreadAddr = "inproc://updateWorkerThread";

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

    ~KeyValueMultiTable() {
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

    ~KVServer() {
        delete tableOpenHelper;
        if (numUpdateThreads > 0) {
            delete[] updateWorkerThreads;
        }
    }
    KeyValueMultiTable tables;
    //External sockets
    void* reqRepSocket; //ROUTER socket that receives remote req/rep. READ requests can only use this socket
    void* subSocket; //SUB socket that subscribes to UPDATE requests (For mirroring etc)
    void* pullSocket; //PULL socket for UPDATE load balancing
    //Internal sockets
    void* updateWorkerThreadSocket; //PUSH socket that distributes update requests among worker threads
    TableOpenHelper* tableOpenHelper; //Only for use in the main thread
    //Thread info
    uint16_t numUpdateThreads;
    std::thread** updateWorkerThreads;
    //Other stuff
    zctx_t* ctx;
};

void handleReadRequest(KeyValueMultiTable& tables, ReadRequest& request, ReadResponse& response, TableOpenHelper& openHelper) {
    cout << "Starting to get RR table" << endl;
    leveldb::DB* db = tables.getTable(request.tableid(), openHelper);
    cout << "Got RR table " << endl;
    //Create the response object
    leveldb::ReadOptions readOptions;
    string value; //Where the value will be placed
    leveldb::Status status;
    //Read each read request
    for (int i = 0; i < request.keys_size(); i++) {
        const std::string& key = request.keys(i);
#ifdef DEBUG_READ
        printf("Reading\n", key.c_str());
#endif
        status = db->Get(readOptions, key, &value);
        if (status.IsNotFound()) {
            response.add_values("");
        } else {
#ifdef DEBUG_READ
            cout << "Read " << value << " (key = " << key << ")" << endl;
#endif
            response.add_values(value);
        }
    }
}

void handleUpdateRequest(KeyValueMultiTable& tables, UpdateRequest& request, TableOpenHelper& helper) {
    leveldb::Status status;
    leveldb::WriteOptions writeOptions;
    leveldb::DB* db = tables.getTable(request.tableid(), helper);
    //The entire update is processed in one batch
#ifdef BATCH_UPDATES
    leveldb::WriteBatch batch;

    for (int i = 0; i < request.write_requests_size(); i++) {
        const KeyValue& kv = request.write_requests(i);
#ifdef DEBUG_UPDATES
        printf("Insert '%s' = '%s'\n", kv.key().c_str(), kv.value().c_str());
#endif
        fflush(stdout);
        batch.Put(kv.key(), kv.value());
    }
    for (int i = 0; i < request.delete_requests_size(); i++) {
#ifdef DEBUG_UPDATES
        printf("Delete '%s'\n", request.delete_requests(i).c_str());
#endif
        batch.Delete(request.delete_requests(i));
    }
    //Commit the batch
#ifdef DEBUG_UPDATES
    printf("Commit batch");
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
    zmsg_t *msg = zmsg_recv(server->reqRepSocket);
    if (msg) {
        //The message consists of four frames: Client addr, empty delimiter, msg type (1 byte) and data
        assert(zmsg_size(msg) >= 4); //return addr + empty delimiter + msg type [+ data frames]
        //Check the header -- send error message if invalid
        zframe_t *headerFrame;
        string errorString;
        
        checkProtocolVersion(zframe_data(headerFrame), zframe_size(headerFrame), errorString);
        
        //        fprintf(stderr, "Got message of type %d from client\n", (int) msgType);
        if (msgType == 1) {
            cout << "Received read request" << endl;
            ReadRequest request;
            request.ParseFromArray(data, dataSize);
            //Create the response
            ReadResponse readResponse;
            //Do the work
            cout << "Parsed read request (size " << request.keys_size() << ")" << endl;
            handleReadRequest(server->tables, request, readResponse, *(server->tableOpenHelper));
            string readResponseString = readResponse.SerializeAsString();
            cout << "Finished handling read request" << endl;
            //Re-use the msg type frame
            zframe_reset(msgTypeFrame, "\x00", 1);
            //Re-use the data frame. It has been removed from the msg before, so we need to re-add it
            zframe_reset(dataFrame, readResponseString.c_str(), readResponseString.length());
        } else if (msgType == 2) { //Process update requests
            //This forwards the data (=serialized update request) to a update worker thread
            zmsg_t *msgToUpdateWorker = zmsg_new();
            zframe_t* dataFrame = zframe_new(data, dataSize);
            zmsg_push(msgToUpdateWorker, dataFrame);
            zmsg_send(&msgToUpdateWorker, server->updateWorkerThreadSocket);
            //Send acknowledge message (update is processed asynchronously)
            zframe_reset(msgTypeFrame, "\x00", 1);
        } else {
            fprintf(stderr, "Unknown message type %d from client\n", (int) msgType);
        }
        cout << "Sending reply (raw)" << endl;
        zmsg_send(&msg, server->reqRepSocket);
    }
    return 0;
}

void initializeDirectoryStructure() {
    mkdir("tables", S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP);
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
 * The main function for the update worker thread
 */
void updateWorkerThreadFunction(zctx_t* ctx, KVServer* serverInfo) {
    void* workPullSocket = zsocket_new(ctx, ZMQ_PULL);
    zsocket_connect(workPullSocket, updateWorkerThreadAddr);
    //Create the table open helper (creates a socket that sends table open requests)
    TableOpenHelper tableOpenHelper(ctx);
    //Create the data structure with all info for the poll handler
    while (true) {
        zmsg_t* msg = zmsg_recv(workPullSocket);
        assert(msg);
        zframe_t* firstFrame = zmsg_first(msg);
        assert(firstFrame);
        //Deserialize the update request
        UpdateRequest req;
        req.ParseFromArray(zframe_data(firstFrame), zframe_size(firstFrame));
        //Process the frame
        handleUpdateRequest(serverInfo->tables, req, tableOpenHelper);
        //Cleanup
        zmsg_destroy(&msg);
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
    zctx_t *ctx = zctx_new();
    //Create the object that will be shared between the threadsloop
    KVServer server(ctx);
    //Start the table opener thread (constructor returns after thread has been started. Cleanup on scope exit.)
    bool dbCompressionEnabled = true;
    TableOpenServer tableOpenServer(ctx, server.tables.getDatabases(), dbCompressionEnabled);
    server.initializeTableOpenHelper();
    //Initialize all worker threads
    initializeUpdateWorkers(ctx, &server);
    //Initialize the sockets that run on the main thread
    server.reqRepSocket = zsocket_new(ctx, ZMQ_ROUTER);
    zsocket_bind(server.reqRepSocket, reqRepUrl);
    //Start the loop
    printf("Starting server...\n");
    zloop_t *reactor = zloop_new();
    zmq_pollitem_t poller = {server.reqRepSocket, 0, ZMQ_POLLIN};
    zloop_poller(reactor, &poller, handleRequestResponse, &server);
    zloop_start(reactor);
    //Cleanup (called when finished)
    zloop_destroy(&reactor);
    zctx_destroy(&ctx);
    //All tables are closed at scope exit.
}