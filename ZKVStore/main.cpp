
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

void handleUpdateRequest(KeyValueMultiTable& tables, zmsg_t* msg, TableOpenHelper& helper) {
    leveldb::Status status;
    leveldb::WriteOptions writeOptions;
    //Parse the table id
    //This function requires that the given message has the table id in its first frame
    zframe_t* tableIdFrame = zmsg_first(msg);
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
    zmsg_t *msg = zmsg_recv(server->reqRepSocket);
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
            //The handler function rewrites the message
            cout << "RRRRRRR " << zmsg_size(msg) << endl;
        } else if (requestType == RequestType::PutRequest) {
            //We reuse the header frame and the routing information to send the acknowledge message
            //The rest of the msg (the table id + data) is directly forwarded to one of the handler threads
            //Remove the routing information
            zframe_t* routingFrame = zmsg_unwrap(msg);
            //Remove the header frame
            headerFrame = zmsg_pop(msg);
            zframe_destroy(&headerFrame);
            //Send the message to the update worker (--> processed async)
            zmsg_send(&msg, server->updateWorkerThreadSocket);
            //Send acknowledge message
            msg = zmsg_new();
            zmsg_wrap(msg, routingFrame);
            zmsg_addmem(msg, "\x31\x01\x20\x00", 4); //Send response code 0x00 (ack)
        } else {
            fprintf(stderr, "Unknown message type %d from client\n", (int) requestType);
        }
        cout << "Sending reply to" << (requestType == RequestType::PutRequest ? " put request " : " read request ") << endl;
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
        assert(zmsg_size(msg) >= 1);
        //Process the frame
        handleUpdateRequest(serverInfo->tables, msg, tableOpenHelper);
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