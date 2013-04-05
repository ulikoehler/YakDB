#include <leveldb/db.h>
#include <czmq.h>
#include <string>
#include <cstdio>
#include <cstdlib>
#include "../protobuf/KVDB.pb.h"

using std::string;


/**
 * Encapsulates multiple key-value tables in one interface.
 * The tables are addressed by number and 
 */
class KeyValueMultiTable {
public:
    typedef uint32_t IndexType;

    KeyValueMultiTable(IndexType defaultTablespaceSize = 16, bool dbCompression = true) : dbCompression(dbCompression) {
        //Initialize the table array with 16 tables.
        //This avoids early re-allocation
        databasesSize = 16;
        //Use malloc here to allow usage of realloc
        databases = (leveldb::DB**) malloc(sizeof (leveldb::DB*) * databasesSize);
        //Initialize all pointers to zero
        memset(databases, 0, sizeof (leveldb::DB*) * databasesSize);
    }

    ~KeyValueMultiTable() {
        fprintf(stderr, "Flushing and closing tables...");
        //Flush & delete all databases
        for (int i = 0; i < databasesSize; i++) {
            if (databases[i] != NULL) {
                delete databases[i];
            }
        }
        //Delete the database array
        free(databases);
    }

    void resizeTablespace(IndexType minimumValidIndex) {
        assert(minimumValidIndex > databasesSize);
        IndexType newDatabaseSize = (minimumValidIndex + 1) * 2; //Avoid frequent re-allocations
        databases = (leveldb::DB**) realloc(databases, sizeof (leveldb::DB*) * newDatabaseSize);
        //Fill the newly-allocated memory with zeroes (indicates that DB has not been opened)
        for (IndexType i = databasesSize; i < newDatabaseSize; i++) {
            databases[i] = NULL;
        }
        //Flush the changes
        databasesSize = newDatabaseSize;
    }

    leveldb::DB* getTable(IndexType index) {
        //Check if the tablespace is large enough
        if (index >= databasesSize) {
            resizeTablespace(index);
        }
        //Check if the database has already been opened
        if (databases[index] == NULL) {
            leveldb::Options options;
            options.create_if_missing = true;
            options.compression = (dbCompression ? leveldb::kSnappyCompression : leveldb::kNoCompression);
            std::string tableName = "tables/" + std::to_string(index);
            leveldb::Status status = leveldb::DB::Open(options, tableName.c_str(), &databases[index]);
            if (!status.ok()) {
                fprintf(stderr, "Error while trying to open database in %s: %s", tableName.c_str(), status.ToString().c_str());
            }
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
private:
    leveldb::DB** databases; //Array indexed by table num
    uint32_t databasesSize;
    bool dbCompression;
};

/**
 * This struct is used to shared common variables across a server thread
 */
struct KVServer {

    KVServer() : tables() {

    }
    KeyValueMultiTable tables;
    void* reqRepSocket;
};

static void handleReadRequest(KeyValueMultiTable& tables, ReadRequest& request, ReadResponse& response) {
    leveldb::DB* db = tables.getTable(request.tableid());
    //Create the response object
    leveldb::ReadOptions readOptions;
    string value; //Where the value will be placed
    leveldb::Status status;
    //Read each read request
    for (int i = 0; i < request.keys_size(); i++) {
        status = db->Get(readOptions, request.keys(i), &value);
        if (status.IsNotFound()) {
            response.add_values("");
        } else {
            response.add_values(value);
        }
    }
}

static void handleUpdateRequest(KeyValueMultiTable& tables, UpdateRequest& request) {
    leveldb::Status status;
    leveldb::WriteOptions writeOptions;
    leveldb::DB* db = tables.getTable(request.tableid());
    for (int i = 0; i < request.write_requests_size(); i++) {
        const KeyValue& kv = request.write_requests(i);
        status = db->Put(writeOptions, kv.key(), kv.value());
    }
    for (int i = 0; i < request.delete_requests_size(); i++) {
        status = db->Delete(writeOptions, request.delete_requests(i));
    }
}

/*
 * Request/response codes are determined by the first byte
 * Request codes (from client):
 *      1<Read request>: Read. Response: Serialized ReadResponse
 *      2<Update request>: Update
 * Response codes (to client):
 *      0: Acknowledge
 */
int handleRequestResponse(zloop_t *loop, zmq_pollitem_t *poller, void *arg) {
    KVServer* server = (KVServer*) arg;
    //Initialize a scoped lock
    zmsg_t *msg = zmsg_recv(server->reqRepSocket);
    if (msg) {
        //The message consists of three frames: Client addr, empty delimiter and data frame
        //Check which message has been sent by the client (in the data frame)
        zframe_t* dataFrame = zmsg_last(msg);
        size_t dataSize = zframe_size(dataFrame);
        assert(zframe_size(dataFrame) > 0);
        unsigned char* data = zframe_data(dataFrame);
        uint8_t msgType = data[0];
        data++; //Now data is a pointer
        dataSize--; //msg type is the first character
        std::string response;
        //        fprintf(stderr, "Got message of type %d from client\n", (int) msgType);
        if (msgType == 1) {
            fprintf(stderr, "Client requested new work chunk\n");

            ReadRequest request;
            request.ParseFromArray(data, dataSize);
            uint32_t tableId = request.tableid();
            //Create the response
            ReadResponse readResponse;
            //Do the work
            handleReadRequest(server->tables, request, readResponse);
            response = readResponse.SerializeAsString();
            zframe_reset(zmsg_last(msg), "\x02", 1);
        } else if (msgType == 2) {
            UpdateRequest request;
            request.ParseFromArray(data, dataSize);
            //Send acknowledge message
            zframe_reset(zmsg_last(msg), "\x02", 1);
        } else {
            fprintf(stderr, "Unknown message type %d from client\n", (int) msgType);
        }
        zmsg_send(&msg, server->reqRepSocket);
    }
    return 0;
}

/**
 * ZDB KeyValueServer
 * Port 
 */
int main() {
    const char* reqRepUrl = "tcp://*:7100";
//    const char* writeSubscriptionUrl = "tcp://*:7101";
//    const char* errorPubUrl = "tcp://*:7102";
    //Create the object that will be shared between the threadsloop
    KVServer server;
    //Create the sockets
    zctx_t *ctx = zctx_new();
    server.reqRepSocket = zsocket_new(ctx, ZMQ_ROUTER);
    zsocket_bind(server.reqRepSocket, reqRepUrl);
    //Start the loop
    zloop_t *reactor = zloop_new();
    zmq_pollitem_t poller = {server.reqRepSocket, 0, ZMQ_POLLIN};
    zloop_poller(reactor, &poller, handleRequestResponse, &server);
    zloop_start(reactor);
    //Cleanup (called when finished)
    zloop_destroy(&reactor);
    zctx_destroy(&ctx);
    //All tables are closed at scope exit.
}