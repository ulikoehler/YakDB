/* 
 * File:   TableOpenHelper.cpp
 * Author: uli
 * 
 * Created on 6. April 2013, 18:15
 */

#include "TableOpenHelper.hpp"
#include <thread>
#include <cassert>
#include <iostream>
#include <leveldb/db.h>
#include "zutil.hpp"

using namespace std;

static const char* tableOpenEndpoint = "inproc://tableOpenWorker";

/**
 * Main function for table open worker thread.
 * 
 * Msg format:
 *      - STOP THREAD msg: One empty frame
 *      - CREATE TABLE IF NOT OPENED YET msg: A 4-byte frame containing the binary ID
 */
static void tableOpenWorkerThread(zctx_t* context, std::vector<leveldb::DB*>& databases, bool dbCompressionEnabled) {
    assert(context);
    //assert(zctx_underlying(context));
    //cout << "OTR " << context->iothreads << endl;
    errno = 0;
    void* repSocket = zsocket_new(context, ZMQ_REP);
    debugZMQError("Create table opener reply socket", errno);
    assert(repSocket);
    errno = 0;
    zmq_bind(repSocket,  "inproc://tableOpenWorker");
    if(errno == ENOTSOCK) {
        cerr << "OH DUDE" << endl;
    }
    debugZMQError("Bind table opener reply socket", errno);
    while (true) {
        cout << "TOS Waiting for msg" << endl;
        zmsg_t* msg = zmsg_recv(repSocket);
        debugZMQError("Receive TableOpenServer message", errno);
        assert(msg);
        cout << "RECV" << endl;
        //Msg only contains one frame
        zframe_t* frame = zmsg_first(msg);
        size_t frameSize = zframe_size(frame);
        //Check for a STOP msg
        if (frameSize == 0) {
            cout << "RECV STOP" << endl;
            break;
        }
        //If it's not null, it must have the appropriate size
        assert(frameSize == sizeof (TableOpenHelper::IndexType));
        TableOpenHelper::IndexType index = *((TableOpenHelper::IndexType*)zframe_data(frame));
        //Resize if neccessary
        if (databases.size() <= index) {
            cout << "Resizing table" << endl;
            databases.reserve(index + 16); //Avoid large vectors
        }
        //Create the table only if it hasn't been created yet, else just ignore the request
        if (databases[index] == NULL) {
            cout << "Creating/opening " << index << endl;
            leveldb::Options options;
            options.create_if_missing = true;
            options.compression = (dbCompressionEnabled ? leveldb::kSnappyCompression : leveldb::kNoCompression);
            std::string tableName = "tables/" + std::to_string(index);
            leveldb::Status status = leveldb::DB::Open(options, tableName.c_str(), &databases[index]);
            if (!status.ok()) {
                fprintf(stderr, "Error while trying to open database in %s: %s", tableName.c_str(), status.ToString().c_str());
            }
        }
        //Send back 0 (acknowledge)
        zstr_send(repSocket, "\x00");
        debugZMQError("Send reply in TOS", errno);
        cout << "TOS action finished" << endl;
    }
    cout << "TOS ... Stopping TOS Thread" << endl;
    //Reply to the exit msg
    zstr_send(repSocket, "");
    //Stop msg received, cleanup
    zsocket_destroy(context, &repSocket);
}

TableOpenServer::TableOpenServer(zctx_t* context, std::vector<leveldb::DB*>& databases, bool dbCompressionEnabled) : context(context) {
    workerThread = new std::thread(tableOpenWorkerThread, context, std::ref(databases), dbCompressionEnabled);
}

TableOpenServer::~TableOpenServer() {
    //Create a temporary socket
    void* tempSocket = zsocket_new(context, ZMQ_REQ);
    zsocket_connect(tempSocket, tableOpenEndpoint);
    //Send an empty msg, don't wait for a reply
    zstr_send(tempSocket, ""); //--> single zero byte frame
    //Receive the (empty) reply
    char* reply = zstr_recv(tempSocket);
    //Cleanup
    free(reply);
    zsocket_destroy(context, &tempSocket);
    //The thread might take some time to exit, but we want to delete the thread ptr immediately
    //So, let it finish on his 
    workerThread->detach();
    //Delete the worker thread
    delete workerThread;
}

TableOpenHelper::TableOpenHelper(zctx_t* context) {
    id = rand();
    cout << "Create TOH " << id << endl;
    this->context = context;
    reqSocket = zsocket_new(context, ZMQ_REQ);
    debugZMQError("Create table opener request socket", errno);
    assert(reqSocket);
    int rc = zsocket_connect(reqSocket, tableOpenEndpoint);
    debugZMQError("Connect table opener request socket", errno);
    if(rc) {
        fprintf(stderr, "TOH cfail\n");
    }
}

void TableOpenHelper::openTable(TableOpenHelper::IndexType index) {
    cout << "TOH Send index: " << index << " " << id << endl;
    //Just send a message containing the table index to the opener thread
    zmsg_t* msg = zmsg_new();
    assert(msg);
    zframe_t* frame = zframe_new(&index, sizeof (TableOpenHelper::IndexType));
    assert(frame);
    assert(zframe_size(frame) == 4);
    zmsg_add(msg, frame);
    zmsg_send(&msg, reqSocket);
    debugZMQError("Sending TOH msg", errno);
    //Wait for the reply (it's empty but that does not matter)
    cout << "TOH: Finish send, waiting" << " " << id << endl;
    msg = zmsg_recv(reqSocket); //Blocks until reply received
    debugZMQError("Receive reply from table opener", errno);
    zmsg_destroy(&msg);
    cout << "TOH: Completed" << " " << id << endl;
}

TableOpenHelper::~TableOpenHelper() {
    cout << "Destruct TOH" << " " << id << endl;
    zsocket_destroy(context, &reqSocket);
}