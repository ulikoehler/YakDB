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

using namespace std;

#define TABLE_OPEN_ENDPOINT "inproc://tableOpenWorker"

/**
 * Main function for table open worker thread.
 * 
 * Msg format:
 *      - STOP THREAD msg: One empty frame
 *      - CREATE TABLE IF NOT OPENED YET msg: A 4-byte frame containing the binary ID
 */
static void tableOpenWorkerThread(zctx_t* context, std::vector<leveldb::DB*>& databases, bool dbCompressionEnabled) {
    cout << "OTR" << endl;
    void* repSocket = zsocket_new(context, ZMQ_REP);
    zsocket_bind(repSocket, TABLE_OPEN_ENDPOINT);
    while (true) {
        zmsg_t* msg = zmsg_recv(repSocket);
        cout << "RECV" << endl;
        //Msg only contains one frame
        zframe_t* frame = zmsg_first(msg);
        size_t frameSize = zframe_size(frame);
        //Check for a STOP msg
        if (frameSize == 0) {
            break;
        }
        //If it's not null, it must have the appropriate size
        assert(frameSize == sizeof (TableOpenHelper::IndexType));
        TableOpenHelper::IndexType index = *((TableOpenHelper::IndexType*)zframe_data(frame));
        //Resize if neccessary
        if (databases.size() <= index) {
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
        cout << "Finished  " << endl;
        //Send back the original msg (inproc --> --> no overhead)
        zmsg_send(&msg, repSocket);
    }
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
    zsocket_connect(tempSocket, TABLE_OPEN_ENDPOINT);
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
    cout << "Create TOH" << endl;
    this->context = context;
    reqSocket = zsocket_new(context, ZMQ_REQ);
    zsocket_connect(reqSocket, TABLE_OPEN_ENDPOINT);
}

void TableOpenHelper::openTable(TableOpenHelper::IndexType index) {
    cout << "TOH Send index: " << index << endl;
    //Just send a message containing the table index to the opener thread
    zmsg_t* msg = zmsg_new();
    zframe_t* frame = zframe_new(&index, sizeof (TableOpenHelper::IndexType));
    zmsg_add(msg, frame);
    zmsg_send(&msg, reqSocket);
    //Wait for the reply (it's empty but that does not matter)
    msg = zmsg_recv(reqSocket); //Blocks until reply received
    zmsg_destroy(&msg);
    cout << "ENDTOH" << endl;
}

TableOpenHelper::~TableOpenHelper() {
    cout << "Destruct TOH" << endl;
    zsocket_destroy(context, &reqSocket);
}
