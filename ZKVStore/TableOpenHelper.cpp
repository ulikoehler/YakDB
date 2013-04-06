/* 
 * File:   TableOpenHelper.cpp
 * Author: uli
 * 
 * Created on 6. April 2013, 18:15
 */

#include "TableOpenHelper.hpp"
#include <thread>

/**
 * Main function for table open worker thread
 * @param pointerToDatabases A pointer to an array of leveldb::DB* instances that is managed by this method.
 * The array is automatically initialized and resized on demand
 */
static void tableOpenWorkerThread(zctx_t* context, TableOpenHelper::LevelDBArray* pointerToDatabases) {
    void* repSocket = zsocket_new(context, ZMQ_REQ);
    zsocket_connect(repSocket, "inproc://tableOpenWorker");
    while(true) {
        zmsg_t* msg = zmsg_recv(repSocket);
        //Msg only contains one frame
        zframe_t* frame = zmsg_first(msg);
    }
}

TableOpenHelper::TableOpenHelper(zctx_t* context, LevelDBArray* databases) {
    //Initialize the table open helper
    
    //Initialize the REQ socket
    reqSocket = zsocket_new(context, ZMQ_REQ);
    zsocket_bind(reqSocket, "inproc://tableOpenWorker");
}