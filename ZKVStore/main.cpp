#include <leveldb/db.h>
#include <czmq.h>
#include "../protobuf/KVDB.pb.h"

//A struct that contains info about the server thread

struct Server {
    void* reqRepSocket;
    void* writeSubscriptionUrl;
    void* errorPubSocket;
};

/*
 * Request/response codes are determined by the first byte
 * Request codes (from client):
 *      1<Read request>: Read
 *      2<Update request>: Update
 * Response codes (to client):
 *      0: Stop client
 *      1<ISBN>: Work request
 *      2: Acknowledge request
 *      3: Wait 1s and try again
 */
int handleRequestResponse(zloop_t *loop, zmq_pollitem_t *poller, void *arg) {
    Server* server = (Server*) arg;
    //Initialize a scoped lock
    zmsg_t *msg = zmsg_recv(server->serverSocket);
    if (msg) {
        //The message consists of three frames: Client addr, empty delimiter and data frame
        //Check which message has been sent by the client (in the data frame)
        zframe_t* dataFrame = zmsg_last(msg);
        assert(zframe_size(dataFrame) > 0);
        unsigned char* data = zframe_data(dataFrame);
        uint8_t msgType = data[0];
        std::string response;
        //        fprintf(stderr, "Got message of type %d from client\n", (int) msgType);
        if (msgType == 1) {
            fprintf(stderr, "Client requested new work chunk\n");
            //Find an unassigned ISBN number
            bool foundISBN = false;
            leveldb::Iterator* it = server->isbnQueueDB->NewIterator(leveldb::ReadOptions());
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                std::string currentISBN = it->key().ToString();
                //Check if the current ISBN is currently assigned to anyone
                if (server->currentlyAssigned.count(currentISBN) == 0) {
                    foundISBN = true;
                    server->currentlyAssigned.insert(currentISBN);
                    //Build the response
                    response = "\x01";
                    response += currentISBN;
                    break;
                }
                //If it's already assigned, just move on to the next ISBN in the database
            }
            delete it;
            //If there is no ISBN left, stop the client
            if (!foundISBN) {
                response = "\x03";
                fprintf(stderr, "Didn't found unassigned ISBN number, telling client to wait 1s (assigned ISBNs size = %ld)...\n", (size_t) server->currentlyAssigned.size());
            }
            //Send
            zframe_reset(zmsg_last(msg), response.c_str(), response.length());
        } else if (msgType == 2) {
            //Parse the book
            Book book;
            book.ParseFromArray(data + 1, zframe_size(dataFrame) - 1);
            std::string isbn = book.isbn();
            fprintf(stderr, "Client sent result for ISBN %s\n", isbn.c_str());
            //Write the book data
            leveldb::Status status = server->bookDB->Put(leveldb::WriteOptions(), isbn, book.SerializeAsString());
            assert(status.ok());
            //Delete it from the ISBN queue DB
            server->isbnQueueDB->Delete(leveldb::WriteOptions(), isbn);
            //Remove it from the currently assigned list
            server->currentlyAssigned.erase(isbn);
            //Add the related ISBNs to the queue
            size_t newRelatedISBNs = 0;
            for (int i = 0; i < book.relatedisbn_size(); i++) {
                std::string relatedISBN = book.relatedisbn(i);
                //Only add the related book to the queue
                std::string value; //Just a dummy
                status = server->bookDB->Get(leveldb::ReadOptions(), relatedISBN, &value);
                if (status.IsNotFound()) {
                    newRelatedISBNs++;
                    leveldb::WriteOptions writeOptions;
                    status = server->isbnQueueDB->Put(writeOptions, relatedISBN, "-");
                    assert(status.ok());
                }
            }
            //            fprintf(stderr, "\tISBN %s yielded %ld new ISBNs (related: %ld ISBNs)\n", isbn.c_str(), newRelatedISBNs, (size_t) book.relatedisbn_size());
            //Send acknowledge message
            zframe_reset(zmsg_last(msg), "\x02", 1);
        } else {
            fprintf(stderr, "Unknown message type %d from client\n", (int) msgType);
        }
        zmsg_send(&msg, server->serverSocket);
    }
    return 0;
}

/**
 * ZDB KeyValueServer
 * Port 
 */
int main() {
    const char* reqRepUrl = "tcp://*:7100";
    const char* writeSubscriptionUrl = "tcp://*:7101";
    const char* errorPubUrl = "tcp://*:7102";
    //Create the object that will be shared between the threads
    Server server;
    //Create the sockets
    zctx_t *ctx = zctx_new();
    server.reqRepSocket = zsocket_new(ctx, ZMQ_ROUTER);
    zsocket_bind(server.reqRepSocket, reqRepUrl);
    //Start the loop

    zctx_destroy(&ctx);
}