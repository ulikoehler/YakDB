#include <leveldb/db.h>
#include <czmq.h>
#include <string>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include "../ZKVStore/client.h"

using namespace std;

/**
 * ZMQ zero-copy free function that uses standard C free
 */
void standardFree(void *data, void *hint) {
    free(data);
}

/**
 * ZDB KeyValueServer
 * Port 
 */
int main() {
    const char* reqRepUrl = "tcp://localhost:7100";
    //    const char* writeSubscriptionUrl = "tcp://*:7101";
    //    const char* errorPubUrl = "tcp://*:7102";
    printf("Starting client...\n");
    fflush(stdout);
    //Create the sockets
    zctx_t *ctx = zctx_new();
    void* reqRepSocket = zsocket_new(ctx, ZMQ_REQ);
    zsocket_connect(reqRepSocket, reqRepUrl);
    //Send the data
    zmsg_t* msg = buildSinglePutRequest(0, "testkey", "testvalue");
    zmsg_send(&msg, reqRepSocket);
    char* reply = zstr_recv(reqRepSocket);
    printf("Update reply (0 = acknowledge): %d", (int) reply[3]);
    free(reply);
    //
    //Read
    //
//    ReadRequest readRequest;
//    readRequest.add_keys("testkey");
//    msg = buildMessage(readRequest, 1); //1 = Read request
//    cout << "RR size " << zmsg_size(msg) << endl;
//    zmsg_send(&msg, reqRepSocket);
//    //Receive the reply
//    msg = zmsg_recv(reqRepSocket);
//    zframe_t* readResponseFrame = zmsg_last(msg);
//    ReadResponse response;
//    response.ParseFromString(string((const char*)zframe_data(readResponseFrame), zframe_size(readResponseFrame)));
//    cout << "RR " << response.values(0) << endl;
//    zmsg_destroy(&msg);
    zctx_destroy(&ctx);
    //All tables are closed at scope exit.
}