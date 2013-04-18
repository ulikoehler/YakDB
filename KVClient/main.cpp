#include <leveldb/db.h>
#include <czmq.h>
#include <string>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include "../protobuf/KVDB.pb.h"
#include "../ZKVStore/client.h"

using namespace std;

/**
 * ZMQ zero-copy free function that uses standard C free
 */
void standardFree(void *data, void *hint) {
    free(data);
}

/**
 * Build a ZeroMQ ZKV message
 * @param readRequest A Protobuf message
 * @param requestValue
 * @return 
 */
template<typename PayloadType>
inline zmsg_t* buildMessage(PayloadType& requestPayload, uint8_t requestType) {
    zmsg_t* msg = zmsg_new();
    //Create & add the msg type frame
    zframe_t* msgTypeFrame = zframe_new((const char*) &requestType, 1);
    zmsg_add(msg, msgTypeFrame);
    //Create and add the data frame (zero-copy)
    int size = requestPayload.ByteSize();
    uint8_t* data = (uint8_t*)malloc(sizeof (uint8_t)* size); //Protobuf uses uint8_t instead of char
    requestPayload.SerializeWithCachedSizesToArray(data);
    zframe_t* payloadFrame = zframe_new_zero_copy((char*)data, size, standardFree, nullptr);
    zmsg_add(msg, payloadFrame);
    //Create the zero copy frame
    return msg;
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
    printf("Reply: %d", (int) reply[0]);
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