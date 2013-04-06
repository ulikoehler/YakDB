#include <leveldb/db.h>
#include <czmq.h>
#include <string>
#include <cstdio>
#include <cstdlib>
#include "../protobuf/KVDB.pb.h"

using namespace std;

/**
 * Build a ZeroMQ ZKV message
 * @param readRequest A Protobuf message
 * @param requestValue
 * @return 
 */
template<typename PayloadType>
inline char* buildMessage(PayloadType& readRequest, uint8_t requestType) {
    int size = readRequest.ByteSize() + 2;
    char* data = new char[size];
    data[0] = 1; //Read request
    data[size - 1] = 0; //NUL-terminator
    readRequest.SerializeWithCachedSizesToArray(data+1);
    return data;
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
    //Build a request
    UpdateRequest request;
    KeyValue* val = request.add_write_requests();
    val->set_key("testkey");
    val->set_value("testvalue");
    //Send the frame
    zstr_send(reqRepSocket, (string("\x02") + request.SerializeAsString()).c_str());
    char *reply = zstr_recv(reqRepSocket);
    printf("Reply: %d", (int)reply[0]);
    zctx_destroy(&ctx);
    //All tables are closed at scope exit.
}