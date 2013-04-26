#include <czmq.h>
#include <string>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include "client.h"

using namespace std;

/**
 * ZDB KeyValue Client (test)
 * Port 
 */
int main() {
    const char* reqRepUrl = "tcp://localhost:7100";
    //    const char* writeSubscriptionUrl = "tcp://*:7101";
    //    const char* errorPubUrl = "tcp://*:7102";
    printf("Starting client...\n");
    fflush(stdout);
    srand(time(0));
    //Create the sockets
    zctx_t *ctx = zctx_new();
    void* reqRepSocket = zsocket_new(ctx, ZMQ_REQ);
    zsocket_connect(reqRepSocket, reqRepUrl);
    //Send a lot of data (10001 put requests in one message)
    zmsg_t* msg = buildSinglePutRequest(0, "testkey", "testvalue");
    for(int i = 0; i < 10000; i++) {
        addKeyValueToPutRequest(msg, std::to_string(rand()).c_str(), std::to_string(rand()).c_str());
    }
    zmsg_send(&msg, reqRepSocket);
    msg = zmsg_recv(reqRepSocket);
    zframe_t* replyHeader = zmsg_first(msg);
    printf("Update reply (0 = acknowledge): %d\n", (int) zframe_data(replyHeader)[0]);
    zmsg_destroy(&msg);
    //
    //Read
    //
    printf("Sending read request...\n");
    fflush(stdout);
    msg = buildSingleReadRequest(0, "testkey");
    zmsg_send(&msg, reqRepSocket);
    //Receive the reply
    msg = zmsg_recv(reqRepSocket);
    vector<string> result;
    parseReadRequestResult(msg, result);
    cout << "Got read result: " << result[0] << endl;
    fflush(stdout);
    //
    //Count
    //
    CountRequest request(0);
    cout << "Got count " << request.execute(reqRepSocket) << endl;
    //Cleanup
    zmsg_destroy(&msg);
    zctx_destroy(&ctx);
    //All tables are closed at scope exit.
}