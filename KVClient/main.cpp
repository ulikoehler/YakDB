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
    printf("Starting client...\n");
    fflush(stdout);
    srand(time(0));
    //Create the sockets
    zctx_t *ctx = zctx_new();
    void* reqRepSocket = zsocket_new(ctx, ZMQ_REQ);
    zsocket_connect(reqRepSocket, reqRepUrl);
    //Send a lot of data (10001 put requests in one message)
    PutRequest putRequest("testkey", "testvalue", 0);
    for(int i = 0; i < 10000; i++) {
        putRequest.addKeyValue(std::to_string(rand()), std::to_string(rand()));
    }
    printErr(putRequest.execute(reqRepSocket), "Write testdata");
    cout << "Finished writing - sending read request..." << endl;
    //
    //Read
    //
    cout << "Sending read request..." << endl;
    ReadRequest readRequest("testkey", 0);
    string readResult;
    printErr(readRequest.executeSingle(reqRepSocket, readResult), "Read testkey");
    //Receive the reply
    cout << "Got read result: " << readResult << endl;
    //
    //Count
    //
    CountRequest countRequest(0);
    uint64_t count;
    printErr(countRequest.execute(reqRepSocket, count), "Count values");
    cout << "Got count " << count << endl;
    zctx_destroy(&ctx);
    //All tables are closed at scope exit.
}