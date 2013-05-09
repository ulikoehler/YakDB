#include <czmq.h>
#include <string>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include "client.hpp"
#include "dbclient.hpp"

using namespace std;

/**
 * ZDB KeyValue Client (test)
 * Port 
 */
int main() {
    srand(time(0));
    printf("Starting client...\n");
    DKVClient client;
//    client.connectRequestReply("tcp://localhost:7100");
    client.connectPushPull("tcp://localhost:7101");
    //Write some random data
    printErr(client.put(0, "testkey", "testvalue"), "Write testdata");
    for(int i = 0; i < 10000; i++) {
        printErr(client.put(0, std::to_string(rand()), std::to_string(rand())), "Write random testdata");
    }
    cout << "Finished writing - sending read request..." << endl;
    //
    //Read
    //
//    cout << "Sending read request..." << endl;
//    string readResult = client.read(0, "testkey");
//    //Receive the reply
//    cout << "Got read result: " << readResult << endl;
    //
    //Count
    //
    uint64_t count = client.count(0, "","");;
    cout << "Got count " << count << endl;
    //All tables are closed at scope exit.
}