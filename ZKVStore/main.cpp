#include <leveldb/db.h>
#include <czmq.h>
#include <string>
#include <cstdio>
#include <iostream>
#include <sys/stat.h>
#include "Server.hpp"
#include "zutil.hpp"

//Feature toggle
#define BATCH_UPDATES
#define DEBUG_UPDATES
#define DEBUG_READ

const char* readWorkerThreadAddr = "inproc://readWorkerThreads";

using namespace std;

void initializeDirectoryStructure() {
    mkdir("tables", S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP);
}

/**
 * ZDB KeyValueServer
 * Port 
 */
int main() {
    //Ensure the tables directory exists
    initializeDirectoryStructure();
    //Create the ZMQ context
    //Create the object that will be shared between the threadsloop
    printf("Starting server...\n");
    KeyValueServer server;
    server.start(); //Blocks until interrupt
}
