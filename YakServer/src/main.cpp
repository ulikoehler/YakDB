#include <leveldb/db.h>
#include <czmq.h>
#include <string>
#include <cstdio>
#include <iostream>
#include <sys/stat.h>
#include <signal.h>
#include "Server.hpp"
#include "ConfigParser.hpp"
#include "zutil.hpp"

//Feature toggle
#define BATCH_UPDATES
#define DEBUG_UPDATES
#define DEBUG_READ

using namespace std;

//For signal handlers
KeyValueServer* serverInstance;

void initializeDirectoryStructure() {
    mkdir("tables", S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP);
}

/**
 * ZDB KeyValueServer
 * Port 
 */
int main(int argc, char** argv) {
    //Ensure the tables directory exists
    initializeDirectoryStructure();
    //Process command line and config options
    ConfigParser configParser(argc, argv);
    //Create & start the server instance
    KeyValueServer server(configParser);
    serverInstance = &server;
    server.start(); //Blocks until interrupted or forced to exit
}
