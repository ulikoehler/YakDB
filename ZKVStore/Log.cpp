/* 
 * File:   Log.cpp
 * Author: uli
 * 
 * Created on 24. April 2013, 04:29
 */

#include "Log.hpp"
#include "endpoints.hpp" //#defines the endpoint string
#include "zutil.hpp"
#include "macros.hpp"
#include <cstdio>
#include <functional>
#include <iostream>

LogSource::LogSource(zctx_t* ctx, const std::string& name, const std::string& endpoint) : ctx(ctx), loggerName(name) {
    socket = zsocket_new(ctx, ZMQ_PUB);
    if (unlikely(zsocket_connect(socket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}

LogSource::~LogSource() {
    zsocket_destroy(ctx, &socket);
}

void LogSource::log(const std::string& message, LogLevel level) {
    //Send the frames individually so no message needs to be allocated
    zframe_t* frame;
    frame = zframe_new(&level, sizeof (LogLevel));
    zframe_send(&frame, socket, ZFRAME_MORE);
    frame = zframe_new(loggerName.c_str(), loggerName.size());
    zframe_send(&frame, socket, ZFRAME_MORE);
    frame = zframe_new(message.c_str(), message.size());
    zframe_send(&frame, socket, 0);
}

void LogSource::error(const std::string& message) {
    log(message, LogLevel::Error);
}

void LogSource::warn(const std::string& message) {
    log(message, LogLevel::Warn);
}

void LogSource::info(const std::string& message) {
    log(message, LogLevel::Info);
}

void LogSource::debug(const std::string& message) {
    log(message, LogLevel::Debug);
}

void LogSource::trace(const std::string& message) {
    log(message, LogLevel::Trace);
}

LogServer::LogServer(zctx_t* ctx, LogLevel logLevel, const std::string& endpoint) : ctx(ctx), logLevel(logLevel), thread(nullptr) {
    internalSocket = zsocket_new(ctx, ZMQ_SUB);
    const char* subscription = "";
    zsocket_set_subscribe(internalSocket, (char*) subscription);
    if (unlikely(zsocket_bind(internalSocket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}
using namespace std;

LogServer::~LogServer() {
    if (thread) {
        //Send the STOP message (single empty frame);
        zframe_t* frame = zframe_new("", 0);
        assert(!zframe_send(&frame, internalSocket, 0));
        thread->join(); //Wait until it exits
        delete thread;
    }
    int linger = -1;
    zmq_setsockopt(internalSocket, ZMQ_LINGER, &linger, sizeof (int));
    cerr << "" << zmq_strerror(errno) << endl;
    zsocket_destroy(ctx, &internalSocket);
}

void LogServer::start() {
    while (true) {
        zmsg_t* msg = zmsg_recv(internalSocket);
        zframe_t* logLevelFrame = zmsg_first(msg);
        //Handle STOP messages
        if (zframe_size(logLevelFrame) == 0) {
            break;
        }
        zframe_t* senderName = zmsg_next(msg);
        zframe_t* logMessageFrame = zmsg_next(msg);
        //Parse the frames
        LogLevel logLevel = *((LogLevel*) zframe_data(logLevelFrame));
        //Get time info
        //        time_t rawtime;
        //        struct tm * timeinfo;
        //        time(&rawtime);
        //        timeinfo = localtime(&rawgtime);
        std::cout << "\x1B[31m" << frameToString(logMessageFrame) << std::endl;
        zmsg_destroy(&msg);
    }
}

/**
 * Starts a new thread that executes the start() function
 */
void LogServer::startInNewThread() {
    thread = new std::thread(std::mem_fun(&LogServer::start), this);
}

void COLD LogServer::setLogLevel(LogLevel logLevel) {
    this->logLevel = logLevel;
}

LogLevel COLD LogServer::getLogLevel() {
    return logLevel;
}