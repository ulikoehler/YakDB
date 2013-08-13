/* 
 * File:   LogServer.cpp
 * Author: uli
 * 
 * Created on 8. Mai 2013, 19:58
 */

#include "LogServer.hpp"
#include "macros.hpp"
#include "protocol.hpp"
#include "zutil.hpp"
#include <iostream>
#include <czmq.h>

/**
 * For a given log protocol header message, checks & returns if
 * the header frame contains a stop message
 */
inline static bool isStopServerMessage(zframe_t* headerFrame) {
    assert(headerFrame);
    assert(zframe_size(headerFrame) == 3);
    return zframe_data(headerFrame)[2] == '\xFF';
}

LogServer::LogServer(zctx_t* ctx, LogLevel logLevel, const std::string& endpoint)
: ctx(ctx),
logLevel(logLevel),
thread(nullptr),
logger(ctx, "Log server") {
    internalSocket = zsocket_new(ctx, ZMQ_PULL);
    const char* subscription = "";
    if (unlikely(zsocket_bind(internalSocket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}
using namespace std;

LogServer::~LogServer() {
    if(!zctx_interrupted) {
        logger.info("Log server shutting down");
    }
    //Stop thread, if any
    if (thread) {
        //Send the STOP message (single empty frame);
        zframe_t* frame = zframe_new("\x55\x01\xFF", 3);
        assert(!zframe_send(&frame, internalSocket, 0));
        thread->join(); //Wait until it exits
        delete thread;
    }
}

void HOT LogServer::start() {
    while (true) {
        zmsg_t* msg = zmsg_recv(internalSocket);
        if (unlikely(!msg)) {
            //Interrupted (e.g. by SIGINT), but loggers might still want to log something
            // so we can't exit yet.
            if(errno == ETERM && zctx_interrupted) {
                //Context was terminated (ctrl+c or other signal source), exit gracefully
                break;
            } else {
                fprintf(stderr, "\x1B[31;1m[Error] Error while receiving log message in log server: %s \x1B[0;30m\n", zmq_strerror(errno));
                fflush(stderr);
                continue;
            }
        }
        size_t msgSize = zmsg_size(msg);
        if (unlikely(msgSize != 1 && msgSize != 5)) {
            logger.warn("Received log message of illegal size: " + std::to_string(msgSize));
            zmsg_destroy(&msg);
            continue;
        }
        zframe_t* headerFrame = zmsg_first(msg);
        assert(headerFrame);
        if (unlikely(isStopServerMessage(headerFrame))) {
            //Log a message that the server is exiting, to all sinks
            //We can't use the logger here because the log message
            //won't be received any more
            if (logLevel >= LogLevel::Info) {
                //Get the current log time
                uint64_t timestamp = zclock_time();
                for (LogSink* sink : logSinks) {
                    sink->log(LogLevel::Info, timestamp, "Log server", "Log server stopping");
                }
            }
            zmsg_destroy(&msg);
            break;
        }
        //Check the magic byte & protocol version in the header frame,
        // log a warning if an illegal message has been received
        byte* headerData = zframe_data(headerFrame);
        if (unlikely(headerData[0] != '\x55')) {
            logger.warn("Received log message with illegal magic byte: " + std::to_string((uint8_t) headerData[0]));
            zmsg_destroy(&msg);
            continue;
        }
        if (unlikely(headerData[1] != '\x01')) {
            logger.warn("Received log message with illegal protocol version: " + std::to_string((uint8_t) headerData[1]));
            zmsg_destroy(&msg);
            continue;
        }
        //Parse the log information from the frames
        LogLevel logLevel = extractBinary<LogLevel>(zmsg_next(msg));
        uint64_t timestamp = extractBinary<uint64_t>(zmsg_next(msg));
        std::string senderName = frameToString(zmsg_next(msg));
        std::string logMessage = frameToString(zmsg_next(msg));
        //Pass the log message to the log sinks
        for (LogSink* sink : logSinks) {
            sink->log(logLevel, timestamp, senderName, logMessage);
        }
        //Cleanup
        zmsg_destroy(&msg);
    }
    //Cleanup the socket and the sinks
    zsocket_destroy(ctx, internalSocket);
    for (const LogSink* sink : logSinks) {
        delete sink;
    }
}

void LogServer::log(const std::string& loggerName, LogLevel msgLogLevel, const std::string& message) {
    if(msgLogLevel >= logLevel) {
        uint64_t timestamp = zclock_time();
        for (LogSink* sink : logSinks) {
            sink->log(logLevel, timestamp, loggerName, message);
        }
    }
}

/**
 * Starts a new thread that executes the start() function
 */
void LogServer::startInNewThread() {
    this->thread = new std::thread(std::mem_fun(&LogServer::start), this);
}

void COLD LogServer::setLogLevel(LogLevel logLevel) {
    this->logLevel = logLevel;
}

LogLevel COLD LogServer::getLogLevel() {
    return logLevel;
}

void LogServer::addLogSink(LogSink* logSink) {
    logSinks.push_back(logSink);
}