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
    return ((char*)zframe_data(headerFrame))[2] == '\xFF';
}

LogServer::LogServer(void* ctxParam, LogLevel logLevel, bool autoStart, const std::string& endpointParam)
: logRequestInputSocket(zmq_socket_new_bind(ctxParam, ZMQ_PULL, endpointParam.c_str())),
logLevel(logLevel),
ctx(ctxParam),
thread(nullptr),
logger(ctx, "Log server"),
endpoint(endpointParam) {
    //Autostart if enabled
    if(autoStart) {
        startInNewThread();
    }
}

void COLD LogServer::terminate() {
    //Stop thread, if any
    if (thread) {
        //Final log message
        logger.info("Log server shutting down");
        //We need to create a temporary client socket
        void* tempSocket = zmq_socket_new_connect(ctx, ZMQ_PUSH, endpoint.c_str());
        //Send the STOP message to the worker thread
        if(zmq_send(tempSocket, "\x55\x01\xFF", 3, 0) == -1) {
            logMessageSendError("Log server thread stop message", logger);
        }
        thread->join(); //Wait until it exits
        //Cleanup
        zmq_close(tempSocket);
        delete thread;
        thread = nullptr;
    }
    /*
     * The log server is destructed in the late exit phase,
     * so we need to assume the context will be destroyed before
     * the logger in the current LogServer instance
     * has a chance to destroy its socket.
     * Therefore we terminate it manually to be sure.
     */
    logger.terminate();
}

LogServer::~LogServer() {
    terminate();
    //Cleanup the log sinks
    for (LogSink* sink : logSinks) {
        delete sink;
    }
}

static COLD void handleLogServerZMQError() {
    fprintf(stderr, "\x1B[31;1m[Error] Error while receiving log message in log server: %s \x1B[0;30m\n", zmq_strerror(errno));
    fflush(stderr);
}

void HOT LogServer::start() {
    //Create a socket to receive log requests
    while (true) {
        zmq_msg_t frame;
        zmq_msg_init(&frame);
        zmsg_t* msg = zmsg_recv(logRequestInputSocket);
        if (unlikely(!msg)) {
            //Interrupted (e.g. by SIGINT), but loggers might still want to log something
            // so we won't exit yet.
            if(yak_interrupted) {
                //Context was terminated (ctrl+c or other signal source), exit gracefully
                break;
            } else {
                handleLogServerZMQError
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
        //Check if we received a stop message
        if (unlikely(isStopServerMessage(headerFrame))) {
            //Log a message that the server is exiting, to all sinks
            //We can't use the logger here because the log message
            //won't be received any more
            if (logLevel >= LogLevel::Info) {
                //Log that we're stopping
                log( "Log server", LogLevel::Info, "Log server stopping");
            }
            //Cleanup
            zmsg_destroy(&msg);
            break;
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
    //Cleanup
    zmq_close(logRequestInputSocket);
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