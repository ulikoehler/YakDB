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
#include <zmq.h>

/*
 * Utility macros for error handling code dedup and
 * to make the code shorter.
 * These shall only be used inside a LogServer msg loop.
 */
#define CHECK_MORE_FRAMES(msg, description)\
        if(unlikely(zmq_msg_more(&msg) != 1)) {\
            log("Log server", LogLevel::Warn, "Only received " + std::string(description) + ", missing further frames");\
            continue;\
        }
#define CHECK_NO_MORE_FRAMES(msg, description)\
        if(unlikely(zmq_msg_more(&msg))) {\
            log("Log server", LogLevel::Warn, "Expected no more frames after " + std::string(description) + ", but MORE flag is set");\
            continue;\
        }
#define RECEIVE_CHECK_ERROR(msg, socket, description)\
    if(unlikely(zmq_msg_recv(&msg, socket, 0) == -1)) {\
            if(yak_interrupted) {\
                break;\
            } else {\
                log("Log server", LogLevel::Warn, "Error while receiving "\
                    + std::string(description)+ ": " + std::string(zmq_strerror(errno)));\
                continue;\
            }\
    }\

/**
 * For a given log protocol header message, checks & returns if
 * the header frame contains a stop message
 */
inline static bool isStopServerMessage(char* headerData) {
    return headerData[2] == '\xFF';
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
        if(zmq_send_const(tempSocket, "\x55\x01\xFF", 3, 0) == -1) {
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
     * the logger member in the current LogServer instance
     * has a chance to destroy its socket.
     * Therefore we terminate it manually to be safe.
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
    zmq_msg_t frame;
    zmq_msg_init(&frame);
    while (true) {
        //TODO Dedup the size checking code!
        /*
         * Receive and parse header frame
         */
        RECEIVE_CHECK_ERROR(frame, logRequestInputSocket, "header frame");
        if(unlikely(zmq_msg_size(&frame) != 3)) {
            log("Log server",
                LogLevel::Warn,
                "Received log header frame of invalid size: expected size 3, got size "
                + std::to_string(zmq_msg_size(&frame)));
            continue;
        }
        //Check the magic byte & protocol version in the header frame,
        // log a warning if an illegal message has been received
        char* headerData = (char*) zmq_msg_data(&frame);
        if (unlikely(headerData[0] != '\x55')) {
            log("Log server",
                LogLevel::Warn,
                "Received log message header with illegal magic byte: " + std::to_string((uint8_t) headerData[0]));
            continue;
        }
        if (unlikely(headerData[1] != '\x01')) {
            log("Log server",
                LogLevel::Warn,
                "Received log message with illegal protocol version: " + std::to_string((uint8_t) headerData[1]));
            continue;
        }
        //Check if we received a stop message (same effect as SIGINT --> log server is 
        if (unlikely(isStopServerMessage(headerData))) {
            log("Log server",
                LogLevel::Debug,
                "Received stop message, exiting...");
            break;
        }
        //The STOP msg only has a single frame, so we can't check before checking for a STOP msg
        CHECK_MORE_FRAMES(frame, "header frame");
        /*
         * Receive log level frame
         */
        RECEIVE_CHECK_ERROR(frame, logRequestInputSocket, "log level frame");
        CHECK_MORE_FRAMES(frame, "log level frame");
        if(unlikely(sizeof(LogLevel) != zmq_msg_size(&frame))) {
            log("Log server",
                LogLevel::Warn,
                "Received log level frame of invalid size: expected size "
                + std::to_string(sizeof(LogLevel)) + ", got size "
                + std::to_string(zmq_msg_size(&frame)));
            continue;
        }
        LogLevel logLevel = extractBinary<LogLevel>(&frame);
        /*
         * Receive timestamp frame
         */
        RECEIVE_CHECK_ERROR(frame, logRequestInputSocket, "log level frame");
        CHECK_MORE_FRAMES(frame, "log level frame");
        if(unlikely(sizeof(uint64_t) != zmq_msg_size(&frame))) {
            log("Log server",
                LogLevel::Warn,
                "Received log level message of invalid size: expected size "
                + std::to_string(sizeof(uint64_t)) + ", got size "
                + std::to_string(zmq_msg_size(&frame)));
            continue;
        }
        uint64_t timestamp = extractBinary<uint64_t>(&frame);
        /*
         * Receive sender name frame
         */
        RECEIVE_CHECK_ERROR(frame, logRequestInputSocket, "sender name frame");
        CHECK_MORE_FRAMES(frame, "sender name frame");
        std::string senderName((char*) zmq_msg_data(&frame), zmq_msg_size(&frame));
        /*
         * Receive log message frame
         */
        RECEIVE_CHECK_ERROR(frame, logRequestInputSocket, "log message frame");
        CHECK_NO_MORE_FRAMES(frame, "log message frame");
        std::string logMessage((char*) zmq_msg_data(&frame), zmq_msg_size(&frame));
        //Pass the log message to the log sinks
        for (LogSink* sink : logSinks) {
            sink->log(logLevel, timestamp, senderName, logMessage);
        }
    }
    zmq_msg_close(&frame);
    //Log that we're stopping
    log("Log server", LogLevel::Info, "Log server stopping");
    //Cleanup
    zmq_close(logRequestInputSocket);
}

void LogServer::log(const std::string& loggerName, LogLevel msgLogLevel, const std::string& message) {
    if(msgLogLevel <= logLevel) {
        uint64_t timestamp = Logger::getCurrentLogTime();
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