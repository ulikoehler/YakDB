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
#include <ctime>

static const char* const ESCAPE_BOLD = "\x1B[1m";
static const char* const ESCAPE_NORMALFONT = "\x1B[0m";
static const char* const ESCAPE_BLACK_FOREGROUND = "\x1B[30m";
static const char* const ESCAPE_RED_FOREGROUND = "\x1B[31m";
static const char* const ESCAPE_GREEN_FOREGROUND = "\x1B[32m";
static const char* const ESCAPE_YELLOW_FOREGROUND = "\x1B[33m";
static const char* const ESCAPE_BLUE_FOREGROUND = "\x1B[34m";
static const char* const ESCAPE_MAGENTA_FOREGROUND = "\x1B[35m";
static const char* const ESCAPE_CYAN_FOREGROUND = "\x1B[36m";
static const char* const ESCAPE_WHITE_FOREGROUND = "\x1B[37m";

inline static void HOT printDateTime(uint64_t timestamp, std::ostream& stream) {
    //Get microsecond precision time
    struct timeval tv;
    tv.tv_sec = timestamp / 1000;
    tv.tv_usec = (timestamp % 1000) * 1000;
    //Format the tm data
    char dateBuffer[32];
    size_t formattedLength = strftime(dateBuffer, 32, "%F %T", localtime(&(tv.tv_sec)));
    assert(formattedLength > 0);
    //Format the subsecond part
    snprintf(dateBuffer + formattedLength, 32 - formattedLength, ".%03lu", (unsigned long) (tv.tv_usec / 1000));
    stream << '[' << dateBuffer << ']';
}

inline static int checkLogHeaderFrame(zframe_t* headerFrame) {

}

/**
 * For a given log protocol header message, checks & returns if
 * the header frame contains a stop message
 */
inline static bool isStopServerMessage(zframe_t* headerFrame) {
    assert(headerFrame);
    assert(zframe_size(headerFrame) == 3);
    return zframe_data(headerFrame)[2] == '\xFF';
}

StderrLogSink::StderrLogSink() {

}

void HOT StderrLogSink::log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage) {

    switch (logLevel) {
        case LogLevel::Critical:
        {
            std::cerr << ESCAPE_BOLD << ESCAPE_RED_FOREGROUND;
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Error] " << senderName << " - " << logMessage << ESCAPE_NORMALFONT << ESCAPE_BLACK_FOREGROUND << std::endl;
            break;
        }
        case LogLevel::Error:
        {
            std::cerr << ESCAPE_RED_FOREGROUND;
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Error] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        case LogLevel::Warn:
        {
            std::cerr << ESCAPE_YELLOW_FOREGROUND;
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Warning] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        case LogLevel::Info:
        {
            std::cerr << ESCAPE_GREEN_FOREGROUND;
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Info] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        case LogLevel::Debug:
        {
            std::cerr << ESCAPE_BLUE_FOREGROUND;
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Debug] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        case LogLevel::Trace:
        {
            std::cerr << ESCAPE_CYAN_FOREGROUND;
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Trace] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        default:
        {
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Unknown] " << senderName << " - " << logMessage << std::endl;
            break;
        }
    }
}

LogServer::LogServer(zctx_t* ctx, LogLevel logLevel, const std::string& endpoint) : ctx(ctx), logLevel(logLevel), thread(nullptr) {
    internalSocket = zsocket_new(ctx, ZMQ_PULL);
    const char* subscription = "";
    if (unlikely(zsocket_bind(internalSocket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}
using namespace std;

LogServer::~LogServer() {
    fprintf(stderr, "GADGADGQE$QWREZAF\n");
    fflush(stderr);
    if (thread) {
        //Send the STOP message (single empty frame);
        zframe_t* frame = zframe_new("\x55\x01\xFF", 3);
        assert(!zframe_send(&frame, internalSocket, 0));
        thread->join(); //Wait until it exits
        delete thread;
    }
}

void HOT LogServer::start() {
    Logger logger(ctx, "Log server");
    while (true) {
        zmsg_t* msg = zmsg_recv(internalSocket);
        if (unlikely(!msg)) {
            //Interrupted (e.g. by SIGINT), but loggers might still want to log something
            // so we can't exit yet.
            fprintf(stderr, "-----------------Stopping log server--------------- %s\n", zmq_strerror(errno));
            fflush(stderr);
            continue;
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
                uint64_t timestamp = zclock_gettime();
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