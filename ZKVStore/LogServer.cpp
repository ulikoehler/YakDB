/* 
 * File:   LogServer.cpp
 * Author: uli
 * 
 * Created on 8. Mai 2013, 19:58
 */

#include "LogServer.hpp"
#include "macros.hpp"
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

inline static void printDateTime(std::ostream& stream) {
    //Get microsecond precision time
    struct timeval tv;
    gettimeofday(&tv, NULL);
    //Format the tm data
    char dateBuffer[32];
    size_t formattedLength = strftime(dateBuffer, 32, "%F %T", localtime(&(tv.tv_sec)));
    assert(formattedLength > 0);
    //Format the subsecond part
    snprintf(dateBuffer + formattedLength, 32 - formattedLength, ".%03lu", (unsigned long) (tv.tv_usec / 1000));
    stream << '[' << dateBuffer << ']';
}

StderrLogSink::StderrLogSink() {

}

void StderrLogSink::log(LogLevel logLevel, uint64_t timestamp, std::string senderName, std::string& logMessage) {

    switch (logLevel) {
        case LogLevel::Critical:
        {
            LOG_STREAM << ESCAPE_BOLD << ESCAPE_RED_FOREGROUND;
            printDateTime(LOG_STREAM);
            LOG_STREAM << "[Error] " << senderName << " - " << logMessage << ESCAPE_NORMALFONT << ESCAPE_BLACK_FOREGROUND << std::endl;
            break;
        }
        case LogLevel::Error:
        {
            LOG_STREAM << ESCAPE_RED_FOREGROUND;
            printDateTime(LOG_STREAM);
            LOG_STREAM << "[Error] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        case LogLevel::Warn:
        {
            LOG_STREAM << ESCAPE_YELLOW_FOREGROUND;
            printDateTime(LOG_STREAM);
            LOG_STREAM << "[Warning] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        case LogLevel::Info:
        {
            LOG_STREAM << ESCAPE_GREEN_FOREGROUND;
            printDateTime(LOG_STREAM);
            LOG_STREAM << "[Info] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        case LogLevel::Debug:
        {
            LOG_STREAM << ESCAPE_BLUE_FOREGROUND;
            printDateTime(LOG_STREAM);
            LOG_STREAM << "[Debug] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        case LogLevel::Trace:
        {
            LOG_STREAM << ESCAPE_CYAN_FOREGROUND;
            printDateTime(LOG_STREAM);
            LOG_STREAM << "[Trace] " << senderName << " - " << ESCAPE_BLACK_FOREGROUND << logMessage << std::endl;
            break;
        }
        default:
        {
            printDateTime(LOG_STREAM);
            LOG_STREAM << "[Unknown] " << senderName << " - " << logMessage << std::endl;
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
    if (thread) {
        //Send the STOP message (single empty frame);
        zframe_t* frame = zframe_new("", 0);
        assert(!zframe_send(&frame, internalSocket, 0));
        thread->join(); //Wait until it exits
        delete thread;
    }
    zsocket_destroy(ctx, internalSocket);
}

#define LOG_STREAM std::cerr

void LogServer::start() {
    while (true) {
        zmsg_t* msg = zmsg_recv(internalSocket);
        if (unlikely(!msg)) { //Terminated
            break;
        }
        zframe_t* logLevelFrame = zmsg_first(msg);
        assert(logLevelFrame);
        //Handle STOP messages
        if (zframe_size(logLevelFrame) == 0) {
            LOG_STREAM << "Logger exiting" << endl;
            break;
        }
        zframe_t* senderName = zmsg_next(msg);
        assert(senderName);
        zframe_t* logMessageFrame = zmsg_next(msg);
        assert(logMessageFrame);
        //Parse the frames
        LogLevel logLevel = *((LogLevel*) zframe_data(logLevelFrame));

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

void LogServer::addLogSink(LogSink* logSink) {
    logSinks.push_back(logSink);
}