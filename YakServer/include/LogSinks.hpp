/**
 * LogSinks.hpp 
 * 
 * Provides serveral standard logsink implementation
 */

#ifndef LOGSINKS_HPP
#define LOGSINKS_HPP
#include "Logger.hpp"
#include <string>
#include <fstream>

/**
 * A LogSink instance represents the final destination of a log message,
 * e.g. a rotating file log sink or a email log sink
 * 
 * The virtual function overhead seems reasonable here because logging is ZMQ-async.
 */
class LogSink {
public:
    virtual void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage) = 0;
    virtual ~LogSink();
};


/**
 * Convert a LogLevel to a string describing it,
 * e.g. LogLevel.Info --> "[Info]"
 */
std::string logLevelToString(LogLevel logLevel);

/**
 * A stderr logsink that uses ANSI terminal logging
 * if stderr is a TTY.
 */
class StderrLogSink : public LogSink {
public:
    StderrLogSink();
    void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage);
    /**
     * Enable or disable ANSI-colored logging
     */
    void setColoredLogging(bool value);
private:
    bool coloredLogging; //Set to true
};

/**
 * A log sink that logs to a log file.
 * TODO: Auto-rotate log files
 */
class FileLogSink : public LogSink {
public:
    FileLogSink(const std::string& filename);
    ~FileLogSink();
    void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage);
private:
    std::ofstream fout;
    std::string filename;
};

#endif //LOGSINKS_HPP