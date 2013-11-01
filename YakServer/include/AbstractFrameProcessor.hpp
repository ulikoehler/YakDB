/* 
 * File:   AbstractFrameProcessor.hpp
 * Author: uli
 *
 * Created on 11. Juli 2013, 03:16
 */

#ifndef ABSTRACTFRAMEPROCESSOR_HPP
#define	ABSTRACTFRAMEPROCESSOR_HPP
#include <czmq.h>
#include <string>
#include "Logger.hpp"
#include <leveldb/status.h>

/**
 * An abstract class that provides basic parsing
 * and checking methods
 * to subclasses
 */
class AbstractFrameProcessor {
public:
    AbstractFrameProcessor(void* ctx,
            int inputSocketType,
            int outputSocketType,
            const std::string& loggerName);
    ~AbstractFrameProcessor();
protected:
    void* context;
    void* processorInputSocket;
    void* processorOutputSocket;
    Logger logger;
    /**
     * Parse a table ID frame, as little endian 32 bit unsigned integer in one frame.
     * Automatically receives the frame from replyProxySocket.
     * Automatically checks if there is a frame available
     * Input is read from processorInputSocket,
     * output is read from processorOutputSocket.
     * @param tableIdDst Where the table ID shall be placed
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool parseUint32Frame(uint32_t& tableIdDst,
            const char* frameDesc,
            bool generateResponse,
            const char* errorResponseCode,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Parse a 64-bit little-endian unsigned integer in one frame.
     * Automatically receives the frame from workPullSocket.
     * Automatically checks if there is a frame available.
     * @param tableIdDst Where the table ID shall be placed
     * @param frameDesc A string describing the frame, for meaningful debug messages
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool parseUint64Frame(uint64_t& tableIdDst,
            const char* frameDesc,
            bool generateResponse,
            const char* errorResponseCode,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Equivalent to parseUint64Frame(), but assumes a given default
     * value if the frame is empty.
     */
    bool parseUint64FrameOrAssumeDefault(uint64_t& valDst,
            uint64_t defaultValue,
            const char* frameDesc,
            bool generateResponse,
            const char* errorResponseCode,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Equivalent to parseUint32Frame(), but assumes a given default
     * value if the frame is empty.
     */
    bool parseUint32FrameOrAssumeDefault(uint32_t& valDst,
            uint32_t defaultValue,
            const char* frameDesc,
            bool generateResponse,
            const char* errorResponseCode,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Ensure the work pull socket has a next message part in the current message
     * @param errString A descriptive error string logged and sent to the client
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool expectNextFrame(const char* errString,
            bool generateResponse,
            const char* errorResponseCode,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Ensure the given LevelDB status code indicates success
     * @param errString A descriptive error string logged and sent to the client. status error string will be appended.
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool checkLevelDBStatus(const leveldb::Status& status,
            const char* errString,
            bool generateResponse,
            const char* errorResponseCode,
            zmq_msg_t* headerFrame = nullptr);

    /**
     * Parse a start/end range from the next two 8-byte-or-empty-frames.
     * Reports errors to the if any occur.
     * @param startSlice A pointer to a string ptr where the start string (or "" for zero-length frames) is placed
     * @param endSlice A pointer to a string ptr where the end string (or "" for zero-length frames) is placed
     * @param errName A descriptive name of the range (e.g. "Scan request scan range") for error reporting
     * @param errorResponse A 4-character response code
     * @return false if the caller shall exit immediately because of errors.
     */
    bool parseRangeFrames(std::string& startSlice,
            std::string& endSlice,
            const char* errName,
            const char* errorResponse,
            bool generateResponse = true,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Receive a single frame from this.processorInputSocket
     * Automatically handles errors if neccessary
     * @param msg A pointer to a zmq_msg_t to store it it.
     * @param errName A descriptive name of the range (e.g. "Scan request scan range") for error reporting
     * @param errorResponse A 4-character response code
     * @param generateResponse Set this to true if an error response shall be sent back to the client in case of error
     * @return false in case of error, true else
     */
    bool receiveMsgHandleError(zmq_msg_t* msg,
            const char* errName,
            const char* errorResponse,
            bool generateResponse = true,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Same behaviour as receiveMsgHandleError(), but stores the frame in a string instead of a message.
     */
    bool receiveStringFrame(std::string& frame,
            const char* errName,
            const char* errorResponse,
            bool generateResponse = true,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Send a single message over this.processorOutputSocket
     * Automatically handles errors if neccessary.
     * 
     * To this function, t is not known whether this is the first frame or 
     * the error occured somewhere in the middle of the request,
     * so sending an error report to the client (with generateRespone == true)
     * could screw up the client code, but this is the only way we can
     * properly tell the client something went wrong.
     * 
     * @param msg A pointer to a zmq_msg_t to store it it.
     * @param flags The zmq_msg_send flags (e.g. ZMQ_SNDMORE)
     * @param errName A descriptive name of the range (e.g. "Scan request scan range") for error reporting
     * @param errorResponse A 4-character response code
     * @param generateResponse Set this to true if an error response shall be sent back to the client in case of error
     * @return false in case of error, true else
     */
    bool sendMsgHandleError(zmq_msg_t* msg,
            int flags,
            const char* errName,
            const char* errorResponse,
            bool generateResponse = true,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * This function checks if the given frame has a certain size.
     * If the frame sizes match, it returns true and exits.
     * 
     * Else, an error message is logged using the logger instance in the current class
     * and, if generateResponse is set to true, the error reponse plus the error msg
     * is sent over the output socket.
     * @param msg
     * @param expectedSize
     * @param errName
     * @param errorResponse
     * @param generateResponse
     * @return false in case of error, true else
     */
    bool expectExactFrameSize(zmq_msg_t* msg,
            size_t expectedSize,
            const char* errName,
            const char* errorResponse,
            bool generateResponse = true,
            zmq_msg_t* headerFrame = nullptr);
    /**
     * Send a uint64 frame over the processor output socet.
     * Log any error that might occur.
     * @return false if any error occured, true else
     */
    bool sendUint64Frame(uint64_t value, const char* frameDesc, int flags = 0);
    /**
     * Send a uint32 frame over the processor output socet.
     * Log any error that might occur.
     * @return false if any error occured, true else
     */
    bool sendUint32Frame(uint32_t value, const char* frameDesc, int flags = 0);
    /**
     * Send a message over processorOutputSocket.
     * Log any error that might occur.
     * @return false if any error occured, true else
     */
    bool sendMessage(zmq_msg_t* msg, const char* frameDesc, int flags = 0);
    /**
     * If the input socket has any remaining msg parts in the current message,
     * read and dispose them.
     * 
     * Never reads a message part if the RCVMORE flag is not set on the socket.
     * 
     * This method has a builtin error limit that prevents it from going
     * to infinite loop because of repeated errors.
     * 
     * Automatically logs errors if neccessary
     */
    void disposeRemainingMsgParts();
    /**
     * Send a response header frame. This function automatically handles request IDs.
     * The request ID from the request header is automatically copied to the response,
     * if any.
     * 
     * If the headerFrame parameter is NULL, no request ID is generated.
     */
    bool sendResponseHeader(zmq_msg_t* headerFrame,
        const char* responseHeader,
        int flags = 0,
        size_t responseSize = 4,
        size_t requestExpectedSize = 4);
};

#endif	/* ABSTRACTFRAMEPROCESSOR_HPP */

