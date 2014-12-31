/*
 * File:   AbstractFrameProcessor.hpp
 * Author: uli
 *
 * Created on 11. Juli 2013, 03:16
 */

#ifndef ABSTRACTFRAMEPROCESSOR_HPP
#define	ABSTRACTFRAMEPROCESSOR_HPP
#include <zmq.h>
#include <string>
#include <map>
#include "Logger.hpp"
#include <rocksdb/status.h>

/**
 * An abstract class that provides basic parsing and checking methods
 * to subclasses. It represent a black-box entity that reads frames, processes them
 * and crafts answers
 */
class AbstractFrameProcessor {
public:
    /**
     * Initialize a new AbstractFrameProcessor instance
     * using two different sockets for input and output.
     */
    AbstractFrameProcessor(void* ctx,
            int inputSocketType,
            int outputSocketType,
            const std::string& loggerName);
    /**
     * Initialize a new AbstractFrameProcessor instance
     * using a single socket for input and output.
     */
    AbstractFrameProcessor(void* ctx,
            int socketType,
            const std::string& loggerName);
    ~AbstractFrameProcessor();
protected:
    void* context;
    void* processorInputSocket;
    void* processorOutputSocket;
    Logger logger;
    /**
     * Parse a table ID frame, as little endian 32 bit unsigned integer in one frame.
     * Automatically receives the frame from processorInputSocket.
     * Automatically checks if there is a frame available
     * Input is read from processorInputSocket,
     * output is read from processorOutputSocket.
     * @param tableIdDst Where the table ID shall be placed
     * @param generateResponse Set this to true if an error message shall be sent on error
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool parseUint32Frame(uint32_t& dst,
            const char* frameDesc,
            bool generateResponse);
    /**
     * Parse a 64-bit little-endian unsigned integer in one frame.
     * Automatically receives the frame from processorInputSocket.
     * Automatically checks if there is a frame available.
     * @param tableIdDst Where the table ID shall be placed
     * @param frameDesc A string describing the frame, for meaningful debug messages
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool parseUint64Frame(uint64_t& dst,
            const char* frameDesc,
            bool generateResponse);
    /**
     * Parse a 8 bit unsigned integer in frame.
     * Automatically checks if there is a frame available
     * Input is read from processorInputSocket,
     * output is read from processorOutputSocket.
     * @param tableIdDst Where the table ID shall be placed
     * @param generateResponse Set this to true if an error message shall be sent on error
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool parseUint8Frame(uint8_t& tableIdDst,
            const char* frameDesc,
            bool generateResponse);
    /**
     * Parse a binary frame of known length
     * Automatically checks if there is a frame available
     * Input is read from processorInputSocket,
     * output is read from processorOutputSocket.
     * @param dst Pointer where the result is placed
     * @param size The size of binary data to receives
     * @param generateResponse Set this to true if an error message shall be sent on error
     * @param defaultValue If this is NOT nullptr and the received frame is zero-sized,
     *          the value pointed to by this pointer is copied to dst (size bytes must be available)
     * @param acceptFirstFrame If this is set to true, the first frame in a message will be accepted
     * @return True on success, false if an error has been encountered
     */
    bool parseBinaryFrame(void* dst,
            size_t size,
            const char* frameDesc,
            bool generateResponse,
            bool acceptFirstFrame = false,
            void* defaultValue = nullptr);
    /**
     * Equivalent to parseUint64Frame(), but assumes a given default
     * value if the frame is empty.
     */
    bool parseUint64FrameOrAssumeDefault(uint64_t& valDst,
            uint64_t defaultValue,
            const char* frameDesc,
            bool generateResponse);
    /**
     * Equivalent to parseUint32Frame(), but assumes a given default
     * value if the frame is empty.
     */
    bool parseUint32FrameOrAssumeDefault(uint32_t& valDst,
            uint32_t defaultValue,
            const char* frameDesc,
            bool generateResponse);
    /**
     * Ensure the work pull socket has a next message part in the current message
     * @param errString A descriptive error string logged and sent to the client
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool expectNextFrame(const char* errString,
            bool generateResponse);
    /**
     * Ensure the given LevelDB status code indicates success
     * @param errString A descriptive error string logged and sent to the client. status error string will be appended.
     * @param generateResponse Set this to true if an error message shall be sent on error
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool checkLevelDBStatus(const rocksdb::Status& status,
            const char* errString,
            bool generateResponse);

    /**
     * Parse a start/end range from the next two 8-byte-or-empty-frames.
     * Reports errors to the if any occur.
     * @param startSlice A pointer to a string ptr where the start string (or "" for zero-length frames) is placed
     * @param endSlice A pointer to a string ptr where the end string (or "" for zero-length frames) is placed
     * @param errName A descriptive name of the range (e.g. "Scan request scan range") for error reporting
     * @return false if the caller shall exit immediately because of errors.
     */
    bool parseRangeFrames(std::string& startSlice,
            std::string& endSlice,
            const char* errName,
            bool generateResponse = true);
    /**
     * Receive a single frame from this.processorInputSocket
     * Automatically handles errors if neccessary
     * @param msg A pointer to a zmq_msg_t to store it it.
     * @param errName A descriptive name of the range (e.g. "Scan request scan range") for error reporting
     * @param generateResponse Set this to true if an error response shall be sent back to the client in case of error
     * @return false in case of error, true else
     */
    bool receiveMsgHandleError(zmq_msg_t* msg, const char* errName, bool generateResponse);
    /**
     * Same behaviour as receiveMsgHandleError(), but stores the frame in a string instead of a message.
     */
    bool receiveStringFrame(std::string& frame, const char* errName, bool generateResponse);
    /**
     * Receive a map of alternating key/value frames, until the message ends.
     * Does not accept the first frame in a message -- it assumes RCVMORE is set on the socket
     */
    bool receiveMap(std::map<std::string, std::string>& target, const char* errName, bool generateResponse = false);
    /**
     * Send a map as a list of alternating key/value frames
     * Does not accept the first frame in a message -- it assumes RCVMORE is set on the socket.
     *
     * The values inside the map are destroyed for efficiency reasons.
     *
     * Empty maps can't be handled correctly by this function unless more is set to true.
     * If more is set to false (default), the application need to handle empty maps differently.
     *
     * @param more If set to false, the last message is sent with ZMQ_SNDMORE unset.
     */
    bool sendMap(std::map<std::string, std::string>& values, const char* errName, bool generateResponse = false, bool more = false);
    /**
     * Send a single message over this.processorOutputSocket
     * Automatically handles errors if neccessary.
     *
     * To this function, t is not known whether this is the first frame or
     * the error occured somewhere in the middle of the request,
     * so sending an error report to the client (with generateResponse == true)
     * could screw up the client code, but this is the only way we can
     * properly tell the client something went wrong.
     *
     * @param msg A pointer to a zmq_msg_t to store it it.
     * @param flags The zmq_msg_send flags (e.g. ZMQ_SNDMORE)
     * @param errName A descriptive name of the range (e.g. "Scan request scan range") for error reporting
     * @param generateResponse Set this to true if an error response shall be sent back to the client in case of error
     * @return false in case of error, true else
     */
    bool sendMsgHandleError(zmq_msg_t* msg,
            int flags,
            const char* errName,
            bool generateResponse = true);

    /**
     * Same as sendMsgHandleError(), but sends a std::string
     */
    bool sendMsgHandleError(const std::string& msg,
           int flags,
           const char* errName,
           bool generateResponse = true);
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
     * @param generateResponse
     * @return false in case of error, true else
     */
    bool expectExactFrameSize(zmq_msg_t* msg,
            size_t expectedSize,
            const char* errName,
            bool generateResponse);
    /**
     * This function checks if the given frame has a certain size or more
     * If the frame sizes match, it returns true and exits.
     *
     * Else, an error message is logged using the logger instance in the current class
     * and, if generateResponse is set to true, the error reponse plus the error msg
     * is sent over the output socket.
     * @param msg
     * @param expectedSize
     * @param errName
     * @param generateResponse
     * @return false in case of error, true else
     */
    bool expectMinimumFrameSize(zmq_msg_t* msg,
            size_t expectedSize,
            const char* errName,
            bool generateResponse = true);
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
     * This function. has a builtin error limit that prevents it from going
     * to infinite loop because of repeated errors.
     *
     * Automatically logs errors if neccessary.
     */
    void disposeRemainingMsgParts();
    /**
     * Send a response header frame. This function automatically handles request IDs.
     * The request ID from the request header is automatically copied to the response,
     * if any.
     *
     * If the headerFrame parameter is NULL, no request ID is generated.
     * @param responseHeader The response header bytes are stored here
     * @param flags Use ZMQ_SNDMORE here 
     */
    bool sendResponseHeader(const char* responseHeader,
        int flags = 0,
        size_t responseSize = 4);
    /**
     * Calls sendResponseHeader() with the appropriate arguments to
     */
    bool sendErrorResponseHeader(int flags = 0);
    /**
     * A pointer to the currently valid header frame.
     * This is used to ensure request IDs always get sent back
     * even in case of error responses.
     */
    zmq_msg_t headerFrame;
    /**
     * Pointer to the current error response cstr.
     */
    const char* errorResponse;
    /**
     * The expected size of a regular request header frames.
     * This is used to determine the length of the request ID to send back.
     */
    size_t requestExpectedSize;
    /**
     * Bind the input socket and log any errors.
     * @return false in case of error, true otherwise
     */
    bool bindInputSocket(const char* target);
    /**
     * Bind the output socket and log any errors.
     * @return false in case of error, true otherwise
     */
    bool bindOutputSocket(const char* target);
    /**
     * Connect the output socket and log any errors.
     * @return false in case of error, true otherwise
     */
    bool connectInputSocket(const char* target);
    /**
     * Connect the output socket and log any errors.
     * @return false in case of error, true otherwise
     */
    bool connectOutputSocket(const char* target);
    /**
     * Connect the given socket and log any errors.
     * @return false in case of error, true otherwise
     */
    bool connectSocket(void* sock, const char* target);
    /**
     * Bind the given socket and log any errors.
     * @return false in case of error, true otherwise
     */
    bool bindSocket(void* sock, const char* target);
public:
    static bool sendResponseHeader(void* socket,
        Logger& logger,
        zmq_msg_t* headerFrame,
        const char* responseHeader,
        int flags = 0,
        size_t requestExpectedSize = 4,
        size_t responseSize = 4);
};

#endif	/* ABSTRACTFRAMEPROCESSOR_HPP */
