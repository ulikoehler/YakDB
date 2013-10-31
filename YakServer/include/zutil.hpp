/* 
 * File:   zutil.hpp
 * Author: uli
 *
 * Created on 8. April 2013, 01:33
 */

#ifndef ZUTIL_HPP
#define	ZUTIL_HPP
#include <czmq.h>
#include <cstdio>
#include <string>
#include "Logger.hpp"
#include "macros.hpp"

extern volatile bool yak_interrupted;

/**
 * Initialize the SIGINT handler that sets yak_interrupted
 * and avoids hard server shutdown.
 */
void initializeSIGINTHandler();

/**
 * Log an error during a ZMQ operation, evaluating errno
 * @param operation A description of the operation the error occured at
 * @param logger The logger to log the error message to (loglevel: error)
 */
void COLD logOperationError(const char* operation, Logger& logger);
/**
 * Log an error during the ZMQ msg lifecycle, evaluating errno
 * @param frameDesc A description of the frame that is related to the error
 * @param operation The phase during the message lifecycle the error occured at
 * @param logger The logger to log the error message to (loglevel: error)
 */
void COLD logMessageOperationError(const char* frameDesc, const char* operation, Logger& logger);
/**
 * Log an error during a zmq_msg_init() call, evaluating errno
 * @param frameDesc A description of the frame that is related to the error
 * @param logger The logger to log the error message to (loglevel: error)
 */
void COLD logMessageInitializationError(const char* frameDesc, Logger& logger);
/**
 * Log an error during a zmq_msg_send() call, evaluating errno
 * @param frameDesc A description of the frame that is related to the error
 * @param logger The logger to log the error message to (loglevel: error)
 */
void COLD logMessageSendError(const char* frameDesc, Logger& logger);
/**
 * Log an error during a zmq_msg_recv() call, evaluating errno
 * @param frameDesc A description of the frame that is related to the error
 * @param logger The logger to log the error message to (loglevel: error)
 */
void COLD logMessageRecvError(const char* frameDesc, Logger& logger);

/**
 * If errno is not zero, print a
 * @param action What you did before the error happened
 */
inline void debugZMQError(const char* action, int error) {
    if (error == -1) {
        fprintf(stderr, "Error '%s' occured during action '%s'\n", zmq_strerror(error), action);
        fflush(stderr);
    }
}

/**
 * If errno is not zero, log the stringified error.
 * Made to be used inline
 * @param action What you did before the error happened
 */
static inline void logZMQError(int error, const char* action, Logger& logger) {
    if (unlikely(error == -1)) {
        logger.error(std::string("Error '") + zmq_strerror(error) + "' occured during action '" + action);
    }
}

/**
 * Receive a single frame, and store it in msg.
 * If any error occurs, return != 0 and log a warning message on the logger.
 * 
 * Returns 0 on success.
 * 
 * It won't be logged as error because protocol because the server usually
 * can easily recover from protocol errors.
 */
static inline int receiveLogError(zmq_msg_t* msg, void* sock, Logger& logger, const char* frameDesc) {
    if(unlikely(zmq_msg_recv(msg, sock, 0) == -1)) {
        logMessageRecvError(frameDesc, logger);
        return -1;
    }
    return 0;
}

/**
 * Receive a single frame, store it in msg and confirm that the RCVMORE
 * flag is set (--> expect to receive more data).
 * If any error occurs or RCVMORE is not set,
 * return != 0 and log a warning message on the logger.
 * 
 * Returns 0 on success.
 * Returns 1 when the received frame is the last frame.
 * Returns 2 when an error occured
 * 
 * It won't be logged as error because protocol because the server usually
 * can easily recover from protocol errors.
 */
static inline int receiveExpectMore(zmq_msg_t* msg, void* sock, Logger& logger, const char* frameDesc) {
    if (unlikely(receiveLogError(msg, sock, logger, frameDesc) == -1)) {
        return 2;
    }
    if (unlikely(!zmq_msg_more(msg))) {
        logger.warn("RCVMORE flag is unset, but we've been expecting more message parts!");
        return 1;
    }
    return 0;
}

/**
 * Receive a single frame, store it in msg and confirm that the RCVMORE
 * flag is NOT set (--> expect the received frame to be the last frame).
 * If any error occurs or the RCVMORE flag is set, return != 0 and log
 * a warning message on the logger.
 * 
 * Returns 0 on success.
 * Returns 1 when the received frame is the last frame.
 * Returns 2 when an error occured
 * 
 * It won't be logged as error because protocol because the server usually
 * can easily recover from protocol errors.
 */
static inline int receiveExpectNoMore(zmq_msg_t* msg, void* sock, Logger& logger, const char* frameDesc) {
    if (unlikely(receiveLogError(msg, sock, logger, frameDesc) == -1)) {
        return 2;
    }
    //Check rcvmore
    if (unlikely(zmq_msg_more(msg))) {
        logger.warn("RCVMORE flag is set, but we've expected the current message part to be the last one!");
        return 1;
    }
    return 0;
}

/**
 * Send constant data over a socket using zero-copy as far as possible.
 * Uses ZMQ low-level API
 * 
 * Any error is reported on stderr.
 * 
 * Returns -1 on error
 */
static inline int sendConstFrame(const void* data, size_t size, void* socket, Logger& logger, const char* frameDesc, int flags = 0) {
    if (unlikely(zmq_send_const(socket, data, size, flags) == -1)) {
        logMessageSendError(frameDesc, logger);
        return -1;
    }
    return 0;
}

/**
 * Send nonconstant data over a socket.
 * Uses ZMQ low-level API.
 * 
 * Any error is reported on stderr.
 * 
 * Returns -1 on error
 */
static inline int sendFrame(const void* data, size_t size, void* socket, Logger& logger, const char* frameDesc, int flags = 0) {
    if (unlikely(zmq_send(socket, data, size, flags) == -1)) {
        logMessageSendError(frameDesc, logger);
        return -1;
    }
    return 0;
}

/**
 * Send nonconstant data over a socket.
 * Logger-based versions
 * Uses ZMQ low-level API 
 * Returns -1 on error.
 */
static inline int sendFrame(const std::string& msgStr, void* socket, Logger& logger, const char* frameDesc, int flags = 0) {
    zmq_msg_t msg;
    if (unlikely(zmq_msg_init_size(&msg, msgStr.size()) == -1)) {
        logMessageInitializationError(frameDesc, logger);
        return -1;
    }
    memcpy(zmq_msg_data(&msg), msgStr.c_str(), msgStr.size());
    if (unlikely(zmq_msg_send(&msg, socket, flags) == -1)) {
        logMessageSendError(frameDesc, logger);
        return -1;
    }
    return 0;
}

/**
 * For a given socket, return true only if there are more
 * message parts in the current message.
 */
inline static bool socketHasMoreFrames(void* socket) {
    return zsocket_rcvmore(socket);
}

/**
 * Receives frames from the given socket until ZMQ_RCVMORE is false.
 * Releases any received frame immediately.
 */
static inline void recvAndIgnore(void* socket) {
    //TODO check err
    zmq_msg_t msg;
    if (!socketHasMoreFrames(socket)) {
        return;
    }
    zmq_msg_init(&msg);
    while (true) {
        zmq_msg_recv(&msg, socket, 0);
        zmq_msg_close(&msg);
        if (!socketHasMoreFrames(socket)) {
            break;
        }
    }
}

/**
 * Receives and ignores a single frame one the given socket.
 * Logs any errors to the logger
 */
void receiveAndIgnoreFrame(void* socket, Logger& logger, const char* frameDesc = "<Undefined>");

/**
 * Receives message parts from srcSocket until RCVMORE is not set anymore
 * (if RCVMORE is not set when calling this function, it just returns)
 * and directly writes them into dstSocket. Only the last message part that is sent
 * (if any is sent at all) will have ZMQ_SNDMORE unset.
 * 
 * This works similar to zmq_proxy, but only for a single message and only
 * in one direction.
 * 
 * This also works if the message has already been partially read.
 * 
 * @return -1 on error (--> check errno), 0 on success
 */
static inline int proxyMultipartMessage(void* srcSocket, void* dstSocket, const char* frameDesc = "") {
    //TODO check errs
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    bool rcvmore = socketHasMoreFrames(srcSocket);
    int frameCounter = 0;
    while (rcvmore) {
        if(zmq_msg_recv(&msg, srcSocket, 0) == -1) {
            return -1;
        }
        rcvmore = zmq_msg_more(&msg);
        if(zmq_msg_send(&msg, dstSocket, (rcvmore ? ZMQ_SNDMORE : 0)) == -1) {
            return -1;
        }
        frameCounter++;
    }
    return 0;
}

template<typename T>
inline static void sendBinary(T value, void* socket, Logger& logger, const char* frameDesc = "", int flags = 0) {
    assert(socket);
    sendFrame((void*)&value, sizeof(T), socket, logger, frameDesc, flags);
}

/**
 * Utility function to convert frame data to a struct-like type
 * by casting (no explicit deserialization)s
 * @param frame
 * @return 
 */
template<typename T>
inline static T extractBinary(zmq_msg_t* frame) {
    assert(frame);
    assert(zmq_msg_size(frame) == sizeof (T));
    return *((T*) zmq_msg_data(frame));
}

/**
 * Utility function to receive frame data to a binary buffer.
 * An additional bytes
 * @param frame
 * @return 0 on success, -1 on ZMQ error, num received bytes when too few bytes were received
 */
template<typename T>
inline static int receiveBinary(void* socket, T* dest) {
    assert(socket);
    assert(dest);
    int rc = zmq_recv(socket, (void*)dest, sizeof(T), 0);
    if(unlikely(rc == -1)) {
        return -1;
    } else if(rc < sizeof(T)) {
        return rc; //== number of bytes received
    }
    return 0;
}


/**
 * ZMQ zero-copy free function that uses standard C free
 */
void standardFree(void *data, void *hint);
/**
 * ZMQ zero-copy free function that does ...well... nothing     
 * @param data
 * @param hint
 */
void doNothingFree(void *data, void *arg);

/**
 * Creates a new empty-frame message using createEmptyFrameMessage()
 * and sends it over the given socket
 */
void sendEmptyFrameMessage(void* socket);

/**
 * Create and bind a ZeroMQ socket in a single step
 * @return 
 */
void* zmq_socket_new_connect(void* context, int type, const char* endpoint);

/**
 * Create and connect a ZeroMQ socket in a single step
 * @return 
 */
void* zmq_socket_new_bind(void* context, int type, const char* endpoint);

/**
 * Create and connect a ZeroMQ socket in a single step
 * @return 
 */
void* zmq_socket_new_bind_hwm(void* context, int type, const char* endpoint, int hwm);

/**
 * Create and bind a ZeroMQ socket in a single step
 * @return 
 */
void* zmq_socket_new_connect_hwm(void* context, int type, const char* endpoint, int hwm);

void zmq_set_hwm(void* socket, int hwm);
void zmq_set_ipv6(void* socket, bool enableIPv6);

#endif	/* ZUTIL_HPP */

