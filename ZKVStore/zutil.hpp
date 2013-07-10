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

/**
 * If errno is not zero, print a
 * @param action What you did before the error happened
 */
inline void debugZMQError(const char* action, int error) {
    if (error == 0) {
        return;
    } else {
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
    if (likely(error == 0)) {
        return;
    } else {
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
static inline int receiveLogError(zmq_msg_t* msg, void* sock, Logger& logger) {
    int rc = zmq_msg_recv(msg, sock, 0);
    if(unlikely(rc != 0)) {
        logger.warn(std::string("Error while receiving message part: " + zmq_strerror(zmq_errno())));
        return 1;
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
 * 
 * It won't be logged as error because protocol because the server usually
 * can easily recover from protocol errors.
 */
static inline int receiveExpectMore(zmq_msg_t* msg, void* sock, Logger& logger) {
    if(unlikely(receiveLogError(msg, sock, logger))) {
        return 1;
    }
    //Check rcvmore
    int rcvmore = 0;
    size_t rcvmore_size = sizeof(int);
    zmq_getsockopt(sock, ZMQ_RCVMORE, &rcvmore, &rcvmore_size);
    if(unlikely(!rcvmore)) {
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
 * 
 * It won't be logged as error because protocol because the server usually
 * can easily recover from protocol errors.
 */
static inline int receiveExpectNoMore(zmq_msg_t* msg, void* sock, Logger& logger) {
    if(unlikely(receiveLogError(msg, sock, logger))) {
        return 1;
    }
    //Check rcvmore
    int rcvmore = 0;
    size_t rcvmore_size = sizeof(int);
    zmq_getsockopt(sock, ZMQ_RCVMORE, &rcvmore, &rcvmore_size);
    if(unlikely(rcvmore)) {
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
 */
static inline void sendConstFrame(const void* data, size_t size, void* socket, int flags = 0) {
    zmq_msg_t msg;
    if(unlikely(zmq_msg_init_data(&msg, (void*)data, size, nullptr, nullptr) != 0)) {
        fprintf(stderr, "Error '%s' while trying to initialize message part\n", zmq_strerror(errno));
        fflush(stderr);
    }
    memcpy(zmq_msg_data(&msg), data, size);
    if(unlikely(zmq_msg_send(&msg, socket, flags) != 0)) {
        fprintf(stderr, "Error '%s' while trying to send message part\n", zmq_strerror(errno));
        fflush(stderr);
    }
}

/**
 * Send nonconstant data over a socket.
 * Uses ZMQ low-level API.
 * 
 * Any error is reported on stderr.
 */
static inline void sendFrame(const void* data, size_t size, void* socket, int flags = 0) {
    zmq_msg_t msg;
    if(unlikely(zmq_msg_init_size(&msg, size) != 0)) {
        fprintf(stderr, "Error '%s' while trying to initialize message part\n", zmq_strerror(errno));
        fflush(stderr);
    }
    memcpy(zmq_msg_data(&msg), data, size);
    if(unlikely(zmq_msg_send(&msg, socket, flags) != 0)) {
        fprintf(stderr, "Error '%s' while trying to send message part\n", zmq_strerror(errno));
        fflush(stderr);
    }
}

/**
 * Send nonconstant data over a socket.
 * Uses ZMQ low-level API 
 */
static inline void sendFrame(const std::string& msgStr, void* socket, int flags = 0) {
    zmq_msg_t msg;
    if(unlikely(zmq_msg_init_size(&msg, msgStr.size()) != 0)) {
        fprintf(stderr, "Error '%s' while trying to initialize message part\n", zmq_strerror(errno));
        fflush(stderr);
    }
    memcpy(zmq_msg_data(&msg), msgStr.c_str(), msgStr.size());
    if(unlikely(zmq_msg_send(&msg, socket, flags) != 0)) {
        fprintf(stderr, "Error '%s' while trying to send message part\n", zmq_strerror(errno));
        fflush(stderr);
    }
}

/**
 * Receives frames from the given socket until ZMQ_RCVMORE is false.
 * Releases any received frame immediately
 */
static inline void recvAndIgnore(void* socket) {
    //TODO check errs
    zmq_msg_t msg;
    int rcvmore = 0;
    size_t rcvmore_size = sizeof(int);
    while(true) {
        zmq_msg_recv(&msg, socket, 0);
        zmq_msg_close(&msg);
        zmq_getsockopt(socket, ZMQ_RCVMORE, &rcvmore, &rcvmore_size);
        if(!rcvmore) {
            break;
        }
    }
}

/**
 * Receives message parts from srcSocket until RCVMORE is not set anymore
 * (even if RCVMORE is not set at all)
 * and directly writes them into dstSocket
 * 
 * This works similar to zmq_proxy, but only for a single message and only
 * in one direction.
 * 
 * This also works if the message has already been partially read.
 */
static inline void proxySingleMessage(void* srcSocket, void* dstSocket) {
    //TODO check errs
    zmq_msg_t msg;
    int rcvmore = 0;
    size_t rcvmore_size = sizeof(int);
    zmq_getsockopt(srcSocket, ZMQ_RCVMORE, &rcvmore, &rcvmore_size);
    while(true) {
        if(!rcvmore) {
            break;
        }
        zmq_msg_recv(&msg, srcSocket, 0);
        zmq_getsockopt(srcSocket, ZMQ_RCVMORE, &rcvmore, &rcvmore_size);
        zmq_msg_send(msg, dstSocket, (rcvmore ? ZMQ_SNDMORE : 0));
    }
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
 * Create a new empty (zero-length) frame
 * @return 
 */
zframe_t* createEmptyFrame();

/**
 * Create a new message that contains exactly one zero-length frame
 * @return 
 */
zmsg_t* createEmptyFrameMessage();

/**
 * Creates a new empty-frame message using createEmptyFrameMessage()
 * and sends it over the given socket
 */
void sendEmptyFrameMessage(void* socket);

/**
 * Create a new frame of constant data.
 * The data will not be deallocated after usage.
 * @param data
 * @param size
 * @return 
 */
zframe_t* createConstFrame(const char* data, size_t size);
/**
 * Create a new frame of constant data.
 * The data will not be deallocated after usage.
 * strlen(data) is used as size.
 * @param data
 * @return 
 */
zframe_t* createConstFrame(const char* data);

void zmsg_remove_destroy(zmsg_t* msg, zframe_t** frame);

/**
 * Create and bind a ZeroMQ socket in a single step
 * @return 
 */
void* zsocket_new_bind(zctx_t* context, int type, const char* endpoint);

/**
 * Create and connect a ZeroMQ socket in a single step
 * @return 
 */
void* zsocket_new_connect(zctx_t* context, int type, const char* endpoint);

/**
 * Convert a ZMQ frame to a string.
 * Can be inefficient, avoid using in IML if possible
 * @param frame
 * @return 
 */
std::string frameToString(zframe_t* frame);

#endif	/* ZUTIL_HPP */

