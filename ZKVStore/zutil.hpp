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
 * Send constant data over a socket using zero-copy as far as possible.
 * Uses ZMQ low-level API 
 */
static inline void sendConstFrame(const void* data, size_t size, void* socket, int flags = 0) {
    //TODO check errs
    zmq_msg_t msg;
    zmq_msg_init_data(&msg, (void*)data, size, nullptr, nullptr);
    zmq_msg_send(&msg, socket, flags);
}

/**
 * Send nonconstant data over a socket.
 * Uses ZMQ low-level API 
 */
static inline void sendFrame(const void* data, size_t size, void* socket, int flags = 0) {
    //TODO check errs
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, size);
    memcpy(zmq_msg_data(&msg), data, size);
    zmq_msg_send(&msg, socket, flags);
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

