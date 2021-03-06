/*
 * File:   zmq_utils.hpp
 * Author: uli
 *
 * This header contains inline utility bindings to efficiently
 * send std::strings/cstrings/binary data/const data over a ZMQ socket.
 *
 * It is not recommended to include this header if not needed.
 *
 * Created on 1. August 2013, 19:33
 */

#ifndef ZMQ_UTILS_HPP
#define	ZMQ_UTILS_HPP
#include <zmq.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <map>

static inline int zmq_sockopt_get_rcvmore(void* socket) {
    int rcvmore;
    size_t optlen = sizeof(int);
    zmq_getsockopt(socket, ZMQ_RCVMORE, &rcvmore, &optlen);
    return rcvmore;
}

static inline int socketHasMoreFrames(void* socket) {
    int rcvmore;
    size_t optlen = sizeof(int);
    zmq_getsockopt(socket, ZMQ_RCVMORE, &rcvmore, &optlen);
    return rcvmore == 1; 
}

/**
 * Send an empty (zero-length) frame
 * @param socket The socket to send data over
 * @param flags The send flags, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendEmptyFrame(void* socket, int flags = 0) {
    return (zmq_send_const (socket, nullptr, 0, flags) == -1);
}

/**
 * Send a little-endian uint32_t in a size-4-frame
 * @param socket The socket to send the data over
 * @param data The vahttp://en.wikipedia.org/wiki/Michio_Kakulue to send
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendUint32Frame(void* socket, uint32_t num, int flags = 0) {
    return zmq_send(socket, (void*) &num, sizeof (uint32_t), flags);
}

/**
 * Send a little-endian uint64_t in a size-8-frame
 * @param socket The socket to send the data over
 * @param data The value to send
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendUint64Frame(void* socket, uint64_t num, int flags = 0) {
    return zmq_send(socket, (char*) &num, sizeof (uint64_t), flags);
}

/**
 * Receive a single frame from a socket and store the results in a string.
 * The frame is not expected to contain a terminating NUL character.
 * @param socket The socket to receive from
 * @param str A reference to a string that will be set to the received value
 * @return -1 on error (--> str value is unchanged). 0 else
 */
static inline int receiveStringFrame(void* socket, std::string& str) {
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (rc == -1) {
        return -1;
    }
    str = std::string((char*) zmq_msg_data(&msg), zmq_msg_size(&msg));
    zmq_msg_close(&msg);
    return 0;
}

/**
 * Receive a single-byte boolean-representing frame.
 * If the byte is 0, the resulting boolean is expected to be false.
 * Else, the resulting boolean i
 * @param socket The socket to receive from
 * @param str A reference to a string that will be set to the received value
 * @return < 0 on error. 0 on false result, 1 on true result.
 */
static inline int receiveBooleanFrame(void* socket) {
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (rc == -1) {
        return -1;
    }
    if(zmq_msg_size(&msg) != 1) {
        zmq_msg_close(&msg);
        return -2; //Frame size mismatch
    }
    int val = (((char*)zmq_msg_data(&msg))[0] == 0 ? 0 : 1);
    zmq_msg_close(&msg);
    return val;
}

/**
 * Simple responses are composed of:
 * - A header frame, with character 4 expected to be 0, else an error is assumed
 * - If the 4th header character is not 0, a second frame containing an error message shall be received
 * @param errorString Left unchanged if no error occurs, set to an error description string if any error occurs
 * @return 0 on success. -1 for communication errors, 1 with errorMessage set in case of error-indicating response
 */
static inline int receiveSimpleResponse(void* socket, std::string& errorString) {
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (rc == -1) {
        return -1;
    }
    //Check if there is any error frame (there *should* be one, if the fourth byte is != 0)
    if (((char*) zmq_msg_data(&msg))[3] != 0) {
        if (!zmq_sockopt_get_rcvmore(socket)) {
            errorString = "No error message received from server -- Exact error cause is unknown";
            return -1;
        }
        //We have an error frame from the server. Return it.
        if(receiveStringFrame(socket, errorString) == -1) {
            return -1;
        }
        return 1;
    }
    zmq_msg_close(&msg);
    return 0;
}

/**
 * This sends a two-frame-range construct.
 * @param startKey The range start or the empty string to generate a zero-length frame
 * @param endKey The range end or the empty string to generate a zero-length frame
 * @return -1 on error (--> check errno with zmq_strerror()), 0 else
 */
static inline int sendRange(void* socket, const std::string& startKey, const std::string& endKey, int flags = 0) {
    int rc = zmq_send(socket, startKey.data(), startKey.size(), ZMQ_SNDMORE);
    if (rc == -1) {
        return rc;
    }
    return zmq_send(socket, endKey.data(), endKey.size(), flags);
}

/**
 * Receive two frames that represent key & value
 * @param socket The socket to send over
 * @param key A string ref where the key will be placed
 * @param value A string ref where the value will be placed
 * @param last If this parameter is set to true, the value frame will be sent without SNDMORE flag
 * @return -1 on error (--> check errno with zmq_strerror()), 0 if this is the last frame, 1 if more frames are to follow
 */
static int receiveKeyValue(void* socket, std::string& keyTarget, std::string& valueTarget) {
    if (receiveStringFrame(socket, keyTarget) == -1) {
        return -1;
    }
    //Check if there is another frame
    if (!zmq_sockopt_get_rcvmore(socket)) {
        errno = EAGAIN;
        return -1;
    }
    if(receiveStringFrame(socket, valueTarget) == -1) {
        return -1;
    }
    return (zmq_sockopt_get_rcvmore(socket) ? 1 : 0);
}


/**
 * Receive a map of alternating key/value frames from a socket
 * and place the k/v pairs in a std::map.
 * If no frames are available, no action is being performed.
 */
static bool receiveMap(void* socket, std::map<std::string, std::string>& target) {
    //Maybe there are no key/value frames at all!?!? This is not an error
    if(!socketHasMoreFrames(socket)) {
        return true;
    }
    //Receive frame pairs until nothing is left
    std::string key;
    std::string value;
    while(true) {
        if(receiveStringFrame(socket, key) == -1) {
            return -1;
        }
        //If there is a trailing key frame (with no value frame), it's an error
        if(socketHasMoreFrames(socket)) {
            return -2;
        }
        if(receiveStringFrame(socket, value) == -1) {
            return -3;
        }
        //Insert into result map
        target[key] = value;
        //Stop if there is no frame left
        if (!socketHasMoreFrames(socket)) {
            return true;
        }
    }
}

#endif	/* ZMQ_UTILS_HPP */
