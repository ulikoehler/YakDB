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

/**
 * Check if the given socket has another frame available in the current message.
 * In other words, check if the ZMQ_RCVMORE flag is set for the given socket
 * @param socket
 * @return true if and only if the ZMQ_RCVMORE flag is set
 */
static inline bool currentMessageHasAnotherFrame(void* socket) {
    int rcvmore;
    size_t optsize;
    zmq_getsockopt(socket, ZMQ_RCVMORE, &rcvmore, &optsize);
    return rcvmore != 0;
}

/**
 * Send constant binary data in a single frame.
 * The data is assumed to be in a const-memory section that is assumed
 * not to change. This function may therefore use zero-copy mechanisms,
 * without freeing the data pointer afterwards in any way.
 * @param socket The socket to send the data over
 * @param constStr A pointer to the beginning of the data
 * @param size The number of bytes to send, beginning at param constStr
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendConstFrame(void* socket, const char* constStr, size_t size, int flags = 0) {
    zmq_msg_t msg;
    zmq_msg_init_data(&msg, (void*) constStr, size, nullptr, nullptr);
    if (zmq_msg_send(&msg, socket, flags) == -1) {
        zmq_msg_close(&msg);
        return -1;
    }
    return 0;
}

/**
 * Send an empty (zero-length) frame
 * @param socket The socket to send data over
 * @param flags The send flags, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendEmptyFrame(void* socket, int flags = 0) {
    zmq_msg_t msg;
    zmq_msg_init_data(&msg, nullptr, 0, nullptr, nullptr);
    if (zmq_msg_send(&msg, socket, flags) == -1) {
        zmq_msg_close(&msg);
        return -1;
    }
    return 0;
}

/**
 * Send a cstring (length determined using strlen()) in a single frame
 * @param socket The socket to send the data over
 * @param data A pointer to the beginning of the cstring
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendStringFrame(void* socket, const std::string& str, int flags = 0) {
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, str.size());
    memcpy(zmq_msg_data(&msg), str.c_str(), str.size());
    return (zmq_msg_send(&msg, socket, flags) == -1) ? errno : 0;
}

/**
 * Send a cstring (length determined using strlen()) in a single frame
 * @param socket The socket to send the data over
 * @param data A pointer to the beginning of the cstring
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendCStringFrame(void* socket, const char* str, int flags = 0) {
    zmq_msg_t msg;
    size_t len = strlen(str);
    zmq_msg_init_size(&msg, len);
    memcpy((char*) zmq_msg_data(&msg), str, len);
    if (zmq_msg_send(&msg, socket, flags) == -1) {
        zmq_msg_close(&msg);
        return -1;
    }
    return 0;
}

/**
 * Send binary data in a single frame
 * @param socket The socket to send the data over
 * @param data A pointer to the beginning of the data
 * @param size The number of bytes to read and send, starting at param data
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendBinaryFrame(void* socket, const char* data, size_t size, int flags = 0) {
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, size);
    memcpy(zmq_msg_data(&msg), data, size);
    if (zmq_msg_send(&msg, socket, flags) == -1) {
        zmq_msg_close(&msg);
        return -1;
    }
    return 0;
}

/**
 * Send a little-endian uint32_t in a size-4-frame
 * @param socket The socket to send the data over
 * @param data The value to send
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendUint32Frame(void* socket, uint32_t num, int flags = 0) {
    return sendBinaryFrame(socket, (char*) &num, sizeof (uint32_t), flags);
}

/**
 * Send a little-endian uint64_t in a size-8-frame
 * @param socket The socket to send the data over
 * @param data The value to send
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendUint64Frame(void* socket, uint64_t num, int flags = 0) {
    return sendBinaryFrame(socket, (char*) &num, sizeof (uint64_t), flags);
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
 * @return 0 on success. -1 for errors without errno, errno else
 */
static inline int receiveSimpleResponse(void* socket, std::string& errorString) {
    zmq_msg_t msg;
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (rc == -1) {
        return zmq_errno();
    }
    zmq_msg_close(&msg);
    //Check if there is any error frame (there *should* be one)
    if (((char*) zmq_msg_data(&msg))[3] != 0) {
        if (!currentMessageHasAnotherFrame(socket)) {
            errorString = "No error message received from server -- Exact error cause is unknown";
            return -1;
        }
        //We have an error frame from the server. Return it.
        receiveStringFrame(socket, errorString);
        return -1;
    }
    return 0;
}

/**
 * Send a key-value frame
 * @param socket The socket to send over
 * @param key The key to send (always sent without ZMQ_SNDMORE)
 * @param value The value to send
 * @param last If this parameter is set to true, the value frame will be sent without SNDMORE flag
 * @return -1 on error (--> check errno with zmq_strerror()), 0 else
 */
static int sendKeyValue(void* socket,
        const std::string& key,
        const std::string& value,
        bool last = false) {
    int rc = sendStringFrame(socket, key, ZMQ_SNDMORE);
    if (!rc) {
        return rc;
    }
    return sendStringFrame(socket, value, (last ? 0 : ZMQ_SNDMORE));
}

/**
 * Send a key-value frame
 * @param socket The socket to send over
 * @param key The cstring key to send (always sent without ZMQ_SNDMORE)
 * @param value The cstring value to send
 * @param last If this parameter is set to true, the value frame will be sent without SNDMORE flag
 * @return -1 on error (--> check errno with zmq_strerror()), 0 else
 */
static int sendKeyValue(void* socket,
        const char* key,
        const char* value,
        bool last = false) {
    int rc = sendCStringFrame(socket, key, ZMQ_SNDMORE);
    if (!rc) {
        return rc;
    }
    return sendCStringFrame(socket, value, (last ? 0 : ZMQ_SNDMORE));
}

/**
 * Send a key-value frame
 * @param socket The socket to send over
 * @param key The key to send (always sent without ZMQ_SNDMORE)
 * @param keyLength The length of the key in bytes
 * @param value The value to send
 * @param valueLength The length of the value in bytes
 * @param last If this parameter is set to true, the value frame will be sent without SNDMORE flag
 * @return -1 on error (--> check errno with zmq_strerror()), 0 else
 */
static inline int sendKeyValue(void* socket,
        const char* key,
        size_t keyLength,
        const char* value,
        size_t valueLength,
        bool last = false) {
    int rc = sendBinaryFrame(socket, key, keyLength, ZMQ_SNDMORE);
    if (!rc) {
        return rc;
    }
    return sendBinaryFrame(socket, value, valueLength, (last ? 0 : ZMQ_SNDMORE));
}

/**
 * This sends a two-frame-range construct.
 * @param startKey The range start or the empty string to generate a zero-length frame
 * @param endKey The range end or the empty string to generate a zero-length frame
 * @return -1 on error (--> check errno with zmq_strerror()), 0 else
 */
static inline int sendRange(void* socket, const std::string& startKey, const std::string& endKey, int flags = 0) {
    int rc = sendStringFrame(socket, startKey, ZMQ_SNDMORE);
    if (!rc) {
        return rc;
    }
    return sendStringFrame(socket, endKey, flags);
}

/**
 * Receive two frames that represent key & value
 * @param socket The socket to send over
 * @param key A string ref where the key will be placed
 * @param value A string ref where the value will be placed
 * @param last If this parameter is set to true, the value frame will be sent without SNDMORE flag
 * @return -1 on error (--> check errno with zmq_strerror()), 0 else
 */
static int receiveKeyValue(void* socket, std::string& keyTarget, std::string& valueTarget) {
    if (receiveStringFrame(socket, keyTarget) == -1) {
        return -1;
    }
    //Check if there is another frame
    if (!currentMessageHasAnotherFrame(socket)) {
        return -1;
    }
    return receiveStringFrame(socket, valueTarget);
}

#endif	/* ZMQ_UTILS_HPP */
