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
static inline void sendConstFrame(void* socket, const char* constStr, size_t size, int flags = 0) {
    zmq_msg_t msg;
    zmq_msg_init_data(&msg, constStr, size, nullptr, nullptr);
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
static inline void sendEmptyFrame(void* socket, int flags = 0) {
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
static inline void sendStringFrame(void* socket, const std::string& str, int flags = 0) {
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, str.size());
    memcpy(str.data(), (char*) zmq_msg_data(&msg), str.size());
    return (zmq_msg_send(&msg, socket, flags) == -1) ? errno : 0;
}

/**
 * Send a cstring (length determined using strlen()) in a single frame
 * @param socket The socket to send the data over
 * @param data A pointer to the beginning of the cstring
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline void sendCStringFrame(void* socket, const char* str, int flags = 0) {
    zmq_msg_t msg;
    size_t len = strlen(str);
    zmq_msg_init_size(&msg, len);
    memcpy(str, (char*) zmq_msg_data(&msg), len);
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
static inline void sendBinaryFrame(void* socket, const char* data, size_t size, int flags = 0) {
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, size);
    memcpy(data, (char*) zmq_msg_data(&msg), size);
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
static inline int sendUint32Frame(void* socket, uint32_t data, int flags = 0) {
    return sendBinaryFrame(socket, &data, sizeof (uint32_t). flags);
}

/**
 * Send a little-endian uint64_t in a size-8-frame
 * @param socket The socket to send the data over
 * @param data The value to send
 * @param flags The ZeroMQ zmq_send flags to use, e.g. ZMQ_SNDMORE
 * @return -1 on error (you can check errno to get more information), 0 on success
 */
static inline int sendUint64Frame(void* socket, uint64_t data, int flags = 0) {
    return sendBinaryFrame(socket, &data, sizeof (uint64_t). flags);
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
    str = std::string(zmq_msg_data(&msg), zmq_msg_size(&msg));
    zmq_msg_close(&msg);
    return 0;
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
    //Check if there is any error frame
    int rcvmore;
    if (zmq_msg_data(&msg)[3] != 0) {
        zmq_getsockopt(socket, ZMQ_RCVMORE, &rcvmore, sizeof (int));
        if (!rcvmore) {
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
static int sendKeyValue(void* socket,
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

#endif	/* ZMQ_UTILS_HPP */

