/* 
 * File:   Requests.cpp
 * Author: uli
 * 
 * Created on 18. Juli 2013, 21:55
 */

#include "Requests.hpp"
#include <zmq.h>

using ZettaCrunchDB::WriteRequests::PutRequest;
using ZettaCrunchDB::MetaRequests::ServerInfoRequest;

static inline void sendConstFrame(void socket, const char* constStr, size_t size, int flags) {
    zmq_msg_t msg;
    zmq_msg_init_data(&msg, constStr, size, nullptr, nullptr);
    return (zmq_msg_send(&msg, socket, flags) == -1) ? errno : 0;
}

/**
 * Send an empty (zero-length) frame
 */
static inline void sendEmptyFrame(void socket, int flags) {
    zmq_msg_t msg;
    zmq_msg_init_data(&msg, nullptr, 0, nullptr, nullptr);
    return (zmq_msg_send(&msg, socket, flags) == -1) ? errno : 0;
}

static inline void sendStringFrame(void socket, const std::string& str, int flags) {
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, str.size());
    memcpy(str.data(), (char*) zmq_msg_data(&msg), str.size());
    return (zmq_msg_send(&msg, socket, flags) == -1) ? errno : 0;
}

static inline void sendCStringFrame(void socket, const char* str, int flags) {
    zmq_msg_t msg;
    size_t len = strlen(str);
    zmq_msg_init_size(&msg, len);
    memcpy(str, (char*) zmq_msg_data(&msg), len);
    return (zmq_msg_send(&msg, socket, flags) == -1) ? errno : 0;
}

static inline void sendBinaryFrame(void socket, const char* str, size_t size, int flags) {
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, size);
    memcpy(str, (char*) zmq_msg_data(&msg), size);
    return (zmq_msg_send(&msg, socket, flags) == -1) ? errno : 0;
}

static inline int receiveStringFrame(void socket, std::string& str) {
    zmq_msg_t msg;
    int rc = zmq_msg_recv(&msg, socket, 0);
    if(rc == -1) {
        return -1;
    }
    str = std::string(zmq_msg_data(&msg), zmq_msg_size(&msg));
    zmq_msg_close(&msg);
}

static void PutRequest::sendHeader(void* socket, uint32_t table, uint8_t flags) {
    zmq_msg_t msg;
    char data[] = "\x31\x01\x20\x00";
    data[3] = flags;
    //Can't use zero-copy here because of stack alloc
    zmq_msg_init_size(&msg, 4);
    memcpy((char*) zmq_msg_data(&msg), data, 4);
    zmq_msg_send(&msg, socket, ZMQ_SNDMORE);
}

static int ServerInfoRequest::sendRequest(void* socket) {
    return sendConstFrame(socket, "\x31\x01\x00", 3, 0);
}

static int ServerInfoRequest::receiveFeatureFlags(void* socket, uint64_t flags) {
    zmq_msg_t msg;
    zmq_msg_recv(&msg, socket, 0);
    if (zmq_msg_size(msg) < (3 + 8)) {
        return errno; //Response size does not match
    }
    char* data = (char*) zmq_msg_data(&msg);
    if (data[0] != 0x31 || data[1] != 0x01 || data[2] != 0x00) {
        return 1; //Magic byte mismatch
    }
    flags = ((uint64_t*) (data + 3))[0];
    zmq_msg_close();
    return 0; //OK
}

static int receiveVersion(void* socket, std::string& serverVersion) {
    zmq_msg_t msg;
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (!rc) {
        return rc;
    }
    serverVersion = std::string((char*) zmq_msg_data(&msg), zmq_msg_size(&msg));
    zmq_msg_close();
    return 0; //OK
}

static int PutRequest::sendKeyValue(void* socket,
        const std::string& key,
        const std::string& value,
        bool last = false) {
    int rc = sendStringFrame(socket, key, ZMQ_SNDMORE);
    if (!rc) {
        return rc;
    }
    return sendStringFrame(socket, value, (last ? 0 : ZMQ_SNDMORE));
}

static int PutRequest::sendKeyValue(void* socket,
        const char* key,
        const char* value,
        bool last = false) {
    int rc = sendCStringFrame(socket, key, ZMQ_SNDMORE);
    if (!rc) {
        return rc;
    }
    return sendCStringFrame(socket, value, (last ? 0 : ZMQ_SNDMORE));
}

static int PutRequest::sendKeyValue(void* socket,
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

static void TableOpenRequest::sendRequest(void* socket, uint32_t tableNo,
        uint64_t lruCacheSize,
        uint64_t tableBlockSize,
        uint64_t writeBufferSize,
        uint64_t bloomFilterSize,
        bool enableCompression) {
    char header[] = "\x31\x01\x00\x00";
    if (enableCompression) {
        header[3] = '\x01';
    }
    sendBinaryFrame(socket, header, 3, ZMQ_SNDMORE);
    sendBinaryFrame(socket, &tableNo, sizeof (uint32_t), ZMQ_SNDMORE);
    //LRU cache size
    if (lruCacheSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, &lruCacheSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Table block size
    if (tableBlockSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, &tableBlockSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Write buffer size
    if (writeBufferSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, &writeBufferSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Bloom filter size
    if (bloomFilterSize == UINT64_MAX) {
        sendEmptyFrame(socket, 0);
    } else {
        sendBinaryFrame(socket, &bloomFilterSize, sizeof (uint64_t), 0);
    }
    return 0;
}

static int TableOpenRequest::receiveResponse(void* socket, std::string& errorString) {
    zmq_msg_t msg;
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (rc == -1) {
        return zmq_errno();
    }
    zmq_msg_close(&msg);
    if(zmq_msg_data(&msg)[3] != 0) {
        //Server indicated error
        receiveStringFrame(socket, errorString);
        return -1;
    }
    return 0;
}