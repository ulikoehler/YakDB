#include <cstdlib>
#include "zutil.hpp"

void doNothingFree(void *data, void *arg) {
}

void standardFree(void *data, void *hint) {
    assert(data);
    free(data);
}

zframe_t* createEmptyFrame() {
    return zframe_new_zero_copy(NULL, 0, doNothingFree, NULL);
}

zframe_t* createConstFrame(const char* data, size_t size) {
    return zframe_new_zero_copy(const_cast<char*> (data), size, doNothingFree, NULL);
}

zframe_t* createConstFrame(const char* data) {
    return createConstFrame(data, strlen(data));
}

zmsg_t* createEmptyFrameMessage() {
    zmsg_t* msg = zmsg_new();
    zmsg_add(msg, createEmptyFrame());
    return msg;
}

void sendEmptyFrameMessage(void* socket) {
    assert(socket);
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, 0);
    zmq_msg_send(&msg, socket, 0);
}

void zmsg_remove_destroy(zmsg_t* msg, zframe_t** frame) {
    assert(msg);
    assert(frame);
    zmsg_remove(msg, *frame);
    zframe_destroy(frame);
}

void* zsocket_new_bind(zctx_t* context, int type, const char* endpoint) {
    void* sock = zsocket_new(context, type);
    zsocket_bind(sock, endpoint);
    return sock;
}

void* zsocket_new_connect(zctx_t* context, int type, const char* endpoint) {
    void* sock = zsocket_new(context, type);
    zsocket_connect(sock, endpoint);
    return sock;
}

std::string frameToString(zframe_t* frame) {
    return std::string((char*) zframe_data(frame), zframe_size(frame));
}

std::string frameToString(zmq_msg_t* msg) {
    
}

void COLD logOperationError(const char* operation, Logger& logger) {
    logger.error("Error '"
                + std::string(zmq_strerror(errno))
                + "' while trying to do operation: '"
                + std::string(operation) + "'");
}

void COLD logMessageOperationError(const char* frameDesc, const char* operation, Logger& logger) {
    logger.error("Error '"
                + std::string(zmq_strerror(errno))
                + "' while trying to "
                + std::string(operation) + " frame '"
                + std::string(frameDesc) + "'");
}

void COLD logMessageInitializationError(const char* frameDesc, Logger& logger) {
    logMessageOperationError(frameDesc, "initialize", logger);
}

void COLD logMessageSendError(const char* frameDesc, Logger& logger) {
    logMessageOperationError(frameDesc, "send", logger);
}

void COLD logMessageRecvError(const char* frameDesc, Logger& logger) {
    logMessageOperationError(frameDesc, "receive", logger);
}