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
    zmsg_t* msg = createEmptyFrameMessage();
    zmsg_send(&msg, socket);
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
}

void* zsocket_new_connect(zctx_t* context, int type, const char* endpoint) {
    void* sock = zsocket_new(context, type);
    zsocket_bind(sock, endpoint);
}

