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

void removeAndDestroyFrame(zmsg_t* msg, zframe_t* frame) {
    assert(msg);
    assert(frame);
    zmsg_remove(msg, frame);
    zmsg_destroty(msg);
}