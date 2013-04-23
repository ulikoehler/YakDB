#include <cstdlib>
#include "zutil.hpp"

void doNothingFree(void *data, void *arg) {
}

void standardFree(void *data, void *hint) {
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
    zmsg_send(createEmptyFrameMessage(), socket);
}