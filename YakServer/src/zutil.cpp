#include <cstdlib>
#include "zutil.hpp"

volatile bool yak_interrupted = 0;
//Default handlers, currently unused, can later to be used to properly react to multiple SIGINTs
static struct sigaction sigint_default;
static struct sigaction sigterm_default;

static void sigintHandler (int signal_value) {
    yak_interrupted = 1;
}

void initializeSIGINTHandler() {
    struct sigaction action;
    action.sa_handler = sigintHandler;
    action.sa_flags = 0;
    sigemptyset (&action.sa_mask);
    sigaction (SIGINT, &action, &sigint_default);
    sigaction (SIGTERM, &action, &sigterm_default);
}

void doNothingFree(void *data, void *arg) {
}

void standardFree(void *data, void *hint) {
    assert(data);
    free(data);
}

void sendEmptyFrameMessage(void* socket) {
    assert(socket);
    zmq_send(socket, nullptr, 0, 0);
}

void zmsg_remove_destroy(zmsg_t* msg, zframe_t** frame) {
    assert(msg);
    assert(frame);
    zmsg_remove(msg, *frame);
    zframe_destroy(frame);
}

void* zsocket_new_bind(zctx_t* context, int type, const char* endpoint) {
    assert(context);
    assert(endpoint);
    void* sock = zsocket_new(context, type);
    if(unlikely(!sock)) {
        return NULL;
    }
    if(unlikely(zsocket_bind(sock, endpoint) == -1)) {
        zsocket_destroy(context, sock);
        return NULL;
    }
    return sock;
}

void* zsocket_new_connect(zctx_t* context, int type, const char* endpoint) {
    assert(context);
    assert(endpoint);
    void* sock = zsocket_new(context, type);
    if(unlikely(!sock)) {
        return NULL;
    }
    if(unlikely(zsocket_bind(sock, endpoint) == -1)) {
        zsocket_destroy(context, sock);
        return NULL;
    }
    return sock;
}

void* zmq_socket_new_connect(void* context, int type, const char* endpoint) {
    assert(context);
    assert(endpoint);
    void* sock = zmq_socket(context, type);
    if(unlikely(!sock)) {
        return NULL;
    }
    if(unlikely(zmq_connect(sock, endpoint) == -1)) {
        zmq_close(sock);
        return NULL;
    }
    return sock;
}

void* zmq_socket_new_bind(void* context, int type, const char* endpoint) {
    assert(context);
    assert(endpoint);
    void* sock = zmq_socket(context, type);
    if(unlikely(!sock)) {
        return NULL;
    }
    if(unlikely(zmq_bind(sock, endpoint) == -1)) {
        zmq_close(sock);
        return NULL;
    }
    return sock;
}

void zmq_set_hwm(void* socket, int hwm) {
    int rc = zmq_setsockopt (socket, ZMQ_SNDHWM, &hwm, sizeof (int));
    assert(rc != -1);
}

void zmq_set_ipv4only(void* socket, bool isIPv4Only) {
    int ipv4Only = isIPv4Only ? 1 : 0;
    int rc = zmq_setsockopt (socket, ZMQ_IPV4ONLY, &ipv4Only, sizeof (int));
    assert(rc != -1);
}

void* zmq_socket_new_bind_hwm(void* context, int type, const char* endpoint, int hwm) {
    assert(context);
    assert(endpoint);
    void* sock = zmq_socket(context, type);
    if(unlikely(!sock)) {
        return NULL;
    }
    zmq_set_hwm(sock, hwm);
    if(unlikely(zmq_bind(sock, endpoint) == -1)) {
        zmq_close(sock);
        return NULL;
    }
    return sock;
}

void* zmq_socket_new_connect_hwm(void* context, int type, const char* endpoint, int hwm) {
    assert(context);
    assert(endpoint);
    void* sock = zmq_socket(context, type);
    if(unlikely(!sock)) {
        return NULL;
    }
    zmq_set_hwm(sock, hwm);
    if(unlikely(zmq_connect(sock, endpoint) == -1)) {
        zmq_close(sock);
        return NULL;
    }
    return sock;
}

std::string frameToString(zframe_t* frame) {
    return std::string((char*) zframe_data(frame), zframe_size(frame));
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