#include <cstdlib>
#include <signal.h>
#include "zutil.hpp"

volatile bool yak_interrupted = 0;
//Default handlers, currently unused, can later to be used to properly react to multiple SIGINTs
static struct sigaction sigint_default;
static struct sigaction sigterm_default;

static void sigintHandler (int signal_value) {
    yak_interrupted = 1;
}

int zmqRecvString(void* socket, std::string& result) {
    zmq_msg_t frame;
    zmq_msg_init(&frame);
    int rc = zmq_msg_recv(&frame, socket, 0);
    if(rc == -1) {
        zmq_msg_close(&frame);
        return -1;
    }
    //Convert to string
    result = std::move(std::string((char*)zmq_msg_data(&frame), zmq_msg_size(&frame)));
    return 0;
}

int zmq_proxy_single(void* srcSocket, void* dstSocket) {
    zmq_msg_t msg;
    if(unlikely(zmq_msg_init(&msg) == -1)) {
        return -1;
    }
    int rcvmore;
    do {
        if(unlikely(zmq_msg_recv(&msg, srcSocket, 0) == -1)) {
            return -1;
        }
        rcvmore = zmq_msg_more(&msg);
        if(unlikely(zmq_msg_send(&msg, dstSocket, (rcvmore ? ZMQ_SNDMORE : 0)) == -1)) {
            zmq_msg_close(&msg);
            return -1;
        }
    } while(rcvmore);
    return 0;
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
    int rc = zmq_send_const(socket, nullptr, 0, 0);
    assert(rc != -1);
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

void setHWM(void* socket, int rcvhwm, int sndhwm, Logger& logger) {
    if(zmq_setsockopt(socket, ZMQ_SNDHWM, &sndhwm, sizeof (int)) == -1) {
        logger.error("Error while setting external send HWM: "
                     + std::string(zmq_strerror(errno)));
    }
    if(zmq_setsockopt(socket, ZMQ_RCVHWM, &rcvhwm, sizeof (int)) == -1) {
        logger.error("Error while setting external receive HWM: "
                     + std::string(zmq_strerror(errno)));
    }
}

void zmq_set_ipv6(void* socket, bool isIPv6Enabled) {
    int ipv6 = isIPv6Enabled ? 1 : 0;
    int rc = zmq_setsockopt (socket, ZMQ_IPV6, &ipv6, sizeof (int));
    assert(rc != -1);
}

void* zmq_socket_new_bind_hwm(void* context, int type, const char* endpoint, int rcvhwm, int sndhwm, Logger& logger) {
    assert(context);
    assert(endpoint);
    void* sock = zmq_socket(context, type);
    if(unlikely(!sock)) {
        return NULL;
    }
    setHWM(sock, rcvhwm, sndhwm, logger);
    if(unlikely(zmq_bind(sock, endpoint) == -1)) {
        zmq_close(sock);
        return NULL;
    }
    return sock;
}

void* zmq_socket_new_connect_hwm(void* context, int type, const char* endpoint, int rcvhwm, int sndhwm, Logger& logger) {
    assert(context);
    assert(endpoint);
    void* sock = zmq_socket(context, type);
    if(unlikely(!sock)) {
        return NULL;
    }
    setHWM(sock, rcvhwm, sndhwm, logger);
    if(unlikely(zmq_connect(sock, endpoint) == -1)) {
        zmq_close(sock);
        return NULL;
    }
    return sock;
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

void receiveAndIgnoreFrame(void* socket, Logger& logger, const char* frameDesc) {
    if(unlikely(zmq_recv(socket, nullptr, 0, 0) == -1)) {
        logMessageOperationError(frameDesc, "receive", logger);
    }
}