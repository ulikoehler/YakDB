#include "ThreadUtil.hpp"
#include <pthread.h>
#include <thread>

void setCurrentThreadName(const char* threadName) {
    pthread_setname_np(pthread_self(), threadName);
}