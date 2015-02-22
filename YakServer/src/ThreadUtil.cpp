#include "ThreadUtil.hpp"
#include <pthread.h>
#include <thread>
#include <cassert>
#include <cstring>

void setCurrentThreadName(const char* threadName) {
    //Only 16 chars thread name are supported, including NUL
    assert(strlen(threadName) < 16);
    pthread_setname_np(pthread_self(), threadName);
}