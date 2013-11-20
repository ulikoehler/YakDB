/* 
 * File:   ServerBeacon.cpp
 * Author: uli
 * 
 * Created on 9. Mai 2013, 21:16
 */

#include <string>
//Dynamic linker
#include <dlfcn.h>

#include "ServerBeacon.hpp"

//Typedefs for dynamically loaded functions
#define zbeacon_t void
typedef zbeacon_t* (*zbeacon_new_t)(int /*port_nbr*/);
typedef void (*zbeacon_destroy_t)(zbeacon_t** /*self_p*/);
typedef void (*zbeacon_set_interval_t)(zbeacon_t* /* self*/, int /*interval*/);
typedef void (*zbeacon_noecho_t)(zbeacon_t* /*self*/);
typedef void (*zbeacon_publish_t)(zbeacon_t* /*self*/, byte* /*transmit*/, size_t /*size*/);
typedef void (*zbeacon_silence_t)(zbeacon_t* /*self*/);
typedef void (*zbeacon_subscribe_t)(zbeacon_t* /*self*/, byte* /*filter*/, size_t /*size*/);
typedef void (*zbeacon_unsubscribe_t)(zbeacon_t* self);
typedef void* (*zbeacon_socket_t)(zbeacon_t* self);

//Global functions that are dynamically loaded
//TODO Is there a better solution that global without clogging up the header file and the sourcecode?
static zbeacon_new_t zbeacon_new;
static zbeacon_destroy_t zbeacon_destroy;
static zbeacon_set_interval_t zbeacon_set_interval;
static zbeacon_noecho_t zbeacon_noecho;
static zbeacon_publish_t zbeacon_publish;
static zbeacon_silence_t zbeacon_silence;
static zbeacon_subscribe_t zbeacon_subscribe;
static zbeacon_unsubscribe_t zbeacon_unsubscribe;
static zbeacon_socket_t zbeacon_socket;

ServerBeacon::ServerBeacon(void* context, const std::string& clusterName, uint32_t interval) : 
    context(context),
    clusterName(clusterName),
    interval(interval),
    logger(context, "UDP beacon") {
    //Load the library 
    libczmq = dlopen("libczmq.so", RTLD_LAZY);
    char* dlopenError = dlerror();
    if(unlikely(dlopenError != nullptr)) {
        logger.error("Error while loading CZMQ library for the UDP beacon: " + std::string(dlopenError));
        return;
    }
    //Resolve the symbols
    zbeacon_new = (zbeacon_new_t) dlsym(libczmq, "zbeacon_new");
    zbeacon_destroy =  (zbeacon_destroy_t) dlsym(libczmq, "zbeacon_destroy");
    zbeacon_set_interval =  (zbeacon_set_interval_t) dlsym(libczmq, "zbeacon_set_interval");
    zbeacon_noecho =  (zbeacon_noecho_t) dlsym(libczmq, "zbeacon_noecho");
    zbeacon_publish = (zbeacon_publish_t) dlsym(libczmq, "zbeacon_publish");
    zbeacon_silence = (zbeacon_silence_t) dlsym(libczmq, "zbeacon_silence");
    zbeacon_subscribe = (zbeacon_subscribe_t) dlsym(libczmq, "zbeacon_subscribe");
    zbeacon_socket = (zbeacon_socket_t) dlsym(libczmq, "zbeacon_socket_t");
    //assert(*zbeacon_hostname(beacon));
    zbeacon_noecho(beacon);
    zbeacon_set_interval(beacon, interval);
    std::string publishName = "ZKV/" + clusterName;
    zbeacon_publish(beacon, (byte *) publishName.c_str(), publishName.size());
}

ServerBeacon::~ServerBeacon() {
    
    //zbeacon_silence(beacon);
    //zbeacon_destroy(&beacon);
}

