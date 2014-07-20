/* 
 * File:   ServerBeacon.hpp
 * Author: uli
 *
 * Created on 9. Mai 2013, 21:16
 */

#ifndef SERVERBEACON_HPP
#define	SERVERBEACON_HPP
#include <zmq.h>

/**
 * A CZMQ ZBeacon-based autodiscovery controller that loads CZMQ using the dynamic
 * linker and therefore does not introduce a hard dependency on it.
 * 
 * The beacon can be 
 */
class ServerBeacon {
public:
    ServerBeacon(void* context, const std::string& clusterName, uint32_t interval = 1000);
    void startBeacon(uint32_t intervalMsec);
    ~ServerBeacon();
private:
    void* context;
    void* beacon;
    std::string clusterName;
    uint32_t interval;
    /**
     */
    void* libczmq;
    Logger logger;
};

#endif	/* SERVERBEACON_HPP */

