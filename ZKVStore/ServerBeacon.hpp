/* 
 * File:   ServerBeacon.hpp
 * Author: uli
 *
 * Created on 9. Mai 2013, 21:16
 */

#ifndef SERVERBEACON_HPP
#define	SERVERBEACON_HPP
#include <czmq.h>

class ServerBeacon {
public:
    ServerBeacon(const std::string& clusterName, uint32_t interval = 1000);
    void startBeacon(uint32_t intervalMsec);
    ~ServerBeacon();
private:
    zbeacon_t* beacon;
};

#endif	/* SERVERBEACON_HPP */

