/* 
 * File:   ServerBeacon.cpp
 * Author: uli
 * 
 * Created on 9. Mai 2013, 21:16
 */

#include <string>

#include "ServerBeacon.hpp"

ServerBeacon::ServerBeacon(const std::string& clusterName, uint32_t interval) : beacon(zbeacon_new(7007)) {
    assert(*zbeacon_hostname(beacon));
    zbeacon_noecho(beacon);
    zbeacon_set_interval(beacon, interval);
    std::string publishName = "ZKV/" + clusterName;
    zbeacon_publish(beacon, (byte *) publishName.c_str(), publishName.size());
}

ServerBeacon::~ServerBeacon() {
    zbeacon_silence(beacon);
    zbeacon_destroy(&beacon);
}

