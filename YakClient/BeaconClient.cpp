/* 
 * File:   BeaconClient.cpp
 * Author: uli
 * 
 * Created on 9. Mai 2013, 23:21
 */

#include "BeaconClient.hpp"
#include <cassert>

BeaconClient::BeaconClient(const std::string& clusterName) : beacon(zbeacon_new(7007)) {
    assert(*zbeacon_hostname(beacon));
    std::string publishName = "ZKV/" + clusterName;
    zbeacon_subscribe(beacon, (byte *) publishName.c_str(), publishName.size());
}

BeaconClient::~BeaconClient() {
    zbeacon_destroy(&beacon);
}

std::vector<std::string> BeaconClient::findServers(uint64_t timeout) {
    std::vector<std::string> ret;
    zmq_pollitem_t pollitems [] = {
        { zbeacon_pipe(beacon), 0, ZMQ_POLLIN, 0}
    };
    uint64_t stop_at = zclock_time() + timeout;
    while (zclock_time() < stop_at) {
        uint64_t timeout = (uint64_t) (stop_at - zclock_time());
        if (timeout < 0) {
            timeout = 0;
        }
        if (zmq_poll(pollitems, 3, timeout * ZMQ_POLL_MSEC) == -1) {
            break; //  Interrupted
        }
        //  If we get a message on node 1, it must be NODE/2
        if (pollitems [0].revents & ZMQ_POLLIN) {
            char *ipaddress = zstr_recv(zbeacon_pipe(beacon));
            char *beacon = zstr_recv(zbeacon_pipe(beacon));
            ret.push_back(std::string(ipaddress));
            free(ipaddress);
            free(beacon);
        }
    }
    return ret;
}