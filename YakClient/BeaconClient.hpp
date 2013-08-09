/* 
 * File:   BeaconClient.hpp
 * Author: uli
 *
 * Created on 9. Mai 2013, 23:21
 */

#ifndef BEACONCLIENT_HPP
#define	BEACONCLIENT_HPP
#include <vector>
#include <czmq.h>

class BeaconClient {
public:
    BeaconClient(const std::string& clusterName);
    ~BeaconClient();
    /**
     * Listen on the beacon for a given timeout and return a list of servers.
     * @param timeout The timeout in milliseconds
     * @return 
     */
    std::vector<std::string> findServers(uint64_t timeout);
private:
    zbeacon_t* beacon;
};

#endif	/* BEACONCLIENT_HPP */

