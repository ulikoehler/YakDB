/* 
 * File:   endpoints.hpp
 * Author: uli
 *
 * Created on 23. April 2013, 18:51
 */

#ifndef ENDPOINTS_HPP
#define	ENDPOINTS_HPP

/**
 * Any request sent to this PULL socket is directly proxied to the external REQ/REP socket.
 */
#define externalRequestProxyEndpoint "inproc://mainRequestProxy"
/**
 * The table open/close worker thread listens on this endpoint.
 */
#define tableOpenEndpoint "inproc://tableopenWorker"

//Internal endpoints. Do not use externally.
#define updateWorkerThreadAddr "inproc://updateWorkerThreads"
#define readWorkerThreadAddr "inproc://readWorkerThreads"
//"Fast-path" to the main router, NOT the return path!
#define mainRouterAddr "inproc://mainRouter" 
#define asyncJobRouterAddr "inproc://asyncJobRouter"

#endif	/* ENDPOINTS_HPP */

