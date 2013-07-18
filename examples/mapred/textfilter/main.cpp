#include <string>
#include <map>

using std::string;

static void* routerSocket

/**
 * This will be called exactly once, when the Map worker is being
 * initialized.
 * @param parameters The parameters from the Map initialization request
 * @param jobId The job ID, which is guaranteed to be unique in the current server context
 * @param mapWorkerId The ID of the map worker
 * @param ctx The ZeroMQ context used for inproc:// communication to the server
 */
void initialize(const std::map<string, string>& parameters,
                uint64_t jobId,
                uint64_t mapWorkerId,
                zctx_t* ctx,
                void* routerSocket) {
    //N
}

/**
 * This will be called exactly once, when the 
 */
void cleanup() {
    
}