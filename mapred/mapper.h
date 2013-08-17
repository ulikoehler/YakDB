#ifndef __MAPPER_H
#define __MAPPER_H

#include <string>
#include <map>
#include <yakclient/YakClient.hpp>
#include <yakclient/Batch.hpp>

//initialize(), map(), and cleanup() must implement this
#define YAKEXPORT extern "C"

/**
 * This function is called once on initialization.
 * @param connection A fast connection to the server.
 *              Usually this is used to write data to the output table.
 * @param parameters The parameter map.
 *      The "outputTable" parameter contains the configured output table.
 */
void YAKEXPORT initialize(YakConnection& connection,
                          const std::map<std::string, std::string>& parameters);

/**
 * This function is called for every map key.
 * @parameter key The key. The application may modify this without restrictions,
 *      but not access more than keyLength bytes.
 * @param
 * @parameter value The value, corresponding to the key. The application may modify 
 *      this without restrictions, but not access more than keyLength bytes.
 */
void YAKEXPORT map(char* key, size_t keyLength, char* value, size_t valueLength);

/**
 * This is called once when the last key has been read.
 * It shall cleanup the application state.
 * 
 * The map job might be deallocated after this is called,
 * or initialize() might be called again.
 */
void YAKEXPORT cleanup(void);

#endif //__MAPPER_H