/*
 * File:   TableOpenHelper.cpp
 * Author: uli
 *
 * Created on 6. April 2013, 18:15
 */

#include "TableOpenHelper.hpp"
#include <thread>
#include <cassert>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>
#include <rocksdb/options.h>
#include <exception>
#include <fstream>
#include <memory>
#include "zutil.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "Logger.hpp"
#include "protocol.hpp"
#include "MergeOperators.hpp"
#include "FileUtils.hpp"

int sendTableOperationRequest(void* socket, TableOperationRequestType requestType, int flags) {
    return zmq_send(socket, &requestType, sizeof(TableOperationRequestType), flags);
}

TableOpenParameters::TableOpenParameters(const ConfigParser& cfg) :
    lruCacheSize(cfg.defaultLRUCacheSize),
    tableBlockSize(cfg.defaultTableBlockSize),
    writeBufferSize(cfg.defaultWriteBufferSize),
    bloomFilterBitsPerKey(cfg.defaultBloomFilterBitsPerKey),
    compression(cfg.defaultCompression),
    mergeOperatorCode(cfg.defaultMergeOperator) {
}

void TableOpenParameters::parseFromParameterMap(std::map<std::string, std::string>& parameters) {
    if(parameters.count("LRUCacheSize")) {
        lruCacheSize = stoull(parameters["LRUCacheSize"]);
    }
    if(parameters.count("Blocksize")) {
        tableBlockSize = stoull(parameters["Blocksize"]);
    }
    if(parameters.count("WriteBufferSize")) {
        writeBufferSize = stoull(parameters["WriteBufferSize"]);
    }
    if(parameters.count("BloomFilterBitsPerKey")) {
        bloomFilterBitsPerKey = stoull(parameters["BloomFilterBitsPerKey"]);
    }
    if(parameters.count("CompressionMode")) {
        compression = compressionModeFromString(parameters["CompressionMode"]);
    }
    if(parameters.count("MergeOperator")) {
        mergeOperatorCode = parameters["MergeOperator"];
    }
}

void COLD TableOpenParameters::toParameterMap(std::map<std::string, std::string>& parameters) {
    parameters["LRUCacheSize"] = std::to_string(lruCacheSize);
    parameters["Blocksize"] = std::to_string(tableBlockSize);
    parameters["WriteBufferSize"] = std::to_string(writeBufferSize);
    parameters["BloomFilterBitsPerKey"] = std::to_string(bloomFilterBitsPerKey);
    parameters["CompressionMode"] = compressionModeToString(compression);
    parameters["MergeOperator"] = mergeOperatorCode;
}

void TableOpenParameters::getOptions(rocksdb::Options& options) {
    rocksdb::BlockBasedTableOptions bbOptions;
    //For all numeric options: <= 0 --> disable / use default
    if(lruCacheSize > 0) {
        bbOptions.block_cache =
            std::shared_ptr<rocksdb::Cache>(rocksdb::NewLRUCache(lruCacheSize));
    }
    if (tableBlockSize > 0) {
        bbOptions.block_size = tableBlockSize;
    }
    if (writeBufferSize > 0) {
        options.write_buffer_size = writeBufferSize;
    }
    if(bloomFilterBitsPerKey > 0) {
        const rocksdb::FilterPolicy* fp =
            rocksdb::NewBloomFilterPolicy(bloomFilterBitsPerKey);
        bbOptions.filter_policy = std::shared_ptr<const rocksdb::FilterPolicy>(fp);
    }
    options.create_if_missing = true;
    //Compression. Default: Snappy
    options.compression = (rocksdb::CompressionType) compression;
    //Merge operator
    options.merge_operator = createMergeOperator(mergeOperatorCode);
    //Create table factory from advanced block options
    rocksdb::TableFactory* tf = rocksdb::NewBlockBasedTableFactory(bbOptions);
    options.table_factory = std::shared_ptr<rocksdb::TableFactory>(tf);
}

void COLD TableOpenParameters::readTableConfigFile(const ConfigParser& cfg, uint32_t tableIndex) {
    std::string cfgFileName = cfg.getTableConfigFile(tableIndex);
    if(fileExists(cfgFileName)) {
        std::string line;
        std::ifstream fin(cfgFileName.c_str());
        while(fin.good()) {
            fin >> line;
            //Split line (at '=') into  & value
            size_t sepIndex = line.find_first_of('=');
            assert(sepIndex != std::string::npos);
            std::string key = line.substr(0, sepIndex);
            std::string value = line.substr(sepIndex+1);
            if(key == "LRUCacheSize") {
                lruCacheSize = stoull(value);
            } else if(key == "Blocksize") {
                tableBlockSize = stoull(value);
            } else if(key == "WriteBufferSize") {
                writeBufferSize = stoull(value);
            } else if(key == "BloomFilterBitsPerKey") {
                bloomFilterBitsPerKey = stoull(value);
            } else if(key == "CompressionMode") {
                compression = compressionModeFromString(value);
            } else if(key == "MergeOperator") {
                mergeOperatorCode = value;
            } else {
                std::cerr << "Unknown key in table config file : "
                         << key << " (= " << value << ")" << std::endl;
            }
        }
        fin.close();
    }
}

void COLD TableOpenParameters::writeToFile(const ConfigParser& cfg, uint32_t tableIndex) {
    std::string cfgFileName = cfg.getTableConfigFile(tableIndex);
    std::ofstream fout(cfgFileName.c_str());
    if(lruCacheSize != std::numeric_limits<uint64_t>::max()) {
        fout << "LRUCacheSize=" << lruCacheSize << '\n';
    }
    if(tableBlockSize != std::numeric_limits<uint64_t>::max()) {
        fout << "Blocksize=" << tableBlockSize << '\n';
    }
    if(writeBufferSize != std::numeric_limits<uint64_t>::max()) {
        fout << "WriteBufferSize=" << writeBufferSize << '\n';
    }
    if(bloomFilterBitsPerKey != std::numeric_limits<uint64_t>::max()) {
        fout << "BloomFilterBitsPerKey=" << bloomFilterBitsPerKey << '\n';
    }
    fout << "CompressionMode=" << compressionModeToString(compression) << '\n';
    fout << "MergeOperator=" << mergeOperatorCode << '\n';
    fout.close();
}


COLD TableOpenHelper::TableOpenHelper(void* context, ConfigParser& cfg) :
    context(context), cfg(cfg), logger(context, "Table open client") {
    reqSocket = zmq_socket_new_connect(context, ZMQ_REQ, tableOpenEndpoint);
    if (unlikely(!reqSocket)) {
        logger.critical("Table open client REQ socket initialization failed: " + std::string(zmq_strerror(errno)));
    }
}

void COLD TableOpenHelper::openTable(uint32_t tableId, void* paramSrcSock) {
    //Note that the socket could be NULL <=> no parameters
    //Just send a message containing the table index to the opener thread
    if(sendTableOperationRequest(reqSocket, TableOperationRequestType::OpenTable, ZMQ_SNDMORE) == -1) {
        logMessageSendError("table open header message", logger);
    }
    //Determine if there are ANY key/value parameters to be sent
    bool haveParameters = paramSrcSock != nullptr && socketHasMoreFrames(paramSrcSock);
    //Send table no -- last frame if there are no parameters
    sendBinary(tableId, reqSocket, logger, "Table ID", haveParameters ? ZMQ_SNDMORE : 0);
    //Send parameters if there is any socket to proxy them from
    if(haveParameters) {
        if(unlikely(zmq_proxy_single(paramSrcSock, reqSocket) == -1)) {
            logger.critical("Table open client parameter transfer failed: " + std::string(zmq_strerror(errno)));
        }
    }
    //Wait for the reply (reply content is ignored)
    zmq_recv(reqSocket, nullptr, 0, 0); //Blocks until reply received
}

void COLD TableOpenHelper::closeTable(TableOpenHelper::IndexType index) {
    //Just send a message containing the table index to the opener thread
    if(sendTableOperationRequest(reqSocket, TableOperationRequestType::CloseTable, ZMQ_SNDMORE) == -1) {
        logMessageSendError("table close message", logger);
    }
    sendFrame(&index, sizeof (TableOpenHelper::IndexType), reqSocket, logger, "Close table request table index frame", ZMQ_SNDMORE);
    //Wait for the reply (it's empty but that does not matter)
    zmq_msg_t reply;
    zmq_msg_init(&reply);
    //Blocks until reply received == until request processed
    if (unlikely(zmq_msg_recv(&reply, reqSocket, 0) == -1)) {
        logger.error("Close table receive failed: " + std::string(zmq_strerror(errno)));
    }
    zmq_msg_close(&reply);
}

void COLD TableOpenHelper::truncateTable(TableOpenHelper::IndexType index) {
    /**
     * Note: The reason this doesn't use CZMQ even if efficiency does not matter
     * is that repeated calls using CZMQ API cause SIGSEGV somewhere inside calloc.
     */
    if(sendTableOperationRequest(reqSocket, TableOperationRequestType::TruncateTable, ZMQ_SNDMORE) == -1) {
        logMessageSendError("table truncate message", logger);
    }
    sendFrame(&index, sizeof (IndexType), reqSocket, logger, "Table index");
    //Wait for the reply (it's empty but that does not matter)
    recvAndIgnore(reqSocket, logger);
}

COLD TableOpenHelper::~TableOpenHelper() {
    zmq_close(reqSocket);
}
