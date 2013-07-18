#include <string>

/**
 * A data sink for MapReduce applications.
 * Only one thread may write to this.
 * Not synchronized.
 * 
 * Provides a simple Key-Value interface to a database.
 * 
 * Auto-batches writes.
 */
class DataSink {
public:
    /**
     * Construct a new data sink.
     * 
     * @param socket A ZeroMQ REQ/REP socket.
     *  Must be already connected, won't be closed from inside this class
     * @param batchSize The batch size for autobatching
     */
    DataSink(void* socket, size_t batchSize=1000);
    int write();
    /**
     * Destructs the current DataSink instance.
     * Automatically flushes the current batch if neccessary.
     */
    ~DataSink();
private:
public:
    void* socket;
    size_t currentSize;
    size_t batchSize;
}