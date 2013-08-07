/* 
 * File:   SequentialIDGenerator.hpp
 * Author: uli
 *
 * Created on 10. Februar 2013, 04:14
 */

#ifndef SEQUENTIALIDGENERATOR_HPP
#define	SEQUENTIALIDGENERATOR_HPP
#include <atomic>
#include <string>

/**
 * This class provides a persistent auto-incrementing ID generator.
 * The data is persisted to a file specified at construction time when the constructor
 * is called or upon persist() calls.
 * 
 * In any case the IDs returned by this class are always larger than 0.
 * 
 * This class is completely thread-safe and doesn't require any external synchronization.
 * In order to improve performance, this class doesn't use locks.
 */
class SequentialIDGenerator {
public:
    /**
     * Constructor that generates a 
     * @param file
     */
    SequentialIDGenerator(const std::string& file);
    /**
     * Constructs a new sequential ID generator instance that is not persisted to a file
     * persist() calls are ignored silently.
     * 
     * Subsequent calls to setFilename() change this behaviour, the class then behaves
     * like it was constructed with the filename as argument
     */
    SequentialIDGenerator();
    /**
     * Behaves like SequentialIDGenerator() but sets the next ID without needing
     * to call setNextId() manually
     */
    SequentialIDGenerator(uint64_t nextId);


    ~SequentialIDGenerator();

    /**
     * Enables file-persisting behaviour and sets a new filename.
     * If file-persistance is already enabled, changes the filename
     */
    void setFilename(const std::string& newFilename);

    /**
     * Disables file persistence. Subsequent calls to persist() are silently ignored
     * and the destructor does not perform any form of file IO.
     */
    void disableFilePersistence();

    /**
     * Gets a new unique ID by incrementing the ID counter. Synchronized.
     * @return 
     */
    uint64_t getNewId();
    /**
     * Returns the ID that would be returned by getNewId() without modifying
     * the internal ID store.
     * @return 
     */
    uint64_t getNextId() const;

    /**
     * Set the current ID counter to the specified value
     * Any subsequent call to getNewId() will return a value greater than
     * newValue. Synchronized.
     */
    void setNextId(uint64_t newValue);

    /**
     * Immediately persists the current ID counter into the file specified at construction time.
     * 
     * This is automatically called in the destructor and provided for backup & data security reasons.
     * 
     * Synchronized.
     */
    void persist();
private:
    std::string filename;
    std::atomic<uint64_t> nextId;
    bool noFilePersistence;
};

#endif	/* SEQUENTIALIDGENERATOR_HPP */

