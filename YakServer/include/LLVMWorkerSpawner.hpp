#ifndef LLVM_WORKER_SPAWNER_HPP
#define LLVM_WORKER_SPAWNER_HPP
#include <llvm/Module.h> 
#include <llvm/LLVMContext.h>
#include <llvm/Support/system_error.h>
#include <llvm/ADT/OwningPtr.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Bitcode/ReaderWriter.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/ExecutionEngine/JIT.h>
#include <czmq.h>
#include <string>
#include "zutil.hpp"

class YakLLVMContext {
public:
    YakLLVMContext();
private:
    LLVMContext* context;
};

class LLVMWorkerThread {
public:
    LLVMWorkerThread(zctx_t* ctx,
                     const std::string& endpoint,
                     Module* m);
    ~LLVMWorkerThread();
    void workerMain();
    std::thread* getThread();
private:
    /**
     * Pull socket to receive work chunks
     */
    void* inSocket;
    /**
     * Socket to the main router
     */
    void* outSocket;
    Module* module;
    zctx_t* ctx;
    std::thread* thread;
}

#endif //LLVM_WORKER_SPAWNER_HPP