#include "LLVMWorkerSpawner.hpp"

/**
 * HEAVY WIP TODO FIXME !!!!
 * 
 * Do not even try to link this into the main application yet!
 */
/**
 * We want to avoid including a shitload of LLVM headers from anywhere else.
 */
struct StateWorker {
    LLVMContext* context;
}

/**
 * Initialize LLVM. Call this ONCE (only once!).
 * 
 * This **might** be slow (no benchmarks have been performed yet).
 * --> It might be wise not to call it on startup.
 */
void llvmInit() {
     InitializeNativeTarget();
     Fllvm_start_multithreaded();
}

int threadFun(Module* m, int id) {
    cout << "Starting thread " << id << endl;
    ExecutionEngine *ee = ExecutionEngine::create(m);
    /*for(Function& f : *m) {
        cout << f.getName().str() << endl;
    }*/
    Function* func = m->getFunction("fun");
    assert(func);
    typedef void (*PFN)(int id);
    PFN pfn = reinterpret_cast<PFN>(ee->getPointerToFunction(func));
    pfn(id);
    delete ee;
}
 
int main(int argc, char** argv) {
    InitializeNativeTarget();
    llvm_start_multithreaded();
    LLVMContext context;
    string error;
    OwningPtr<MemoryBuffer> memBuf;
    MemoryBuffer::getFile(std::string(argv[1]), memBuf);
    Module *m = ParseBitcodeFile(memBuf.get(), context, &error);
    //Create a thread every few seconds
    thread* thd;
    for(int id = 0; id < 10; id++) {
        thd = new std::thread(threadFun, m, id);
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
    //wait
    thd->join();
}