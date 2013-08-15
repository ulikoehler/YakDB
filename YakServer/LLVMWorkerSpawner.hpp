#ifndef LLVM_WORKER_SPAWNER_HPP
#define LLVM_WORKER_SPAWNER_HPP

class YakLLVMContext {
public:
    YakLLVMContext();
private:
    void* state;
}

#endif //LLVM_WORKER_SPAWNER_HPP