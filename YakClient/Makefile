all:
	g++ -o kvc -std=c++0x main.cpp ../ZKVStore/client.cpp  -lleveldb -lzmq -lczmq 
llvm:
	clang++ -stdlib=libc++ -std=c++11 -o zkv.bc main.cpp ../protobuf/KVDB.pb.cc -lprotobuf -lleveldb -lzmq -lczmq -lc++ 