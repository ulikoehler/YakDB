all:
	g++ -o kvc -lprotobuf -lleveldb -lzmq -lczmq -std=c++11 main.cpp ../protobuf/KVDB.pb.cc
llvm:
	clang++ -stdlib=libc++ -std=c++11 -o zkv.bc main.cpp ../protobuf/KVDB.pb.cc -lprotobuf -lleveldb -lzmq -lczmq -lc++ 