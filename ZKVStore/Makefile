all:
	g++ -o zkv -g -lprotobuf -lleveldb -lzmq -lboost_filesystem -lboost_system -lczmq -std=c++11 main.cpp ../protobuf/KVDB.pb.cc TableOpenHelper.cpp
llvm:
	clang++ -stdlib=libc++ -std=c++11 -o zkv.bc main.cpp ../protobuf/KVDB.pb.cc -lprotobuf -lleveldb -lzmq -lczmq -lc++ 