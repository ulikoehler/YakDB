all:
	g++ -o zkv -g -lprotobuf -std=c++0x main.cpp TableOpenHelper.cpp  -lleveldb -lzmq -lboost_filesystem -lboost_system -lczmq 
llvm:
	clang++ -stdlib=libc++ -std=c++11 -o zkv.bc main.cpp ../protobuf/KVDB.pb.cc -lprotobuf -lleveldb -lzmq -lczmq -lc++ 