#!/bin/sh
for file in *.proto ; do protoc --cpp_out=. $file ; done
