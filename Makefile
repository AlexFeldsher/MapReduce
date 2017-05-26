CC=g++
CPPFLAGS=-std=c++11
OUT=Search
LIB=MapReduceFramework.a

all: lib search
lib: MapReduceFramework.o
	ar rcs $(LIB) MapReduceFramework.o
MapReduceFramework.o: MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h
	$(CC) $(CPPFLAGS) -lpthread -c MapReduceFramework.cpp
search: Search.h Search.cpp MapReduceClient.h MapReduceFramework.h
	$(CC) $(CPPFLAGS) -lpthread Search.cpp $(LIB) -o $(OUT)
clean:
	rm -rf $(LIB) Search.o MapReduceFramework.o $(OUT)
.PHONY: search lib clean
