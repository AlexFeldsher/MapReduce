//
// Created by alex on 5/21/17.
//

#ifndef MAPREDUCE2_SEARCH_H
#define MAPREDUCE2_SEARCH_H

#include <string>
#include "MapReduceClient.h"

/**
 * @brief Folder key class
 */
struct Key1: public k1Base
{
	std::string key;

	Key1(std::string folderName) : key(folderName) {}
	Key1(Key1 &key1) : key(key1.key) {}
	~Key1(){}

	virtual bool operator<(const k1Base &other) const
	{
		return key < ((Key1*)(&other))->key;
	}
};

struct Value1: public v1Base {
	Value1(void* ptr) {}
	Value1(Value1 &val1) {}

	~Value1() {}
};

struct Key2 : public k2Base
{
	std::string key;
	Key2(std::string filename) : key(filename) {}
	Key2(Key2 &key2) : key(key2.key) {}
	~Key2() {}
	virtual bool operator<(const k2Base &other) const
	{
		return key < ((Key2*)(&other))->key;
	}
};

struct Value2 : public v2Base
{
	int value;
	Value2(int v) : value(v) {}
	Value2(Value2 &val2) : value(val2.value) {}
	~Value2() {}
};

struct Key3 : public k3Base
{
	std::string key;
	Key3(std::string filename) : key(filename) {}
	Key3(Key3 &key3) : key(key3.key) {}
	~Key3() {}
	virtual bool operator<(const k3Base &other) const
	{
		return key < ((Key3*)(&other))->key;
	}
};

struct Value3 : public v3Base
{
	int value;
	Value3(int v) : value(v) {}
	Value3(Value3 &val3) : value(val3.value) {}
	~Value3() {}
};

/**
 * Class containing the Map and Reduce methods
 */
struct MapReduce : public MapReduceBase
{
	virtual void Map(const k1Base *const key, const v1Base *const val) const;
    virtual void Reduce(const k2Base *const key, const V2_VEC &vals) const;
};

#endif //MAPREDUCE2_SEARCH_H
