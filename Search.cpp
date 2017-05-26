#include <iostream>
#include <algorithm>
#include <dirent.h>
#include "Search.h"
#include "MapReduceFramework.h"

/**
 * @brief program usage message
 */
#define MSG_USAGE "Usage: <substring to search> <folders, separated by space>"

/**
 * The substring the program searches for
 */
std::string gSubString;

/**
 * Vector of k1Base*, v1Base pairs that are sent to the map reduce framework function
 */
IN_ITEMS_VEC inItemsVector;


/**
 * Map method
 * @param key
 * @param val
 */
void MapReduce::Map(const k1Base *const key, const v1Base *const val) const
{

	// open directory
	DIR* dirp = opendir(((Key1*)key)->key.c_str());
	struct dirent* dirstruct;

	// return if directory doesn't exists
	if (dirp == NULL)
		return;

	// iterate over directory files
	dirstruct = readdir(dirp);
	while (dirstruct != NULL)
	{

		std::string filename = dirstruct->d_name;
		Key2* key2 = new Key2(std::string(filename));
		Value2* value2;

		// search for substring in file names
		std::size_t found = (filename).find(gSubString);
		if (found != std::string::npos)
		{
			value2 = new Value2(1);
			Emit2(key2, value2);	// found substring
		}
		else
		{
			value2 = new Value2(0);
			Emit2(key2, value2);	// didn't find substring
		}
		// get next file
		dirstruct = readdir(dirp);
	}
	// close directory
	closedir(dirp);
}

/**
 * Reduce method
 * @param key
 * @param vals
 */
void MapReduce::Reduce(const k2Base *const key, const V2_VEC &vals) const
{
	int sum = 0;

	for (auto b = vals.begin(); b != vals.end(); ++b)
		sum += ((Value2*)(*b))->value;

	std::string filename = (((Key2*)key)->key);

	Key3* key3 = new Key3(std::string(filename));
	Value3* value3 = new Value3(sum);

	Emit3(key3, value3);
}

/**
 * Print the given vector
 * @param vec the vector to print
 */
static void printResult(OUT_ITEMS_VEC vec)
{
	for (const OUT_ITEM &p : vec)
	{
		std::string filename = ((Key3*)p.first)->key;
		int nTimesAppeared = ((Value3*)p.second)->value;
		for (int i = 0; i < nTimesAppeared; ++i)
			std::cout << filename << " ";
	}
}

/**
 * Free the objects in inItemsVector
 */
static void freeInItemsVec()
{
	for (auto &pair : inItemsVector)
	{
		delete ((Key1*) pair.first);
		delete ((Value1*) pair.second);
	}
}

/**
 * @brief Main function
 * @param argc number of arguments
 * @param argv[] command line arguments
 * @return 0 if successful otherwise 1
 */
int main(int argc, char* argv[])
{
	if (argc == 1)
	{
		std::cerr << MSG_USAGE << std::endl;
		return 1;
	}

	gSubString = argv[1];

	if (argc == 2)	// no folders specified
		return 0;

	// initialized the class containing the Map & Reduce methods
	MapReduce mapReduce;

	// create <folder, null> list
	for (int i = 2; i < argc; ++i)
	{
		k1Base* key = (k1Base*) new Key1(std::string(argv[i]));
		v1Base* value = (v1Base*) new Value1(nullptr);

		inItemsVector.push_back(std::make_pair(key, value));
	}

	int multiThreadLevel = argc - 2;

	OUT_ITEMS_VEC outItemsVector = RunMapReduceFramework(mapReduce, inItemsVector, multiThreadLevel, true);

	// print all the file names
	printResult(outItemsVector);

	freeInItemsVec();
	return 0;
}
