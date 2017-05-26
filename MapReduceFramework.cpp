#include <pthread.h>
#include <cstdlib>
#include <sys/time.h>
#include <string>
#include <utility>
#include <fstream>
#include <iostream>
#include <sstream>
#include <ctime>
#include <map>
#include <vector>
#include <semaphore.h>
#include <algorithm>
#include "MapReduceFramework.h"

/**
 * implementation of less class for use in map with k2Base pointers as keys
 */
namespace std {
	template<>
	struct less<k2Base*> {
		bool operator()(k2Base* const& lhs, k2Base* const& rhs) const {
			return (*lhs) < (*rhs);
		}
	};
}

//--------------------------------------- Definitions ----------------------------------------------
/**
 * The "chunk" size of data each ExecMap/ExecReduce thread takes from a data structure to process
 */
#define CHUNK 10
/**
 * Log file path
 */
#define LOG_FILE ".MapReduceFramework.log"

/**
 * Time format to be printed in the log file
 */
#define TIME_FORMAT "[%d.%m.%Y %H:%M:%S]"

/**
 * Buffer size for the time character array
 */
#define TIME_BUFFER 22

/**
 * Convert seconds to nanoseconds
 */
#define SEC_TO_NS(x) ((x) * 1000000000)

/**
 * Convert microseconds to nanoseconds
 */
#define USEC_TO_NS(x) ((x) * 1000)

//------------------------------------ Global Variables ------------------------------------------
/**
 * The thread level of map reduce framework
 */
int gMultiThreadLevel;

/**
 * pointer to items vector given to runMapReduceFramework as an argument
 */
IN_ITEMS_VEC* gInItemsVec;

/**
 * pointer to map reduce object given to runMapReduceFramework as an argument
 */
MapReduceBase* gMapReduce;

/**
 * log file stream
 */
std::ofstream logFile;

/**
 * Counter of ExecMap threads that were terminated
 */
int nTermMapThreads = 0;

/**
 * Used to measure the time when the map reduce framework started
 */
struct timeval mapStartTime;

/**
 * Used to measure the time the map and shuffle process ended
 */
struct timeval mapEndTime;

/**
 * Used to measure the time the reduce process ended
 */
struct timeval reduceEndTime;

//-------------------------------------- Data structures --------------------------------------------------
/**
 * Holds the data sent to Emit2
 */
typedef std::pair<k2Base*, v2Base*>* EMIT2_PAIR;
std::map<pthread_t, std::vector<EMIT2_PAIR>*> emit2Data;

/**
 * Holds the data sent to Emit3
 */
typedef std::pair<k3Base*, v3Base*>* EMIT3_PAIR;
std::map<pthread_t, std::vector<EMIT3_PAIR>*> emit3Data;

/**
 * Shuffle data structure
 */
std::map<k2Base*, std::vector<v2Base*>> shuffleData;

/**
 * vector of map threads
 */
std::vector<pthread_t> mapThreads;

//----------------------------------------- mutex ---------------------------------------------------------
/**
 * used to lock the log output
 */
pthread_mutex_t mut_log = PTHREAD_MUTEX_INITIALIZER;

/**
 * used to lock the database index in ExecMap and ExecReduce threads
 */
pthread_mutex_t mut_index = PTHREAD_MUTEX_INITIALIZER;

/**
 * used to lock access to the number of terminated map threads counter
 */
pthread_mutex_t mut_counter = PTHREAD_MUTEX_INITIALIZER;

/**
 * used to lock access to emit2Data during its initialization
 */
pthread_mutex_t mut_emitData = PTHREAD_MUTEX_INITIALIZER;

/**
 * used to lock getTime function
 */
pthread_mutex_t mut_time = PTHREAD_MUTEX_INITIALIZER;

/**
 * condition variable
 */
pthread_cond_t cv = PTHREAD_COND_INITIALIZER;

// ----------------------------------------- semaphores --------------------------------------------------
/**
 * Used by Emit2 to notify Shuffle that new data is available to shuffle
 */
sem_t sem_shuffle;
/**
 * Used to block Emit2 access to emit2Data while shuffle is using it
 */
sem_t sem_shuffleDone;

//------------------------------------- function declarations --------------------------------------------

static bool OUT_ITEMS_COMP(OUT_ITEM const& rhs, OUT_ITEM const& lhs);
static void failure(int retVal, std::string functionName);
static void log(std::string msg);
static void* ExecMap(void* p);
static void* Shuffle(void* p);
static void* ExecReduce(void* p);
static std::string getTime();
static std::string elapsedTime(const struct timeval &start, const struct timeval &end);
static void freeEmit2Data(bool autoDeleteV2K2);
static void freeEmit3Data();

static void _gettimeofday(struct timeval *time);
static void _pthread_mutex_lock(pthread_mutex_t *mutex);
static void _pthread_mutex_unlock(pthread_mutex_t *mutex);
static void _sem_init(sem_t *sem, unsigned int value);
static void _pthread_create(pthread_t *thread, void *(*start_routine)(void *));
static void _pthread_join(pthread_t thread);
static void _pthread_mutex_destroy(pthread_mutex_t *mutex);
static void _sem_destroy(sem_t *sem);
static void _pthread_cond_destroy(pthread_cond_t *cond);
static void _sem_wait(sem_t *sem);
static void _sem_post(sem_t *sem);
static void _pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex);
static void _sem_getvalue(sem_t *sem, int *sval);
static void _pthread_cancel(pthread_t thread);
//---------------------------------------------------------------------------------------------------


OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
									int multiThreadLevel, bool autoDeleteV2K2)
{
	/// get map start time
	_gettimeofday(&mapStartTime);

	// save data in global variables
	gInItemsVec = &itemsVec;
	gMapReduce = &mapReduce;
	gMultiThreadLevel = multiThreadLevel;

	// open log file
	logFile.open(LOG_FILE, std::ios::out | std::ios::app);
	failure(logFile.fail(), "open");

	// log
	_pthread_mutex_lock(&mut_log);
	std::stringstream ss;
	ss << "RunMapReduceFramework started with " << multiThreadLevel << " threads";
	log(ss.str());
	_pthread_mutex_unlock(&mut_log);

	// initialized shuffle semaphore
	_sem_init(&sem_shuffle, 0);
	// initialize shuffle done semaphore
	_sem_init(&sem_shuffleDone, 1);

	// initialize threads data structures
	mapThreads = std::vector<pthread_t>(multiThreadLevel);
	std::vector<pthread_t> reduceThreads = std::vector<pthread_t>(multiThreadLevel);
	pthread_t shuffle;

	// create ExecMap threads and initialize their data structures
	for (pthread_t &thread : mapThreads)
	{
		_pthread_mutex_lock(&mut_emitData);
		_pthread_create(&thread, &ExecMap);
		emit2Data[thread] = new std::vector<EMIT2_PAIR>;	// initialize thread data structure
		_pthread_mutex_unlock(&mut_emitData);
	}

	// create Shuffle thread
	_pthread_create(&shuffle, &Shuffle);

	// wait for shuffle to end, run shuffle as long as there are values to shuffle or not all the map
	// threads terminated
	_pthread_mutex_lock(&mut_counter);
	int sval = 0;
	while (nTermMapThreads < gMultiThreadLevel || sval > 0)
	{
		_pthread_cond_wait(&cv, &mut_counter);
		_sem_getvalue(&sem_shuffle, &sval);
	}
	_pthread_mutex_unlock(&mut_counter);

	// cancel shuffle thread
	_pthread_cancel(shuffle);
	// join remaining threads to free thread data
	_pthread_join(shuffle);
	for (pthread_t& thread : mapThreads)
		_pthread_join(thread);

	// log
	_pthread_mutex_lock(&mut_log);
	std::stringstream msg;
	msg << "Thread Shuffle terminated " << getTime();
	log(msg.str());

	msg.str(std::string());
	_gettimeofday(&mapEndTime);
	msg << "Map and Shuffle took " << elapsedTime(mapStartTime, mapEndTime) << "ns";
	log(msg.str());
	_pthread_mutex_unlock(&mut_log);

	// create ExecReduce threads
	for (pthread_t &thread : reduceThreads)
	{
		_pthread_mutex_lock(&mut_emitData);
		_pthread_create(&thread, &ExecReduce);
		emit3Data[thread] = new std::vector<EMIT3_PAIR>;
		_pthread_mutex_unlock(&mut_emitData);
	}

	// wait for reduce threads to terminate before continuing
	for (pthread_t &thread : reduceThreads)
		_pthread_join(thread);

	// log
	_pthread_mutex_lock(&mut_log);
	msg.str(std::string());
	_gettimeofday(&reduceEndTime);
	msg << "Reduce took " << elapsedTime(mapEndTime, reduceEndTime) << "ns";
	log(msg.str());
	_pthread_mutex_unlock(&mut_log);

	// destroy mutexes
	_pthread_mutex_destroy(&mut_log);
	_pthread_mutex_destroy(&mut_index);
	_pthread_mutex_destroy(&mut_counter);
	_pthread_mutex_destroy(&mut_emitData);
	_pthread_mutex_destroy(&mut_time);

	// destroy cond
	_pthread_cond_destroy(&cv);

	// destroy semaphores
	_sem_destroy(&sem_shuffle);
	_sem_destroy(&sem_shuffleDone);

	logFile.close();

	// create out items vector
	OUT_ITEMS_VEC reduceData;
	for (auto &item : emit3Data)
		for (auto &pair : *item.second)
			reduceData.push_back(*pair);

	std::sort(reduceData.begin(), reduceData.end(), OUT_ITEMS_COMP);

	// free data
	freeEmit2Data(autoDeleteV2K2);
	freeEmit3Data();

	return reduceData;
}

/**
 * Puts given key value pair in the emit2 data structure
 * @param key pointer to a key object
 * @param value pointer to a value object
 */
void Emit2(k2Base* key, v2Base* value)
{
	static int ret;
	pthread_t t = pthread_self();

	// block until shuffle is done with the data structure
	_sem_wait(&sem_shuffleDone);

	emit2Data[pthread_self()]->push_back(new std::pair<k2Base*, v2Base*>(key, value));

	// notify shuffle new data is available
	_sem_post(&sem_shuffle);
}

/**
 * Puts the given key-value pair in the reduce data structure
 * @param key a key object
 * @param value a value object
 */
void Emit3(k3Base* key, v3Base* value)
{
	emit3Data[pthread_self()]->push_back(new std::pair<k3Base*, v3Base*>(key, value));
}

/**
 * Thread function to execute the map method
 * @param p ignored parameter, needed for pthread_init argument signature
 * @return always returns nullptr
 */
static void* ExecMap(void* p)
{
	static size_t i = 0;
	int ret;

	_pthread_mutex_lock(&mut_emitData);
	_pthread_mutex_unlock(&mut_emitData);

	//emit2Data[pthread_self()] = (std::vector<EMIT2_PAIR>*)p;

	_pthread_mutex_lock(&mut_log);

	std::stringstream msg;
	msg << "Thread ExecMap created " << getTime();
	log(msg.str());
	msg.str(std::string());

	_pthread_mutex_unlock(&mut_log);

	while (true)
	{
		// lock
		_pthread_mutex_lock(&mut_index);

		// check that i isn't out of bounds
		if (i >= gInItemsVec->size())
		{
			_pthread_mutex_unlock(&mut_index);
			break;
		}

		// get iterator and increment index
		auto iter = gInItemsVec->begin() + i;
		i = i + CHUNK;

		// unlock
		_pthread_mutex_unlock(&mut_index);

		// perform the map function
		for (int j = 0; j < CHUNK && iter != gInItemsVec->end(); ++iter, ++j)
			gMapReduce->Map((*iter).first, (*iter).second);
	}

	_pthread_mutex_lock(&mut_log);

	msg << "Thread ExecMap terminated " << getTime();
	log(msg.str());

	_pthread_mutex_unlock(&mut_log);

	_pthread_mutex_lock(&mut_counter);
	nTermMapThreads++;
	pthread_cond_signal(&cv);
	_pthread_mutex_unlock(&mut_counter);

	pthread_exit(nullptr);
}

/**
 * Shuffle the map data
 * @param p ignored parameter, needed for pthread_init argument signature
 * @return always returns null
 */
static void* Shuffle(void* p)
{
	int ret;
	_pthread_mutex_lock(&mut_log);

	std::stringstream msg;
	msg << "Thread Shuffle created " << getTime();
	log(msg.str());
	msg.str(std::string());

	_pthread_mutex_unlock(&mut_log);

	std::map<pthread_t, int> threadIndexMap;
	for (auto &thread : mapThreads)
		threadIndexMap[thread] = 0;

	while (true) {
		_sem_wait(&sem_shuffle);

		for (auto &item : emit2Data)
		{
			if (item.second->empty())
				continue;


			auto iter = item.second->begin();
			if (iter == item.second->end())
				continue;

			shuffleData[(*iter)->first].push_back((*iter)->second);
			(item.second)->erase(iter);
			_sem_post(&sem_shuffleDone);
			break;
		}
		pthread_cond_signal(&cv);
	}
}

/**
 * Reduce the shuffle data
 * @param p ignored parameter, needed for pthread_init argument signature
 * @return always returns null
 */
static void* ExecReduce(void* p)
{
	static size_t i = 0;
	int ret;
	size_t j;

	_pthread_mutex_lock(&mut_emitData);

	_pthread_mutex_unlock(&mut_emitData);

	_pthread_mutex_lock(&mut_log);

	std::stringstream msg;
	msg << "Thread ExecReduce created " << getTime();
	log(msg.str());
	msg.str(std::string());

	_pthread_mutex_unlock(&mut_log);

	while (true)
	{
		// lock
		_pthread_mutex_lock(&mut_index);

		if (i >= shuffleData.size())
		{

			_pthread_mutex_unlock(&mut_index);

			break;
		}

		// get iterator and increment index
		auto iter = shuffleData.begin();
		for(j = 0; j < i; ++iter, ++j)	// increment in a loop because iter is bidirectional iterator
			;
		i = i + CHUNK;

		// unlock
		_pthread_mutex_unlock(&mut_index);

		// perform the map function
		for (j = 0; j < CHUNK && iter != shuffleData.end(); ++iter, ++j)
			gMapReduce->Reduce((*iter).first, (*iter).second);
	}

	_pthread_mutex_lock(&mut_log);

	msg << "Thread ExecReduce terminated " << getTime();
	log(msg.str());

	_pthread_mutex_unlock(&mut_log);

	pthread_exit(nullptr);
}

/**
 * Exit program if the given return value indicates a failure
 * 0 = success, otherwise failure
 * @param retVal return value of a function
 * @param functionName the name of the function the return value belongs to
 */
static void failure(int retVal, std::string functionName)
{
	if (retVal == 0)
		return;

	_pthread_mutex_lock(&mut_log);
	std::cerr << "MapReduceFramework Failure: " << functionName << " failed.";
	_pthread_mutex_unlock(&mut_log);
	exit(1);
}

/**
 * Print given message in the log file
 * @param msg the message to print
 */
static void log(std::string msg)
{
	logFile << msg << std::endl;
}

/**
 * Returns the local time in the following format
 * [DD.MM.YYYY HH:MM:SS]
 * @return string of the current data and time
 */
static std::string getTime()
{
	_pthread_mutex_lock(&mut_time);
	int ret;

	std::time_t rawTime = std::time(nullptr);
	failure(rawTime == (std::time_t)(-1), "std::time");
	std::tm* localTime = std::localtime(&rawTime);
	failure(localTime == NULL, "std::localtime");
	char buffer[TIME_BUFFER];

	ret = std::strftime(buffer, sizeof(buffer), TIME_FORMAT, localTime);
	failure(!ret, "std::strftime");

	_pthread_mutex_unlock(&mut_time);

	return std::string(buffer);
}

/**
 * Out items vector comperator
 * @param rhs OUT_ITEM object
 * @param lhs OUT_ITEM object
 * @return true if rhs.first < lhs.first, otherwise false
 */
static bool OUT_ITEMS_COMP(OUT_ITEM const& rhs, OUT_ITEM const& lhs)
{
	return (*(rhs.first) < *(lhs.first));
}

/**
 * Calculates and retuns the difference of time in nanoseconds between 2 given timeval structs
 * @param start start time
 * @param end end time
 * @return the difference between the start and end time in nanoseconds
 */
static std::string elapsedTime(const struct timeval &start, const struct timeval &end)
{
	long long sec = SEC_TO_NS(end.tv_sec - start.tv_sec);
	long long usec = USEC_TO_NS(end.tv_usec - start.tv_usec);

	std::stringstream elapsedTime;
	elapsedTime << sec + usec;

	return elapsedTime.str();
}


/**
 * Wraps gettimeifday function for error handling
 * @param time pointer to timeval struct
 */
static void _gettimeofday(struct timeval *time)
{
	int ret = gettimeofday(time, nullptr);
	failure(ret, "gettimeofday");
}

/**
 * Wraps pthread_mutex_lock for error handling
 * @param mutex pointer to a mutex
 */
static void _pthread_mutex_lock(pthread_mutex_t *mutex)
{
	int ret = pthread_mutex_lock(mutex);
	failure(ret, "pthread_mutex_lock");
}

/**
 * Wraps pthread_mutex_lock for error handling
 * @param mutex pointer to a mutex
 */
static void _pthread_mutex_unlock(pthread_mutex_t *mutex)
{
	int ret = pthread_mutex_unlock(mutex);
	failure(ret, "pthread_mutex_unlock");
}

/**
 * Wraps sem_init for error handling
 * @param sem pointer to a semaphore
 * @param value the initial value of the semaphore
 */
static void _sem_init(sem_t *sem, unsigned int value)
{
	int ret = sem_init(sem, 0, value);
	failure(ret, "sem_init");
}

/**
 * Wraps pthread_create for error handling
 * @param thread pointer to a pthread_t object
 * @param start_routine the thread start  execution by invoking start_routine
 */
static void _pthread_create(pthread_t *thread, void *(*start_routine)(void *))
{
	int ret = pthread_create(thread, nullptr, start_routine, nullptr);
	failure(ret, "pthread_create");
}

/**
 * Wraps pthread_join for error handling
 * @param thread thread to join with
 */
static void _pthread_join(pthread_t thread)
{
	int ret = pthread_join(thread, nullptr);
	failure(ret, "pthread_join");
}

/**
 * Wraps pthread_mutex_destroy for error handling
 * @param mutex mutex to destroy
 */
static void _pthread_mutex_destroy(pthread_mutex_t *mutex)
{
	int ret = pthread_mutex_destroy(mutex);
	failure(ret, "pthread_mutex_destroy");
}

/**
 * Wraps pthread_cond_destroy for error handling
 * @param cond condition object to destroy
 */
static void _pthread_cond_destroy(pthread_cond_t *cond)
{
	int ret = pthread_cond_destroy(cond);
	failure(ret, "pthread_cond_destroy");
}

/**
 * Wraps sem_destroy for error handling
 * @param sem semaphore to destroy
 */
static void _sem_destroy(sem_t *sem)
{
	int ret = sem_destroy(sem);
	failure(ret, "sem_destroy");
}

/**
 * Wraps sem_wait for error handling
 * @param sem semaphore to wait for
 */
static void _sem_wait(sem_t *sem)
{
	int ret = sem_wait(sem);
	failure(ret, "sem_wait");
}

/**
 * Wraps sem_post for error handling
 * @param sem semaphore to post
 */
static void _sem_post(sem_t *sem)
{
	int ret = sem_post(sem);
	failure(ret, "sem_post");
}

/**
 * Wraps pthread_cond_wait for error handling
 * @param cond pthread condition object
 * @param mutex mutex object
 */
static void _pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
	int ret = pthread_cond_wait(cond, mutex);
	failure(ret, "pthread_cond_wait");
}

/**
 * Wraps sem_getvalue for error handling
 * @param sem semaphore object
 * @param sval pointer to an integer
 */
static void _sem_getvalue(sem_t *sem, int *sval)
{
	int ret = sem_getvalue(sem, sval);
	failure(ret, "sem_getvalue");
}

/**
 * Wraps pthread_cancel for error handling
 * @param thread thread to cancel
 */
static void _pthread_cancel(pthread_t thread)
{
	int ret = pthread_cancel(thread);
	failure(ret, "pthread_cancel");
}

/**
 * Free data allocated for emit2Data
 * @param autoDeleteV2K2 if true delete internal pair elementes
 */
static void freeEmit2Data(bool autoDeleteV2K2)
{
	for (auto elem : emit2Data)
	{
		for (auto pair : *elem.second)
		{
			if (autoDeleteV2K2)
			{
				delete pair->first;
				delete pair->second;
			}
		}
		delete elem.second;
	}

}

/**
 * Free data allocated for emit3Data
 */
static void freeEmit3Data()
{
	for (auto &elem : emit3Data) {
		for (auto &pair : *elem.second)
			delete pair;

		delete elem.second;
	}
}
