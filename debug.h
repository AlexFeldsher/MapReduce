//
// Created by alex on 5/21/17.
//

#ifndef MAPREDUCE2_DEBUG_H
#define MAPREDUCE2_DEBUG_H

#ifndef NDEBUG
	#include <iostream>
	#define DEBUG(x)  do {\
						std::cerr << __FILE__ << "::" << __LINE__ << "::" << __func__ << "::" << pthread_self() << ": " << x << std::endl;\
						} while(0);\

#else
	#define DEBUG(x) (void(0))
#endif

#endif //MAPREDUCE2_DEBUG_H
