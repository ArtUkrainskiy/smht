#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <boost/random/random_device.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include "gtest/gtest.h"
#include <ctime>

#define LOG_INFO  std::cout << "\033[1;34m[   INFO   ]\033[0m "
#define LOG_WARN  std::cout << "\033[1;33m[   WARN   ]\033[0m "
#define LOG_ERROR std::cout << "\033[1;31m[   ERROR  ]\033[0m "
#define NL std::endl


void mem_usage(double &vm_usage, double &resident_set);

class RandomGenerator{
public:
    static std::string getRandomString(const int &len, bool nums = false, bool specs = false);
    static int32_t getRandomInt(int32_t min, int32_t max);
};

class TimeProfiler {
public:
    void start() {
        clock_gettime(CLOCK_REALTIME, &begin);
    }

    double get() {
        clock_gettime(CLOCK_REALTIME, &end);
        long seconds = end.tv_sec - begin.tv_sec;
        long nanoseconds = end.tv_nsec - begin.tv_nsec;
        double elapsed = (double)seconds + (double)nanoseconds * 1e-9;
        return elapsed;
    }

protected:
    struct timespec begin{}, end{};

};

std::string findCollision(const std::function<uint32_t(const char *, uint32_t)> &hasher,
                          const std::string &key, uint32_t max);




#endif