#include <fstream>
#include "TestUtils.h"

std::string RandomGenerator::getRandomString(const int &len, bool nums, bool specs) {
    std::string chars(
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    );

    std::string w_nums("1234567890");
    w_nums.append(chars);
    std::string w_specs(
            "!@#$%^&*()"
            "`~-_=+[{]{\\|;:'\",<.>/? "
    );
    w_specs.append(w_nums);

    boost::random::random_device rng;
    std::string str;

    boost::random::uniform_int_distribution<> index_dist(0, chars.size() - 1);
    for (int i = 0; i < len; ++i) {
        if (specs) {
            str += w_specs[index_dist(rng)];
        } else if (nums) {
            str += w_nums[index_dist(rng)];
        } else {
            str += chars[index_dist(rng)];
        }
    }
    return str;
}

int32_t RandomGenerator::getRandomInt(int32_t min, int32_t max) {
    boost::random::random_device rng;
    boost::random::uniform_int_distribution<> random(0, max);
    return random(rng);
}

void mem_usage(double &vm_usage, double &resident_set) {
    vm_usage = 0.0;
    resident_set = 0.0;
    std::ifstream stat_stream("/proc/self/stat", std::ios_base::in); //get info from proc
    //directory
    //create some variables to get info
    std::string pid, comm, state, ppid, pgrp, session, tty_nr;
    std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    std::string utime, stime, cutime, cstime, priority, nice;
    std::string O, itrealvalue, starttime;
    unsigned long vsize;
    long rss;
    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
                >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
                >> utime >> stime >> cutime >> cstime >> priority >> nice
                >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care
    //about the rest
    stat_stream.close();
    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // for x86-64 is configured
    //to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
}

std::string findCollision(const std::function<uint32_t(const char *, uint32_t)> &hasher,
                          const std::string &key, uint32_t max){
    while(true){
        auto hash = hasher(key.c_str(), key.size()) % max;
        auto rand_key = RandomGenerator::getRandomString(8);
        if(hash == hasher(rand_key.c_str(), key.size()) % max){
            return rand_key;
        }
    }
}