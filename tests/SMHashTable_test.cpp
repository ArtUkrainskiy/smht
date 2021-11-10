#include <random>
#include "TestUtils.h"
#include "../SMHashTable.h"


class SMHashTable_test : public ::testing::Test {
protected:
    void SetUp() override {
        table = new SMHashTable("shared_memory", 10000, 40000, 8);
        table->clear();
        LOG_INFO << "Free memory: " << table->getFreeMemorySize() << NL;

    }

    void printMemInfo() {
        auto meminfo = table->memInfo();
        LOG_INFO << "Free memory: " << meminfo->free << NL;
        LOG_INFO << "Memory segments: " << meminfo->segments << NL;
        LOG_INFO << "Memory max alloc block: " << meminfo->max_allocated_block << NL;
        LOG_INFO << "Memory max free block: " << meminfo->max_free_block << NL;

    }

    SMHashTable *table{};
};

TEST(SPEED, first) {
    auto *timer = new TimeProfiler();
    timer->start();
    auto table = new SMHashTable("shared_memory", 1000, 4000, 8);

    LOG_INFO << "speed - " << timer->get() << "s" << NL;
}

TEST_F(SMHashTable_test, crud) {
    auto key = RandomGenerator::getRandomString(6);
    auto value = RandomGenerator::getRandomString(6);
    auto memory = table->getFreeMemorySize();

    table->set(key, value);
    ASSERT_STREQ(value.c_str(), table->get_value(key));
    ASSERT_EQ(memory - 24, table->getFreeMemorySize());


    table->set(RandomGenerator::getRandomString(6), RandomGenerator::getRandomString(6));
    ASSERT_EQ(memory - 24 - 24, table->getFreeMemorySize());


    auto new_value = RandomGenerator::getRandomString(15);
    table->set(key, new_value);
    ASSERT_STREQ(new_value.c_str(), table->get_value(key));
    ASSERT_EQ(memory - 24 - 24 - 8, table->getFreeMemorySize());


    new_value = RandomGenerator::getRandomString(6);
    table->set(key, new_value);
    ASSERT_STREQ(new_value.c_str(), table->get_value(key));
    ASSERT_EQ(memory - 24 - 24, table->getFreeMemorySize());


    table->unset(key);
    ASSERT_STREQ("", table->get_value(key));
    ASSERT_EQ(memory - 24, table->getFreeMemorySize());
}

TEST_F(SMHashTable_test, collision) {
    auto key = RandomGenerator::getRandomString(8);
    auto collision = findCollision(&meiyan, key, 1000);
    auto collision2 = findCollision(&meiyan, key, 1000);
    auto collision3 = findCollision(&meiyan, key, 1000);
    LOG_INFO << "Key - " << key << NL;
    LOG_INFO << "Collision - " << collision << NL;
    LOG_INFO << "Collision2 - " << collision2 << NL;
    LOG_INFO << "Collision3 - " << collision3 << NL;

    table->set(key, "key");
    table->set(collision, "collision");
    table->set(collision2, "collision2");
    table->set(collision3, "collision3");

    ASSERT_STREQ(table->get_value(key), "key");
    ASSERT_STREQ(table->get_value(collision), "collision");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    ASSERT_STREQ(table->get_value(collision3), "collision3");
}

TEST_F(SMHashTable_test, unset_single_item) {
    auto key = RandomGenerator::getRandomString(8);
    auto collision = findCollision(&meiyan, key, 1000);
    auto collision2 = findCollision(&meiyan, key, 1000);
    auto collision3 = findCollision(&meiyan, key, 1000);
    LOG_INFO << "Key - " << key << NL;
    LOG_INFO << "Collision - " << collision << NL;
    LOG_INFO << "Collision2 - " << collision2 << NL;
    LOG_INFO << "Collision3 - " << collision3 << NL;

    auto before_memory = table->getFreeMemorySize();
    table->set(key, "key");
    LOG_WARN << table->unset(key) << NL;
    ASSERT_EQ(before_memory, table->getFreeMemorySize());

    table->set(collision, "collision");
    table->set(collision2, "collision2");
    table->set(collision3, "collision3");

    ASSERT_STREQ(table->get_value(key), "");
    ASSERT_STREQ(table->get_value(collision), "collision");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    ASSERT_STREQ(table->get_value(collision3), "collision3");

}

TEST_F(SMHashTable_test, unset_last_linked_item) {
    auto key = RandomGenerator::getRandomString(8);
    auto collision = findCollision(&meiyan, key, 1000);
    auto collision2 = findCollision(&meiyan, key, 1000);
    auto collision3 = findCollision(&meiyan, key, 1000);
    LOG_INFO << "Key - " << key << NL;
    LOG_INFO << "Collision - " << collision << NL;
    LOG_INFO << "Collision2 - " << collision2 << NL;
    LOG_INFO << "Collision3 - " << collision3 << NL;

    table->set(key, "key");
    auto before_memory = table->getFreeMemorySize();
    table->set(collision, "collision");

    ASSERT_STREQ(table->get_value(key), "key");
    ASSERT_STREQ(table->get_value(collision), "collision");
    LOG_WARN << table->unset(collision) << NL;
    ASSERT_EQ(before_memory, table->getFreeMemorySize());

    table->set(collision2, "collision2");
    table->set(collision3, "collision3");

    ASSERT_STREQ(table->get_value(key), "key");
    ASSERT_STREQ(table->get_value(collision), "");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    ASSERT_STREQ(table->get_value(collision3), "collision3");

}

TEST_F(SMHashTable_test, unset_middle_linked_item) {
    auto key = RandomGenerator::getRandomString(8);
    auto collision = findCollision(&meiyan, key, 1000);
    auto collision2 = findCollision(&meiyan, key, 1000);
    auto collision3 = findCollision(&meiyan, key, 1000);
    LOG_INFO << "Key - " << key << NL;
    LOG_INFO << "Collision - " << collision << NL;
    LOG_INFO << "Collision2 - " << collision2 << NL;
    LOG_INFO << "Collision3 - " << collision3 << NL;

    table->set(key, "key");
    auto before_set = table->getFreeMemorySize();
    table->set(collision, "collision");
    auto after_set = table->getFreeMemorySize();
    table->set(collision2, "collision2");

    ASSERT_STREQ(table->get_value(key), "key");
    ASSERT_STREQ(table->get_value(collision), "collision");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    auto before_unset = table->getFreeMemorySize();
    LOG_WARN << table->unset(collision) << NL;
    auto after_unset = table->getFreeMemorySize();
    ASSERT_EQ(before_set - after_set, after_unset - before_unset);
    table->set(collision3, "collision3");
    ASSERT_STREQ(table->get_value(key), "key");
    ASSERT_STREQ(table->get_value(collision), "");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    ASSERT_STREQ(table->get_value(collision3), "collision3");

}

TEST_F(SMHashTable_test, unset_first_linked_item) {
    auto key = RandomGenerator::getRandomString(8);
    auto collision = findCollision(&meiyan, key, 1000);
    auto collision2 = findCollision(&meiyan, key, 1000);
    auto collision3 = findCollision(&meiyan, key, 1000);

    LOG_INFO << "Key - " << key << NL;
    LOG_INFO << "Collision - " << collision << NL;
    LOG_INFO << "Collision2 - " << collision2 << NL;
    LOG_INFO << "Collision3 - " << collision3 << NL;

    table->set(key, "key");
    table->set(collision, "collision");
    table->set(collision2, "collision2");

    ASSERT_STREQ(table->get_value(key), "key");
    ASSERT_STREQ(table->get_value(collision), "collision");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    LOG_WARN << table->unset(key) << NL;
    table->set(collision3, "collision3");
    ASSERT_STREQ(table->get_value(key), "");
    ASSERT_STREQ(table->get_value(collision), "collision");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    ASSERT_STREQ(table->get_value(collision3), "collision3");

}

TEST_F(SMHashTable_test, unset_chain) {
    auto key = RandomGenerator::getRandomString(8);
    auto collision = findCollision(&meiyan, key, 1000);
    auto collision2 = findCollision(&meiyan, key, 1000);
    auto collision3 = findCollision(&meiyan, key, 1000);

    LOG_INFO << "Key - " << key << NL;
    LOG_INFO << "Collision - " << collision << NL;
    LOG_INFO << "Collision2 - " << collision2 << NL;
    LOG_INFO << "Collision3 - " << collision3 << NL;

    auto before_set = table->getFreeMemorySize();
    table->set(key, "key");
    table->set(collision, "collision");
    table->set(collision2, "collision2");
    table->set(collision3, "collision3");

    LOG_WARN << table->unset(key) << NL;
    ASSERT_STREQ(table->get_value(collision), "collision");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    ASSERT_STREQ(table->get_value(collision3), "collision3");


    LOG_WARN << table->unset(collision) << NL;
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    ASSERT_STREQ(table->get_value(collision3), "collision3");

    LOG_WARN << table->unset(collision2) << NL;
    ASSERT_STREQ(table->get_value(collision3), "collision3");

    LOG_WARN << table->unset(collision3) << NL;
    auto after_unset = table->getFreeMemorySize();
    ASSERT_EQ(before_set, after_unset);

    table->set(collision, "collision");
    table->set(collision2, "collision2");
    table->set(collision3, "collision3");
    table->set(key, "key");
    ASSERT_STREQ(table->get_value(collision), "collision");
    ASSERT_STREQ(table->get_value(collision2), "collision2");
    ASSERT_STREQ(table->get_value(collision3), "collision3");
    ASSERT_STREQ(table->get_value(key), "key");
}


TEST_F(SMHashTable_test, unsets) {

    std::vector<std::pair<std::string, std::string>> keys;
    for (int i = 0; i < 1000; i++) {
        auto key = "key-" + RandomGenerator::getRandomString(16);
        auto val = RandomGenerator::getRandomString(32);
        if (table->set(key, val)) {
            keys.emplace_back(key, val);
        }
    }
    for (const auto &key: keys) {
        ASSERT_STREQ(key.second.c_str(), table->get_value(key.first));
    }

    uint32_t to_remove = keys.size() / 100 * 50;
    for (; to_remove > 0; to_remove--) {
        uint32_t index = RandomGenerator::getRandomInt(0, (int32_t) keys.size() - 1);
        table->unset(keys[index].first);
        keys.erase(keys.begin() + index);
    }
    for (const auto &key: keys) {
        ASSERT_STREQ(key.second.c_str(), table->get_value(key.first));
    }

    for (int i = 0; i < 1000; i++) {
        auto key = "key-" + RandomGenerator::getRandomString(16);
        auto val = RandomGenerator::getRandomString(32);
        if (table->set(key, val)) {
            keys.emplace_back(key, val);
        }
    }

    for (const auto &key: keys) {
        ASSERT_STREQ(key.second.c_str(), table->get_value(key.first));
    }

    to_remove = keys.size() / 100 * 25;
    for (; to_remove > 0; to_remove--) {
        uint32_t index = RandomGenerator::getRandomInt(0, (int32_t) keys.size() - 1);
        table->unset(keys[index].first);
        keys.erase(keys.begin() + index);
    }
    for (const auto &key: keys) {
        ASSERT_STREQ(key.second.c_str(), table->get_value(key.first));
    }

    for (int i = 0; i < 1000; i++) {
        auto key = "key-" + RandomGenerator::getRandomString(16);
        auto val = RandomGenerator::getRandomString(32);
        if (table->set(key, val)) {
            keys.emplace_back(key, val);
        }
    }

    for (const auto &key: keys) {
        ASSERT_STREQ(key.second.c_str(), table->get_value(key.first));
    }

}

TEST_F(SMHashTable_test, overflow) {
    uint32_t size = 1000;
    uint32_t fails{};
    for (uint32_t i = 0; i < size; i++) {
        if (!table->set(RandomGenerator::getRandomString(16), RandomGenerator::getRandomString(32))) {
            fails++;
        }
    }
    LOG_INFO << "Fails: " << fails << NL;
    printMemInfo();
}

TEST_F(SMHashTable_test, fragmentation) {
    uint32_t size = 1000;
    std::map<std::string, std::string> dataset;
    for (uint32_t i = 0; i < size; i++) {
        int strlen = RandomGenerator::getRandomInt(1, 65);
        dataset["key-" + RandomGenerator::getRandomString(16)] = RandomGenerator::getRandomString(strlen);
    }

    std::vector<std::string> keys;
    for (const auto &data: dataset) {
        if (table->set(data.first, data.second)) {
            keys.push_back(data.first);
        }
    }
    LOG_INFO << "Before unsets" << NL;
    printMemInfo();
    auto to_remove = (uint32_t) ((float) keys.size() / 100 * 50);
    for (; to_remove > 0; to_remove--) {
        uint32_t index = RandomGenerator::getRandomInt(0, (int32_t) keys.size() - 1);
        table->unset(keys[index]);
        keys.erase(keys.begin() + index);
    }

    LOG_WARN << "After unsets" << NL;
    printMemInfo();

    for (const auto &key: keys) {
        ASSERT_STREQ(dataset[key.c_str()].c_str(), table->get_value(key));
    }

    auto timer = new TimeProfiler;
    timer->start();
    table->hardDefragmentation();
    LOG_WARN << "TIME - " << timer->get() << "s" << NL;

    for (const auto &key: keys) {
        ASSERT_STREQ(dataset[key.c_str()].c_str(), table->get_value(key));
    }

    LOG_WARN << "After defragmentation" << NL;
    printMemInfo();

    std::vector<std::pair<std::string, std::string>> n_keys;
    for (int i = 0; i < 1000; i++) {
        auto key = "key-" + RandomGenerator::getRandomString(16);
        int strlen = RandomGenerator::getRandomInt(1, 65);
        auto val = RandomGenerator::getRandomString(strlen);
        if (table->set(key, val)) {
            n_keys.emplace_back(key, val);
        }
    }

    for (const auto &key: keys) {
        ASSERT_STREQ(dataset[key.c_str()].c_str(), table->get_value(key));
    }

}


TEST_F(SMHashTable_test, create_perfomance) {
    uint32_t size = 1000;
    std::map<std::string, std::string> dataset;
    for (uint32_t i = 0; i < size; i++) {
        int strlen = RandomGenerator::getRandomInt(8, 65);
        dataset["key-" + RandomGenerator::getRandomString(16)] = RandomGenerator::getRandomString(strlen);
    }
    LOG_INFO << "Write dataset with 1000 random strings from 8 to 65 characters long" << NL;
    auto fails = 0;

    auto timer = new TimeProfiler;
    timer->start();
    for (const auto &data: dataset) {
        table->set(data.first, data.second);
    }
    LOG_WARN << "INMEM - " << timer->get() << "s" << NL;

    std::map<std::string, std::string> map;
    timer->start();
    for (const auto &data: dataset) {
        map[data.first] = data.second;
    }
    ASSERT_EQ(fails, 0);
    LOG_WARN << "STD::MAP - " << timer->get() << "s" << NL;


    LOG_INFO << "Read dataset with 1000 random strings from 8 to 65 characters long" << NL;
    fails = 0;
    timer->start();
    for (const auto &data: dataset) {
        if (std::strcmp(table->get_value(data.first), data.second.c_str()) != 0) {
            fails++;
        }
    }
    ASSERT_EQ(fails, 0);
    LOG_WARN << "INMEM - " << timer->get() << "s" << NL;

    fails = 0;
    timer->start();
    for (const auto &data: dataset) {
        if (std::strcmp(map[data.first].c_str(), data.second.c_str()) != 0) {
            fails++;
        }
    }
    ASSERT_EQ(fails, 0);
    LOG_WARN << "STD::MAP - " << timer->get() << "s" << NL;


    LOG_INFO << "Update dataset with 1000 random strings from 8 to 65 characters long" << NL;
    std::map<std::string, std::string> new_dataset;
    for (uint32_t i = 0; i < size; i++) {
        int strlen = RandomGenerator::getRandomInt(8, 65);
        new_dataset["key-" + RandomGenerator::getRandomString(16)] = RandomGenerator::getRandomString(strlen);
    }

    fails = 0;
    timer->start();
    for (const auto &data: new_dataset) {
        table->set(data.first, data.second);
    }
    ASSERT_EQ(fails, 0);
    LOG_WARN << "INMEM - " << timer->get() << "s" << NL;

    fails = 0;
    timer->start();
    for (const auto &data: new_dataset) {
        map[data.first] = data.second;
    }
    ASSERT_EQ(fails, 0);
    LOG_WARN << "STD::MAP - " << timer->get() << "s" << NL;

}
