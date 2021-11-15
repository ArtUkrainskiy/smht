#ifndef SMC_SMHASHTABLE_H
#define SMC_SMHASHTABLE_H


#define int_ceil_divide(x, y) ((x + y - 1) / y)

uint32_t SuperFastHash(const char *data, uint32_t len);

static inline uint32_t meiyan(const char *key, uint32_t count) {
    typedef uint32_t *P;
    uint32_t h = 0x811c9dc5;
    while (count >= 8) {
        h = (h ^ ((((*(P) key) << 5) | ((*(P) key) >> 27)) ^ *(P) (key + 4))) * 0xad3e7;
        count -= 8;
        key += 8;
    }
#define tmp h = (h ^ *(uint16_t*)key) * 0xad3e7; key += 2;
    if (count & 4) {
        tmp
        tmp
    }
    if (count & 2) { tmp }
    if (count & 1) { h = (h ^ *key) * 0xad3e7; }
#undef tmp
    return h ^ (h >> 16);
}


#define hash_method meiyan


class SMHashTable {
public:
    struct meminfo {
        uint32_t free{};
        uint32_t max_free_block{};
        uint32_t max_allocated_block{};
        uint32_t segments{};
    };

    explicit SMHashTable(std::string name, int key_count, int data_count, int data_block_size = 512);

    bool set(const std::string &key, const std::string &val);

    char *get_value(const std::string &key);

    int unset(const std::string &key);

    void clear();

    uint32_t getFreeMemorySize();

    uint32_t getLongestFreeBlockSize();

    uint32_t getLongestAllocatedBlockSize();

    struct meminfo *memInfo();

    void hardDefragmentation();

protected:
    struct header {
        void *key_offset{};
        uint32_t key_size{};

        void *val_offset{};
        uint32_t val_size{};

        void *linked_item{};
    };

    struct service {
        pthread_mutex_t memory_mutex;
    };

    inline struct header *get_header(const char *key, uint32_t size);

    inline void *find_memory_block(size_t size, uint32_t offset = 0);

    static inline void reserve_memory_block(void *addr, uint32_t size);

    static inline void free_memory_block(void *addr, uint32_t size);

private:

    static void *find_zero_sequence(void *from, void *to, uint32_t len);

    char eol{};

    size_t _key_count;
    size_t _data_count;
    size_t _data_block_size;
    uint32_t _memory_size;

    size_t _service_size;
    size_t _header_size;
    size_t _header_len;
    size_t _data_len;

    std::string _name;

    struct service *_service_ptr;
    void *_header_ptr;
    void *_memory_map_ptr;
    void *_data_ptr;

    int _mem_descriptor;

    meminfo meminfo{};

    struct header *findParent(struct header *child);

    int lock(pthread_mutex_t *mutex_ptr);

    int unlock(pthread_mutex_t *mutex_ptr);
};


#endif //SMC_SMHASHTABLE_H
