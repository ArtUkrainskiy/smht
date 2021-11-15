#include <vector>
#include <cstring>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


#include "SMHashTable.h"

SMHashTable::SMHashTable(std::string name, int key_count, int data_count, int data_block_size) :
        _key_count(key_count), _data_count(data_count), _data_block_size(data_block_size), _name(std::move(name)) {
    // https://man7.org/linux/man-pages/man3/shm_open.3.html
    bool created = false;
    _mem_descriptor = shm_open(_name.c_str(), O_RDWR, ALLPERMS);
    if (_mem_descriptor == -1) {
        _mem_descriptor = shm_open(_name.c_str(), O_RDWR | O_CREAT, ALLPERMS);
        created = true;
    }
    //расчет объема памяти
    _service_size = sizeof(pthread_mutex_t);
    _header_size = sizeof(struct header);
    _header_len = _header_size * key_count;
    _data_len = data_block_size * data_count;
    _memory_size = _header_len + data_count + _data_len;
    ftruncate(_mem_descriptor, _memory_size);

    _service_ptr =  (struct service *)mmap(nullptr, _memory_size, PROT_READ | PROT_WRITE, MAP_SHARED, _mem_descriptor, 0);
    //Указатель на начало памяти, тут хранятся ключи хеш таблицы
    _header_ptr = (char *) _service_ptr + _service_size;
    //карта распределения памяти
    _memory_map_ptr = (char *) _header_ptr + _header_len;
    //Сегмент с данными
    _data_ptr = (char *) _memory_map_ptr + data_count;

    if(created){
        auto *service = (struct service *)_service_ptr;
        pthread_mutexattr_t attr;

        if (pthread_mutexattr_init(&attr)) {
            std::cerr << errno << std::endl;
        }
        if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED)) {
            std::cerr << errno << std::endl;
        }
        if (pthread_mutex_init(&service->memory_mutex, &attr)) {
            std::cerr << errno << std::endl;
        }
    }
    unlock(&_service_ptr->memory_mutex);
}

bool SMHashTable::set(const std::string &key, const std::string &val) {
    uint32_t val_size = val.size() + 1; // +1 for zero byte
    uint32_t key_size = key.size() + 1;

    //адрес в хеш таблице
    struct header *header = get_header(key.c_str(), key.size());
    if (!header->val_offset) {
        //место в хеш таблице свободно, пишем
        uint32_t need_memory_blocks = int_ceil_divide((val_size + key_size + sizeof(void *)), _data_block_size);
        void *memory_block = find_memory_block(need_memory_blocks);
        if (memory_block == nullptr) {
            return false;
        }

        void *data_dimension = (char *) _data_ptr + (((long) memory_block - (long) _memory_map_ptr) * _data_block_size);
        void *key_dimension = (void *) ((long) data_dimension + sizeof(void *));
        void *val_dimension = (void *) ((long) key_dimension + key_size);

        header->key_offset = (void *) ((long) key_dimension - (long) _data_ptr);
        header->key_size = key_size;
        header->val_offset = (void *) ((long) val_dimension - (long) _data_ptr);
        header->val_size = val_size;
        header->linked_item = nullptr;

        ulong data_dimension_val = ((long) header - (long) _header_ptr);
        data_dimension_val |= 1UL << 63; //set last bit to 1
        *(uint64_t *) (data_dimension) = data_dimension_val;

        std::memcpy(key_dimension, key.c_str(), key_size);
        std::memcpy(val_dimension, val.c_str(), val_size);
    } else {
        //место в хеш таблице занято
        char *key_ptr = (char *) ((void *) ((long) header->key_offset + (long) _data_ptr));
        if (std::strcmp(key_ptr, key.c_str()) == 0) {
            //ключ существует, обновляем value
            uint32_t need_blocks_for_old_data = int_ceil_divide(
                    (header->val_size + header->key_size + sizeof(void *)), _data_block_size);
            uint32_t need_blocks_for_cur_data = int_ceil_divide(
                    (val_size + key_size + sizeof(void *)), _data_block_size);

            void *data_offset = (void *) ((((long) header->key_offset - sizeof(void *)) / _data_block_size) +
                                          (long) _memory_map_ptr);

            //TODO: dont rewrite all data if new block is smaller or equal
            if (need_blocks_for_old_data != need_blocks_for_cur_data) {
                //нужно перевыделить память
                //очищаем старую память
                free_memory_block((void *) data_offset, need_blocks_for_old_data);

                //занимаем новую память
                data_offset = find_memory_block(need_blocks_for_cur_data);
                if (data_offset == nullptr) {
                    return false;
                }

            }
            void *data_dimension =
                    (char *) _data_ptr + (((long) data_offset - (long) _memory_map_ptr) * _data_block_size);
            void *key_dimension = (void *) ((long) data_dimension + sizeof(void *));
            void *val_dimension = (void *) ((long) key_dimension + key_size);
            header->key_offset = (void *) ((long) key_dimension - (long) _data_ptr);
            header->val_offset = (void *) ((long) val_dimension - (long) _data_ptr);
            header->val_size = val_size;

            ulong data_dimension_val = ((long) header - (long) _header_ptr);
            data_dimension_val |= 1UL << 63; //set last bit to 1
            *(uint64_t *) (data_dimension) = data_dimension_val;

            std::memcpy(key_dimension, key.c_str(), key_size);
            std::memcpy(val_dimension, val.c_str(), val_size);
        } else {
            //коллизия, ключ не существует, пишем в связный список
            uint32_t need_blocks_for_header = int_ceil_divide(_header_size, _data_block_size);
            //нельзя чтобы блок попал в самое начало памяти, иначе linked_item будет 0
            void *header_memblock = find_memory_block(need_blocks_for_header, need_blocks_for_header);
            if (header_memblock == nullptr) {
                return false;
            }

            uint32_t need_memory_blocks = int_ceil_divide((val_size + key_size + sizeof(void *)), _data_block_size);
            void *memory_block = find_memory_block(need_memory_blocks);
            if (memory_block == nullptr) {
                //не нашли память под данные, освобождаем занятую память под заголовок
                free_memory_block(header_memblock, need_blocks_for_header);
                return false;
            }

            auto *new_header =
                    (struct header *) ((long) _data_ptr +
                                       (((long) header_memblock - (long) _memory_map_ptr) * _data_block_size));

            //бежим по цепочке пока не найдем крайний элемент, его и делаем активным
            while (header->linked_item) {
                header = (struct header *) ((long) header->linked_item + (long) _data_ptr);
            }
            header->linked_item = (void *) ((long) new_header - (long) _data_ptr);

            void *data_dimension =
                    (char *) _data_ptr + (((long) memory_block - (long) _memory_map_ptr) * _data_block_size);
            void *key_dimension = (void *) ((long) data_dimension + sizeof(void *));
            void *val_dimension = (void *) ((long) key_dimension + key_size);

            new_header->key_offset = (void *) ((long) key_dimension - (long) _data_ptr);
            new_header->key_size = key_size;
            new_header->val_offset = (void *) ((long) val_dimension - (long) _data_ptr);
            new_header->val_size = val_size;
            new_header->linked_item = nullptr;

            ulong data_dimension_val = ((long) new_header - (long) _header_ptr);
            data_dimension_val |= 1UL << 63;
            *(uint64_t *) (data_dimension) = data_dimension_val;

            std::memcpy(key_dimension, key.c_str(), key_size);
            std::memcpy(val_dimension, val.c_str(), val_size);
            return true;
        }
    }
    return true;
}

char *SMHashTable::get_value(const std::string &key) {
    auto *header = get_header(key.c_str(), key.size());
    if (header->val_offset) {
        //хеш существует
        if (std::strcmp(key.c_str(), (char *) ((void *) ((long) header->key_offset + (long) _data_ptr))) == 0) {
            //ключ верный, возвращаем
            return (char *) ((void *) ((long) header->val_offset + (long) _data_ptr));
        } else {
            //коллизия, ищем ключ
            while (header->linked_item) {
                header = (struct header *) ((long) header->linked_item + (long) _data_ptr);
                if (std::strcmp(key.c_str(), (char *) ((void *) ((long) header->key_offset + (long) _data_ptr))) ==
                    0) {
                    return (char *) ((void *) ((long) header->val_offset + (long) _data_ptr));
                }
            }
            return &eol;
        }
    } else {
        //хеша нет
        return &eol;
    }
}

int SMHashTable::unset(const std::string &key) {
    auto *header = get_header(key.c_str(), key.size());
    if (header->val_offset) {
        //хеш существует
        if (std::strcmp(key.c_str(), (char *) ((void *) ((long) header->key_offset + (long) _data_ptr))) == 0) {
            //ключ верный
            if (header->linked_item) {
                //есть связанные элементы
                //нужно переместить связанный на место текущего
                auto next_header = (struct header *) ((long) header->linked_item + (long) _data_ptr);
                uint32_t need_memory_blocks = int_ceil_divide(
                        (header->val_size + header->key_size + sizeof(void *)),
                        _data_block_size);

                void *current_data_offset = (void *) (
                        (((long) header->key_offset - sizeof(void *)) / _data_block_size) +
                        (long) _memory_map_ptr);
                void *next_header_offset = (void *) (
                        (((long) header->linked_item) / _data_block_size) +
                        (long) _memory_map_ptr);

                //освобождаем память под данные
                free_memory_block(current_data_offset, need_memory_blocks);
                free_memory_block(next_header_offset, int_ceil_divide(_header_size, _data_block_size));

                //в данных меняем смещение заголовка
                long *next_data = (long *) ((void *) ((long) next_header->key_offset - sizeof(void *) +
                                                      (long) _data_ptr));
                *next_data = *(long *) ((void *) ((long) header->key_offset - sizeof(void *) + (long) _data_ptr));

                //перемещаем связанный заголовок на место текущего
                std::memcpy(header, next_header, _header_size);
                return 1;
            } else {
                //одиночный элемент, самый простой вариант
                uint32_t need_memory_blocks = int_ceil_divide(
                        (header->val_size + header->key_size + sizeof(void *)),
                        _data_block_size);
                void *current_data_offset = (void *) (
                        (((long) header->key_offset - sizeof(void *)) / _data_block_size) +
                        (long) _memory_map_ptr);
                //освобождаем память под данные
                free_memory_block(current_data_offset, need_memory_blocks);
                //Чистим заголовок
                std::memset(header, 0, _header_size);
                return 2;
            }

        } else {
            //Элементы в связном списке
            //ищем нужный заголовок
            struct header *prev_header;
            while (header->linked_item) {
                prev_header = header;
                header = (struct header *) ((long) header->linked_item + (long) _data_ptr);
                if (std::strcmp(key.c_str(), (char *) ((void *) ((long) header->key_offset + (long) _data_ptr))) == 0) {
                    //нашли
                    if (header->linked_item) {
                        //есть связанные элементы
                        auto next_header = (struct header *) ((long) header->linked_item + (long) _data_ptr);
                        uint32_t need_memory_blocks = int_ceil_divide(
                                (header->val_size + header->key_size + sizeof(void *)),
                                _data_block_size);

                        void *current_data_offset = (void *) (
                                (((long) header->key_offset - sizeof(void *)) / _data_block_size) +
                                (long) _memory_map_ptr);
                        void *next_header_offset = (void *) (
                                (((long) header->linked_item) / _data_block_size) +
                                (long) _memory_map_ptr);

                        //освобождаем память под данные
                        free_memory_block(current_data_offset, need_memory_blocks);
                        //освобождаем память под заголовок
                        free_memory_block(next_header_offset, int_ceil_divide(_header_size, _data_block_size));
                        //в данных меняем смещение заголовка
                        long *next_data = (long *) ((void *) ((long) next_header->key_offset - sizeof(void *) +
                                                              (long) _data_ptr));
                        *next_data = *(long *) ((void *) ((long) header->key_offset - sizeof(void *) +
                                                          (long) _data_ptr));

                        //перемещаем связанный заголовок на место текущего
                        std::memcpy(header, next_header, _header_size);
                        return 3;
                    } else {
                        //Удаляем
                        uint32_t need_memory_blocks = int_ceil_divide(
                                (header->val_size + header->key_size + sizeof(void *)),
                                _data_block_size);

                        //вычисляем смещения занятой памяти в таблице
                        void *data_offset = (void *) (
                                (((long) header->key_offset - sizeof(void *)) / _data_block_size) +
                                (long) _memory_map_ptr);
                        void *header_offset = (void *) (
                                (((long) prev_header->linked_item) / _data_block_size) +
                                (long) _memory_map_ptr);

                        //освобождаем память под данные
                        free_memory_block(data_offset, need_memory_blocks);
                        //освобождаем память под заголовок
                        free_memory_block(header_offset, int_ceil_divide(_header_size, _data_block_size));

                        //чистим заголовок (пофиг, он в сегменте данных, они освобождены)
                        std::memset(header, 0, _header_size);
                        //удаляем из связного списка
                        prev_header->linked_item = nullptr;
                        return 4;
                    }
                }
            }
        }
    }

    return false;
}

void SMHashTable::clear() {
    std::memset(_header_ptr, 0, _memory_size);
}

uint32_t SMHashTable::getFreeMemorySize() {
    uint32_t counter = 0;
    for (auto i = (uint64_t) _memory_map_ptr; i < (uint64_t) _data_ptr; i++) {
        if (*(char *) i == 0) {
            counter++;
        }
    }
    meminfo.free = counter * _data_block_size;
    return meminfo.free;
}

uint32_t SMHashTable::getLongestFreeBlockSize() {
    uint32_t counter = 0;
    uint32_t longest = 0;
    for (auto i = (uint64_t) _memory_map_ptr; i < (uint64_t) _data_ptr; i++) {
        if (*(char *) i == 0) {
            counter++;
        } else {
            if (longest < counter) {
                longest = counter;
            }
            counter = 0;
        }
    }
    if (longest < counter) {
        longest = counter;
    }
    meminfo.max_free_block = longest * _data_block_size;
    return meminfo.max_free_block;
}

uint32_t SMHashTable::getLongestAllocatedBlockSize() {
    uint32_t counter = 0;
    uint32_t longest = 0;
    uint32_t segments = 0;
    for (auto i = (uint64_t) _memory_map_ptr; i < (uint64_t) _data_ptr; i++) {
        if (*(char *) i == 1) {
            counter++;
        } else {
            if (longest < counter) {
                longest = counter;
            }
            if (counter) {
                segments++;
            }
            counter = 0;
        }
    }
    if (longest < counter) {
        longest = counter;
    }
    meminfo.segments = segments;
    meminfo.max_allocated_block = longest * _data_block_size;
    return meminfo.max_allocated_block;
}

struct SMHashTable::meminfo *SMHashTable::memInfo() {
    getFreeMemorySize();
    getLongestAllocatedBlockSize();
    getLongestFreeBlockSize();
    return &meminfo;
}

struct SMHashTable::header *SMHashTable::findParent(struct SMHashTable::header *child) {
    char *key = (char *) ((void *) ((long) child->key_offset + (long) _data_ptr));
    auto check = get_header(key, child->key_size - 1);
    struct SMHashTable::header *prev;
    if (child != check) {
        while (check->linked_item) {
            prev = check;
            check = (struct header *) ((long) check->linked_item + (long) _data_ptr);
            if (check == child) {
                return prev;
            }
        }
    }
    return nullptr;
}

void SMHashTable::hardDefragmentation() {
    //Сдвигаем все блоки влево
    uint64_t free_block_address = 0;
    uint64_t free_block_size = 0;

    for (auto i = (uint64_t) _memory_map_ptr; i < (uint64_t) _data_ptr; i++) {
        if (*(char *) i == 0) {
            if (free_block_size == 0) {
                free_block_address = i;
            }
            free_block_size++;
        } else {
            if (free_block_size != 0) {
                //нашли дырку
                void *data_dimension = (void *) ((long) _data_ptr + (((long) i - (long) _memory_map_ptr) * _data_block_size));
                if ((*(long *) data_dimension >> 63) & 1U) {
                    //кусок данных
                    auto *header = (struct header *) ((*(uint32_t *) data_dimension) + (long) _header_ptr);
                    uint32_t alloc_block_size = int_ceil_divide((header->val_size + header->key_size + sizeof(void *)), _data_block_size);

                    //смещаем данные
                    free_memory_block((void *) i, alloc_block_size);
                    reserve_memory_block((void *) free_block_address, alloc_block_size);

                    void *free_block_dimension = (void *) ((long) _data_ptr +
                                                           (((long) free_block_address - (long) _memory_map_ptr) * _data_block_size));
                    void *occupied_block_dimension = (void *) ((long) _data_ptr +
                                                               (((long) i - (long) _memory_map_ptr) * _data_block_size));

                    long header_new_key_offset = ((long) free_block_dimension - (long) _data_ptr) + 8;
                    long header_new_val_offset = header_new_key_offset + header->key_size;

                    header->key_offset = (void *) header_new_key_offset;
                    header->val_offset = (void *) header_new_val_offset;

                    std::memcpy(free_block_dimension, occupied_block_dimension, alloc_block_size * _data_block_size);
                    //откатываем итератор назад и продолжаем искать свободный блок
                    i -= free_block_size;
                    free_block_size = 0;
                } else {
                    //заголовок
                    uint32_t alloc_block_size = int_ceil_divide(_header_size, _data_block_size);
                    auto *header = (struct header *) data_dimension;
                    void *free_block_dimension = (void *) ((long) _data_ptr +
                                                           (((long) free_block_address - (long) _memory_map_ptr) * _data_block_size));
                    void *occupied_block_dimension = (void *) ((long) _data_ptr +
                                                               (((long) i - (long) _memory_map_ptr) * _data_block_size));

                    auto parent = findParent(header);
                    if (parent != nullptr) {
                        //если у блока есть родитель (связный список), то нужно у родителя поменять адрес потомка
                        parent->linked_item = (void *) ((long) free_block_dimension - (long) _data_ptr);
                    }

                    //смещаем заголовок
                    free_memory_block((void *) i, alloc_block_size);
                    reserve_memory_block((void *) free_block_address, alloc_block_size);

                    //копируем заголовок
                    std::memcpy(free_block_dimension, occupied_block_dimension, alloc_block_size * _data_block_size);

                    //Меняем в данных адрес заголовка
                    auto *nheader = (struct header *) free_block_dimension;
                    uint32_t new_header_offset = ((long) free_block_dimension - (long) _header_ptr);
                    void *data_ptr = ((void *) ((long) nheader->key_offset + (long) _data_ptr - 8));

                    std::memcpy(data_ptr, &new_header_offset, sizeof(uint32_t));

                    i -= free_block_size;
                    free_block_size = 0;
                }
            }
        }
    }
}

inline struct SMHashTable::header *SMHashTable::get_header(const char *key, uint32_t size) {
    return (struct header *) ((void *) ((char *) _header_ptr + (hash_method(key, size) % _key_count * _header_size)));
}

inline void *SMHashTable::find_memory_block(size_t size, uint32_t offset) {
    lock(&_service_ptr->memory_mutex);
    void *ptr = find_zero_sequence((void *) ((long) _memory_map_ptr + offset), (void *) ((long) _memory_map_ptr + (long) _data_count), size);
    if(ptr){
        std::memset(ptr, 1, size);
    }
    unlock(&_service_ptr->memory_mutex);
    return ptr;
}

inline void SMHashTable::reserve_memory_block(void *addr, uint32_t size) {
    std::memset(addr, 1, size);
}

inline void SMHashTable::free_memory_block(void *addr, uint32_t size) {
    std::memset(addr, 0, size);
}

void *SMHashTable::find_zero_sequence(void *from, void *to, uint32_t len) {
    uint32_t counter = 0;
    do {
        if (*(char *) from == 0) {
            counter++;
        } else {
            counter = 0;
        }
        from = (void *) ((long) from + 1);

        if (len == counter) {
            return (void *) ((long) from - len);
        }
        if (from == to) {
            return nullptr;
        }
    } while (len != counter);
    return nullptr;
}

int SMHashTable::lock(pthread_mutex_t *mutex_ptr){
    int result = pthread_mutex_lock(mutex_ptr);
    if (result == EOWNERDEAD) {
        result = pthread_mutex_consistent(mutex_ptr);
        if (result != 0){
            perror("pthread_mutex_consistent");
        }
    }
    return result;
}

int SMHashTable::unlock(pthread_mutex_t *mutex_ptr){
    return pthread_mutex_unlock(mutex_ptr);
}
