#include "waving.h"

#define REDIS_MODULE_TARGET
#ifdef REDIS_MODULE_TARGET
#include "util/redismodule.h"
#define CALLOC(count, size) RedisModule_Calloc(count, size)
#define FREE(ptr) RedisModule_Free(ptr)
#else
#define CALLOC(count, size) calloc(count, size)
#define FREE(ptr) free(ptr)
#endif

#define HASH(key, keylen, i) MurmurHash2(key, keylen, i)
#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))
#define calc_pos(key, keylen) (HASH(key, keylen, 1000) % l)
#define calc_counter(key, keylen) (HASH(key, keylen, 3000) % c)
#define calc_f(key, keylen) (HASH(key, keylen, 2000) % 2 == 0 ? -1 : 1)

void Waving_Sketch::Create(int _l, int _d, int _c, int hh_mod, int _hh_threshold)
{
    l = _l;
    d = _d;
    c = _c;

    counters = (int *)CALLOC(l * c, sizeof(int));
    keys = (char **)CALLOC(l * d, sizeof(char *));
    key_lens = (size_t *)CALLOC(l * d, sizeof(size_t));
    flags = (bool *)CALLOC(l * d, sizeof(bool));
    frequencies = (int *)CALLOC(l * d, sizeof(int));

    hh_threshold = _hh_threshold;
    if (hh_mod != 0)
        hh = new hash_table(hh_mod);
}

void Waving_Sketch::Destroy()
{
    FREE(counters);
    FREE(keys);
    FREE(key_lens);
    FREE(flags);
    FREE(frequencies);
    if (hh)
        delete hh;
}

long long Waving_Sketch::insert(const char *key, size_t key_len)
{
    total_packets += 1;
    int pos = calc_pos(key, key_len), f = calc_f(key, key_len), counter_index = calc_counter(key, key_len) + pos * c;
    int empty = -1, found = -1, smallest = -1;
    for (int i = pos * d; i < pos * d + d; ++i)
    {
        if (key_lens[i] == 0)
        {
            empty = i;
            continue;
        }
        if (key_lens[i] == key_len && strncmp(key, keys[i], key_len) == 0)
        {
            found = i;
            break;
        }
        if (smallest == -1 || frequencies[smallest] > frequencies[i])
            smallest = i;
    }
    if (found != -1)
    {
        entropy -= frequencies[found] * log(frequencies[found]) / log(2);
        frequencies[found] += 1;
        entropy += frequencies[found] * log(frequencies[found]) / log(2);
        if (hh && frequencies[found] >= (int)hh_threshold)
            hh->insert(my_string(key, key_len), frequencies[found]);
        if (!flags[found])
            counters[counter_index] += f;
    }
    else if (empty != -1)
    {
        keys[empty] = (char *)CALLOC(key_len, sizeof(char));
        memcpy(keys[empty], key, key_len);
        key_lens[empty] = key_len;
        flags[empty] = true;
        frequencies[empty] = 1;
        entropy += frequencies[empty] * log(frequencies[empty]) / log(2);
        if (hh && frequencies[empty] >= (int)hh_threshold)
            hh->insert(my_string(key, key_len), frequencies[empty]);
    }
    else
    {
        counters[counter_index] += f;
        int tmp = counters[counter_index] * f;
        /*
        if (hh && tmp >= (int)hh_threshold)
            hh->insert(my_string(key, key_len), tmp);
        */
        if (tmp > frequencies[smallest])
        {
            if (flags[smallest])
                counters[pos * c + calc_counter(keys[smallest], key_lens[smallest])] += frequencies[smallest] * calc_f(keys[smallest], key_lens[smallest]);
            FREE(keys[smallest]);
            keys[smallest] = (char *)CALLOC(key_len, sizeof(char));
            memcpy(keys[smallest], key, key_len);
            key_lens[smallest] = key_len;
            flags[smallest] = false;
            entropy -= frequencies[smallest] * log(frequencies[smallest]) / log(2);
            frequencies[smallest] = tmp;
            entropy += frequencies[smallest] * log(frequencies[smallest]) / log(2);
        }
    }
    return 1;
}

int Waving_Sketch::query(const char *key, size_t key_len)
{
    int pos = calc_pos(key, key_len), f = calc_f(key, key_len);
    int found = -1;
    for (int i = pos * d; i < pos * d + d; ++i)
    {
        if (key_lens[i] == key_len && strncmp(key, keys[i], key_len) == 0)
            return frequencies[i];
    }
    return 0;
}