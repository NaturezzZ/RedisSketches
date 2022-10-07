#ifndef _LIGHT_PART_H_
#define _LIGHT_PART_H_

#include "param.h"

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

class LightPart
{
public:
    int counter_num;
    uint8_t *counters;
    int *mice_dist;

    LightPart()
    {
    }
    ~LightPart()
    {
    }

    void init(int _counter_num)
    {
        counter_num = _counter_num;
        counters = (uint8_t *)CALLOC(counter_num, sizeof(uint8_t));
        mice_dist = (int *)CALLOC(256, sizeof(int));
    }

    /* insertion */
    void insert(const char *key, size_t key_len, int f = 1)
    {
        uint32_t hash_val = HASH(key, key_len, 1000);
        uint32_t pos = hash_val % (uint32_t)counter_num;

        int old_val = (int)counters[pos];
        int new_val = (int)counters[pos] + f;

        new_val = new_val < 255 ? new_val : 255;
        counters[pos] = (uint8_t)new_val;

        mice_dist[old_val]--;
        mice_dist[new_val]++;
    }

    void swap_insert(const char *key, size_t key_len, int f)
    {
        uint32_t hash_val = HASH(key, key_len, 1000);
        uint32_t pos = hash_val % (uint32_t)counter_num;

        f = f < 255 ? f : 255;
        if (counters[pos] < f)
        {
            int old_val = (int)counters[pos];
            counters[pos] = (uint8_t)f;
            int new_val = (int)counters[pos];

            mice_dist[old_val]--;
            mice_dist[new_val]++;
        }
    }

    /* query */
    int query(const char *key, size_t key_len)
    {
        uint32_t hash_val = HASH(key, key_len, 1000);
        uint32_t pos = hash_val % (uint32_t)counter_num;

        return (int)counters[pos];
    }

    /* other measurement task */
    int get_memory_usage() { return counter_num + sizeof(int) * 256; }

    int get_cardinality()
    {
        int mice_card = 0;
        for (int i = 1; i < 256; i++)
            mice_card += mice_dist[i];

        double rate = (counter_num - mice_card) / (double)counter_num;
        return counter_num * log(1 / rate);
    }

    void get_entropy(int &tot, double &entr)
    {
        for (int i = 1; i < 256; i++)
        {
            tot += mice_dist[i] * i;
            entr += mice_dist[i] * i * log2(i);
        }
    }
};

#endif