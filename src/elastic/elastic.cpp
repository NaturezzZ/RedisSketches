#include "elastic.h"

#include <cstdio>

#define REDIS_MODULE_TARGET
#ifdef REDIS_MODULE_TARGET
#include "util/redismodule.h"
#define CALLOC(count, size) RedisModule_Calloc(count, size)
#define FREE(ptr) RedisModule_Free(ptr)
#else
#define CALLOC(count, size) calloc(count, size)
#define FREE(ptr) free(ptr)
#endif

void ElasticSketch::init(int heavy_num, int light_num, uint32_t _hh_threshold)
{
    heavy_part.init(heavy_num);
    light_part.init(light_num);
    hh_threshold = _hh_threshold;
}

void ElasticSketch::Destroy()
{
    for (int i = 0; i < heavy_part.bucket_num; ++i)
        for (int j = 0; j < COUNTER_PER_BUCKET; ++j)
            FREE(heavy_part.buckets[i].key[j]);
    FREE(heavy_part.buckets);

    FREE(light_part.counters);
    FREE(light_part.mice_dist);
}

long long ElasticSketch::insert(const char *key, size_t key_len, int f)
{
    // fprintf(stderr, "%lu\n", key_len);
    char swap_key[100];
    uint32_t swap_val = 0;
    int result = heavy_part.insert(key, key_len, swap_key, swap_val, f);

    switch (result)
    {
    case 0:
        return 1;
    case 1:
    {
        if (HIGHEST_BIT_IS_1(swap_val))
            light_part.insert(swap_key, key_len, GetCounterVal(swap_val));
        else
            light_part.swap_insert(swap_key, key_len, swap_val);
        return 1;
    }
    case 2:
        light_part.insert(key, key_len, 1);
        return 1;
    default:
        printf("error return value !\n");
        exit(1);
    }
    return 0;
}

int ElasticSketch::query(const char *key, size_t key_len)
{
    uint32_t heavy_result = heavy_part.query(key, key_len);
    if (heavy_result == 0 || HIGHEST_BIT_IS_1(heavy_result))
    {
        int light_result = light_part.query(key, key_len);
        return (int)GetCounterVal(heavy_result) + light_result;
    }
    return heavy_result;
}

int ElasticSketch::get_bucket_num() { return heavy_part.get_bucket_num(); }

int ElasticSketch::get_cardinality()
{
    int card = light_part.get_cardinality();
    for (int i = 0; i < heavy_part.get_bucket_num(); ++i)
        for (int j = 0; j < MAX_VALID_COUNTER; ++j)
        {
            int val = heavy_part.buckets[i].val[j];
            int ex_val = light_part.query(heavy_part.buckets[i].key[j], heavy_part.buckets[i].key_len[j]);

            if (HIGHEST_BIT_IS_1(val) && ex_val)
            {
                val += ex_val;
                card--;
            }
            if (GetCounterVal(val))
                card++;
        }
    return card;
}

double ElasticSketch::get_entropy()
{
    int tot = 0;
    double entr = 0;

    light_part.get_entropy(tot, entr);

    for (int i = 0; i < heavy_part.get_bucket_num(); ++i)
        for (int j = 0; j < MAX_VALID_COUNTER; ++j)
        {
            int val = heavy_part.buckets[i].val[j];

            int ex_val = light_part.query(heavy_part.buckets[i].key[j], heavy_part.buckets[i].key_len[j]);

            if (HIGHEST_BIT_IS_1(val) && ex_val)
            {
                val += ex_val;

                tot -= ex_val;

                entr -= ex_val * log2(ex_val);
            }
            val = GetCounterVal(val);
            if (val)
            {
                tot += val;
                entr += val * log2(val);
            }
        }
    return -entr / tot + log2(tot);
}

hash_table *ElasticSketch::get_heavy_hitters()
{
    hash_table *result = new hash_table(10000);
    for (int i = 0; i < heavy_part.bucket_num; ++i)
        for (int j = 0; j < MAX_VALID_COUNTER; ++j)
        {
            if (heavy_part.buckets[i].key_len[j] == 0)
                continue;
            my_string s(heavy_part.buckets[i].key[j], heavy_part.buckets[i].key_len[j]);
            int val = query(s.c_str(), s.len());
            if (val >= hh_threshold)
            {
                result->insert(s, val);
            }
        }
    return result;
}