#ifndef _HEAVYPART_H_
#define _HEAVYPART_H_

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

class HeavyPart
{
public:
    int bucket_num;
    Bucket *buckets;
    HeavyPart()
    {
    }
    ~HeavyPart() {}

    void init(int _bucket_num)
    {
        bucket_num = _bucket_num;
        buckets = (Bucket *)CALLOC(bucket_num, sizeof(Bucket));
    }

    /* insertion */
    int insert(const char *key, size_t key_len, char *swap_key, uint32_t &swap_val, uint32_t f = 1)
    {
        uint32_t fp = HASH(key, key_len, 2000);
        int pos = fp % bucket_num;

        /* find if there has matched bucket */
        int matched = -1, empty = -1, min_counter = 0;
        uint32_t min_counter_val = GetCounterVal(buckets[pos].val[0]);
        for (int i = 0; i < COUNTER_PER_BUCKET - 1; i++)
        {
            if (buckets[pos].key[i] != NULL && strcmp(buckets[pos].key[i], key) == 0)
            {
                matched = i;
                break;
            }
            if (buckets[pos].key[i] == NULL && empty == -1)
                empty = i;
            if (min_counter_val > GetCounterVal(buckets[pos].val[i]))
            {
                min_counter = i;
                min_counter_val = GetCounterVal(buckets[pos].val[i]);
            }
        }

        /* if matched */
        if (matched != -1)
        {
            buckets[pos].val[matched] += f;
            return 0;
        }

        /* if there has empty bucket */
        if (empty != -1)
        {
            buckets[pos].key[empty] = (char *)CALLOC(key_len, sizeof(char));
            memcpy(buckets[pos].key[empty], key, key_len);
            buckets[pos].key_len[empty] = key_len;
            buckets[pos].val[empty] = f;
            return 0;
        }

        /* update guard val and comparison */
        uint32_t guard_val = buckets[pos].val[MAX_VALID_COUNTER];
        guard_val = UPDATE_GUARD_VAL(guard_val);

        if (!JUDGE_IF_SWAP(GetCounterVal(min_counter_val), guard_val))
        {
            buckets[pos].val[MAX_VALID_COUNTER] = guard_val;
            return 2;
        }

        // fprintf(stderr, "%lu\n", buckets[pos].key_len[min_counter]);
        memcpy(swap_key, buckets[pos].key[min_counter], buckets[pos].key_len[min_counter]);
        swap_val = buckets[pos].val[min_counter];

        buckets[pos].val[MAX_VALID_COUNTER] = 0;

        FREE(buckets[pos].key[min_counter]);
        buckets[pos].key[min_counter] = (char *)CALLOC(key_len, sizeof(char));
        memcpy(buckets[pos].key[min_counter], key, key_len);
        buckets[pos].key_len[min_counter] = key_len;
        buckets[pos].val[min_counter] = 0x80000001;

        return 1;
    }

    /* query */
    uint32_t query(const char *key, size_t key_len)
    {
        uint32_t fp = HASH(key, key_len, 2000);
        int pos = fp % bucket_num;

        for (int i = 0; i < MAX_VALID_COUNTER; ++i)
            if (buckets[pos].key_len[i] != 0 && strcmp(buckets[pos].key[i], key) == 0)
                return buckets[pos].val[i];

        return 0;
    }

    /* interface */
    int get_memory_usage()
    {
        return bucket_num * sizeof(Bucket);
    }
    int get_bucket_num()
    {
        return bucket_num;
    }
};

#endif
