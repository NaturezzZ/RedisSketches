#include "asketch.h"

#include <algorithm>

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

void A_Sketch::Create(uint32_t _filter_limit, uint32_t _w, uint32_t _d, int hh_mod, int _hh_threshold)
{
    filter_limit = _filter_limit;
    filter = (filter_node *)CALLOC(filter_limit, sizeof(filter_node));

    w = _w;
    d = _d;
    counters = (uint32_t *)CALLOC(w * d, sizeof(uint32_t));

    hh_threshold = _hh_threshold;
    if (hh_mod != 0)
        hh = new hash_table(hh_mod);
}

void A_Sketch::Destroy()
{
    FREE(counters);

    FREE(filter);
    if (hh)
        delete hh;
}

long long A_Sketch::insert(const char *key, size_t key_len)
{
    total_packets += 1;
    int found = -1, empty = -1;
    uint32_t pos = HASH(key, key_len, 1000) % filter_limit;
    int left = pos >> 3 << 3, right = MIN(left + 8, filter_limit);
    for (int i = left; i < right; ++i)
    {
        if (filter[i].key_len == 0)
        {
            empty = i;
            continue;
        }
        if (filter[i].key_len == key_len && strncmp(filter[i].key, key, key_len) == 0)
        {
            found = i;
            break;
        }
    }
    // fprintf(stderr, "%s %d %d\n", key, found, empty);
    if (found != -1)
    {
        if (filter[found].new_val)
            entropy -= filter[found].new_val * log(filter[found].new_val) / log(2);
        filter[found].new_val += 1;
        entropy += filter[found].new_val * log(filter[found].new_val) / log(2);
        if (hh && filter[found].new_val >= hh_threshold)
            hh->insert(my_string(filter[found].key, filter[found].key_len), filter[found].new_val);
        adjust(found);
    }
    else if (empty != -1)
    {
        filter[empty].key = (char *)CALLOC(key_len, sizeof(char));
        memcpy(filter[empty].key, key, key_len);
        filter[empty].key_len = key_len;
        filter[empty].old_val = 0;
        filter[empty].new_val = 1;
    }
    else
    {
        uint32_t pos[20];
        uint32_t val = UINT32_MAX;
        for (uint32_t i = 0; i < d; ++i)
        {
            pos[i] = i * w + HASH(key, key_len, i) % w;
            val = MIN(val, counters[pos[i]]);
        }
        if (val)
            entropy -= val * log(val) / log(2);
        val = UINT32_MAX;
        for (uint32_t i = 0; i < d; ++i)
        {
            uint32_t *now_counter = counters + pos[i];
            if (UINT32_MAX == *now_counter)
                *now_counter = UINT32_MAX;
            else
                *now_counter += 1;
            val = MIN(val, *now_counter);
        }
        entropy += val * log(val) / log(2);

        if (hh && val >= hh_threshold)
            hh->insert(my_string(key, key_len), val);
        if (val > filter[left].new_val)
        {
            filter_node *p = filter;
            if (p->new_val > p->old_val)
            {
                char *key_tmp = p->key;
                size_t key_len_tmp = p->key_len;
                uint32_t val_tmp = p->new_val - p->old_val;
                for (uint32_t i = 0; i < d; ++i)
                {
                    uint32_t *now_counter = counters + i * w + HASH(key_tmp, key_len_tmp, i) % w;
                    if (UINT32_MAX - *now_counter < val_tmp)
                        *now_counter = UINT32_MAX;
                    else
                        *now_counter += val_tmp;
                }
            }
            FREE(p->key);
            p->key = (char *)CALLOC(key_len, sizeof(char));
            memcpy(p->key, key, key_len);
            p->key_len = key_len;
            p->old_val = p->new_val = val;
            adjust(left);
        }
    }
    return 1;
}

uint32_t A_Sketch::query(const char *key, size_t key_len)
{
    uint32_t pos = HASH(key, key_len, 1000) % filter_limit;
    int left = pos >> 3 << 3, right = MIN(left + 8, filter_limit);
    for (int i = left; i < right; ++i)
    {
        if (filter[i].key_len == key_len && strncmp(filter[i].key, key, key_len) == 0)
        {
            return filter[i].new_val;
        }
    }
    uint32_t val = UINT32_MAX;
    for (uint32_t i = 0; i < d; ++i)
    {
        val = MIN(val, counters[i * w + HASH(key, key_len, i) % w]);
    }
    return val;
}

void A_Sketch::adjust(int pos)
{
    int left = pos >> 3 << 3, right = MIN(left + 8, filter_limit);
    while (pos < right - 1 && filter[pos].new_val > filter[pos + 1].new_val)
    {
        std::swap(filter[pos].key, filter[pos + 1].key);
        std::swap(filter[pos].key_len, filter[pos + 1].key_len);
        std::swap(filter[pos].new_val, filter[pos + 1].new_val);
        std::swap(filter[pos].old_val, filter[pos + 1].old_val);
        ++pos;
    }
}
