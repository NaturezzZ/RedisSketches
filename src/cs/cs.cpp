#include "cs.h"

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

void Count_Sketch::Create(uint32_t _w, uint32_t _d)
{
    assert(_w);
    assert(_d);

    w = _w;
    d = _d;
    counters = (int *)CALLOC(w * d, sizeof(int));
}

void Count_Sketch::Destroy()
{
    FREE(counters);
}

Count_Sketch::Count_Sketch(uint32_t _w, uint32_t _d)
{
    Create(_w, _d);
}

Count_Sketch::~Count_Sketch()
{
    Destroy();
}

long long Count_Sketch::insert(const char *key, size_t key_len, int increment)
{
    assert(key);
    assert(key_len);

    for (uint32_t i = 0; i < d; ++i)
    {
        uint32_t hash_val = HASH(key, key_len, i);
        counters[i * w + hash_val % w] += hash_val >> 31 ? increment : -increment;
    }

    return 1;
}

int Count_Sketch::query(const char *key, size_t key_len)
{
    assert(key);
    assert(key_len);
    assert(d < 20);

    int results[20];
    for (uint32_t i = 0; i < d; ++i)
    {
        uint32_t hash_val = HASH(key, key_len, i);
        results[i] = (hash_val >> 31 ? 1 : -1) * counters[i * w + hash_val % w];
    }
    std::sort(results, results + d);
    return results[d / 2];
}