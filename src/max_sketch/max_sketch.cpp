#include "max_sketch.h"

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

void max_sketch::Create(uint32_t _w, uint32_t _d)
{
    assert(_w);
    assert(_d);

    w = _w;
    d = _d;
    counters = (uint32_t *)CALLOC(w * d, sizeof(uint32_t));
}

void max_sketch::Destroy()
{
    FREE(counters);
}

max_sketch::max_sketch(uint32_t _w, uint32_t _d)
{
    Create(_w, _d);
}

max_sketch::~max_sketch()
{
    Destroy();
}

long long max_sketch::insert(const char *key, size_t key_len, uint32_t value)
{
    assert(key);
    assert(key_len);

    for (uint32_t i = 0; i < d; ++i)
    {
        uint32_t prevValue = counters[i * w + HASH(key, key_len, i) % w];
        counters[i * w + HASH(key, key_len, i) % w] = MAX(prevValue, value);
    }
    return 1;
}

uint32_t max_sketch::query(const char *key, size_t key_len)
{
    assert(key);
    assert(key_len);

    uint32_t result = UINT32_MAX;
    for (uint32_t i = 0; i < d; ++i)
    {
        result = MIN(result, counters[i * w + HASH(key, key_len, i) % w]);
    }
    return result;
}