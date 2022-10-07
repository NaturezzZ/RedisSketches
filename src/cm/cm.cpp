#include "cm.h"

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

uint32_t CM_Sketch::Query(void *o, const char *key, size_t key_len)
{
    return ((CM_Sketch *)o)->query(key, key_len);
}

void CM_Sketch::Create(uint32_t _w, uint32_t _d)
{
    assert(_w);
    assert(_d);

    total_packets = 0;
    w = _w;
    d = _d;
    counters = (uint32_t *)CALLOC(w * d, sizeof(uint32_t));
}

void CM_Sketch::Destroy()
{
    FREE(counters);
}

CM_Sketch::CM_Sketch(uint32_t _w, uint32_t _d)
{
    Create(_w, _d);
}

CM_Sketch::~CM_Sketch()
{
    Destroy();
}

long long CM_Sketch::insert(const char *key, size_t key_len, uint32_t increment)
{
    assert(key);
    assert(key_len);

    total_packets += increment;

    for (uint32_t i = 0; i < d; ++i)
    {
        uint32_t *now_counter = counters + i * w + HASH(key, key_len, i) % w;
        if (UINT32_MAX - *now_counter < increment)
            *now_counter = UINT32_MAX;
        else
            *now_counter += increment;
    }

    return 1;
}

uint32_t CM_Sketch::query(const char *key, size_t key_len)
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

int CM_Sketch::query_total_packets() { return total_packets; }