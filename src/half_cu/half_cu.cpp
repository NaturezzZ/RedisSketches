#include "half_cu.h"

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

void half_CU_Sketch::Create(uint32_t _w, uint32_t _d)
{
    assert(_w);
    assert(_d);

    w = _w;
    d = _d;
    counters = (uint32_t *)CALLOC(w * d, sizeof(uint32_t));
}

void half_CU_Sketch::Destroy()
{
    FREE(counters);
}

half_CU_Sketch::half_CU_Sketch(uint32_t _w, uint32_t _d)
{
    Create(_w, _d);
}

half_CU_Sketch::~half_CU_Sketch()
{
    Destroy();
}

long long half_CU_Sketch::insert(const char *key, size_t key_len, uint32_t increment)
{
    assert(key);
    assert(key_len);

    uint32_t minimum = UINT32_MAX;
    uint32_t val = minimum;
    for (uint32_t i = 0; i < d; ++i)
    {
        val = counters[i * w + HASH(key, key_len, i) % w];
        if(UINT32_MAX - val > increment)
            val += increment;
        else
            val = UINT32_MAX;
        if(val <= minimum) {
            minimum = val;
            counters[i * w + HASH(key, key_len, i) % w] = val;
        }
    }
    return 1;
}

uint32_t half_CU_Sketch::query(const char *key, size_t key_len)
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