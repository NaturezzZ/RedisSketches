#include "tower_cu.h"

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

const uint32_t cs[] = {5, 4, 3, 2, 1};
const uint32_t cpw[] = {0, 1, 2, 3, 4};
const uint32_t lo[] = {0x0, 0x1, 0x3, 0x7, 0xf};
const uint32_t mask[] = {0xffffffff, 0xffff, 0xff, 0xf, 0x3};

void TowerSketchCU::Create(uint32_t _w, uint32_t _d)
{
    d = _d;
    w = (uint32_t *)CALLOC(d, sizeof(uint32_t));
    A = (uint32_t *)CALLOC(_w * d, sizeof(uint32_t));
    hashseed = (uint32_t *)CALLOC(d, sizeof(uint32_t));

    for (uint32_t i = 0; i < d; ++i)
    {
        w[i] = _w << i;
        hashseed[i] = rand() % 1229;
    }
}

void TowerSketchCU::Destroy()
{
    FREE(w);
    FREE(A);
    FREE(hashseed);
}

TowerSketchCU::TowerSketchCU(uint32_t _w, uint32_t _d)
{
    Create(_w, _d);
}

TowerSketchCU::~TowerSketchCU()
{
    Destroy();
}

long long TowerSketchCU::insert(const char *key, size_t key_len, uint32_t increment)
{
    assert(key);
    assert(key_len);

    uint32_t idx[20];
    uint32_t min_val = UINT32_MAX;
    for (uint32_t i = 0; i < d; ++i)
        idx[i] = HASH(key, key_len, hashseed[i]) % w[i];
    for (uint32_t i = 0; i < d; ++i)
    {
        uint32_t a = A[i * w[0] + (idx[i] >> cpw[i])];
        uint32_t shift = (idx[i] & lo[i]) << cs[i];
        uint32_t val = (a >> shift) & mask[i];
        min_val = (val < mask[i] && val < min_val) ? val : min_val;
    }
    for (uint32_t i = 0; i < d; ++i)
    {
        uint32_t &a = A[i * w[0] + (idx[i] >> cpw[i])];
        uint32_t shift = (idx[i] & lo[i]) << cs[i];
        uint32_t val = (a >> shift) & mask[i];
        a += (val < mask[i] && val == min_val) ? (1 << shift) : 0;
    }

    return 1;
}

uint32_t TowerSketchCU::query(const char *key, size_t key_len)
{
    assert(key);
    assert(key_len);

    uint32_t ret = UINT32_MAX;
    for (uint32_t i = 0; i < d; ++i)
    {
        uint32_t idx = HASH(key, key_len, hashseed[i]) % w[i];
        uint32_t a = A[i * w[0] + (idx >> cpw[i])];
        uint32_t shift = (idx & lo[i]) << cs[i];
        uint32_t val = (a >> shift) & mask[i];
        ret = (val < mask[i] && val < ret) ? val : ret;
    }
    return ret;
}