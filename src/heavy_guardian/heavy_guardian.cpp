#include "heavy_guardian.h"

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

void Heavy_Guardian::Create(int _M, int _G, int _ct, double _HK_b, int hh_mod, uint32_t _hh_threshold)
{
    M = _M;
    G = _G;
    ct = _ct;
    HK_b = _HK_b;

    HK = (HG_node *)CALLOC(M * G, sizeof(HG_node));
    ext = (uint8_t *)CALLOC(M * ct, sizeof(uint8_t));

    hh_threshold = _hh_threshold;
    if (hh_mod != 0)
        hh = new hash_table(hh_mod);
}

void Heavy_Guardian::Destroy()
{
    FREE(HK);
    FREE(ext);
    delete hh;
}

long long Heavy_Guardian::insert(const char *key, size_t key_len)
{
    total_packets += 1;
    uint32_t H = HASH(key, key_len, 1000);
    uint32_t FP = (H >> 16), Hsh = H % M;
    bool FLAG = false;
    for (int k = 0; k < G; k++)
    {
        if (HK[Hsh * G + k].FP == FP)
        {
            if (HK[Hsh * G + k].C)
                entropy -= HK[Hsh * G + k].C * log(HK[Hsh * G + k].C) / log(2);
            HK[Hsh * G + k].C++;
            entropy += HK[Hsh * G + k].C * log(HK[Hsh * G + k].C) / log(2);
            if (HK[Hsh * G + k].C == hh_threshold)
                hh->insert(my_string(key, key_len), hh_threshold);
            FLAG = true;
            break;
        }
    }
    if (!FLAG)
    {
        int X = 0;
        uint32_t MIN = UINT32_MAX;
        for (int k = 0; k < G; k++)
        {
            uint32_t c = HK[Hsh * G + k].C;
            if (c < MIN)
            {
                MIN = c;
                X = k;
            }
        }
        if (!(rand() % int(pow(HK_b, HK[Hsh * G + X].C))))
        {
            if (HK[Hsh * G + X].C <= 1)
            {
                HK[Hsh * G + X].FP = FP;
                if (HK[Hsh * G + X].C)
                    entropy -= HK[Hsh * G + X].C * log(HK[Hsh * G + X].C) / log(2);
                HK[Hsh * G + X].C = 1;
                entropy += HK[Hsh * G + X].C * log(HK[Hsh * G + X].C) / log(2);
            }
            else
            {
                if (HK[Hsh * G + X].C)
                    entropy -= HK[Hsh * G + X].C * log(HK[Hsh * G + X].C) / log(2);
                HK[Hsh * G + X].C--;
                if (HK[Hsh * G + X].C)
                    entropy += HK[Hsh * G + X].C * log(HK[Hsh * G + X].C) / log(2);
                int p = Hsh % ct;
                if (ext[Hsh * ct + p])
                    entropy -= ext[Hsh * ct + p] * log(ext[Hsh * ct + p]) / log(2);
                if (ext[Hsh * ct + p] < UINT8_MAX)
                    ext[Hsh * ct + p]++;
                if (ext[Hsh * ct + p])
                    entropy += ext[Hsh * ct + p] * log(ext[Hsh * ct + p]) / log(2);
            }
        }
    }
    return 1;
}

uint32_t Heavy_Guardian::query(const char *key, size_t key_len)
{
    uint32_t H = HASH(key, key_len, 1000);
    uint32_t FP = (H >> 16), Hsh = H % M;
    for (int k = 0; k < G; k++)
    {
        uint32_t c = HK[Hsh * G + k].C;
        if (HK[Hsh * G + k].FP == FP)
            return MAX(1, c);
    }
    int p = Hsh % ct;
    return MAX(1, ext[Hsh * ct + p]);
}

hash_table *Heavy_Guardian::get_heavy_hitter()
{
    for (int i = 0; i < hh->mod; ++i)
        for (hash_table_node *it = hh->h[i]; it; it = it->next)
            it->value = query(it->key.c_str(), it->key.len());
    return hh;
}