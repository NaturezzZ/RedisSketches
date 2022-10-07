#ifndef _HeavyGuarding_H_
#define _HeavyGuarding_H_

#include "util/MurmurHash2.h"
#include "util/hash_table.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <string.h>
#include <math.h>

struct HG_node
{
    uint32_t C, FP;
};

class Heavy_Guardian
{
public:
    int M, G, ct;
    double HK_b;
    HG_node *HK;
    uint8_t *ext;
    hash_table *hh;
    double entropy;
    uint32_t hh_threshold;
    int total_packets;

    void Create(int _M, int _G, int _ct, double _HK_b, int hh_mod, uint32_t _hh_threshold);
    void Destroy();

    long long insert(const char *key, size_t key_len);
    uint32_t query(const char *key, size_t key_len);
    double get_entropy() { return -entropy / total_packets + log(total_packets) / log(2); }
    hash_table *get_heavy_hitter();
};

#endif