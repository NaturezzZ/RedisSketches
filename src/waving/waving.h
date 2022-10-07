#ifndef _WAVING_SKETCH_H_
#define _WAVING_SKETCH_H_

#include "util/MurmurHash2.h"
#include "util/hash_table.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <string.h>
#include <math.h>

class Waving_Sketch
{
public:
    int l, d, c;
    int *counters;
    char **keys;
    size_t *key_lens;
    bool *flags;
    int *frequencies;
    hash_table *hh;
    double entropy;
    uint32_t hh_threshold;
    int total_packets;

    void Create(int _l, int _d, int _c, int hh_mod, int _hh_threshold);
    void Destroy();
    long long insert(const char *key, size_t key_len);
    int query(const char *key, size_t key_len);

    hash_table *get_heavy_hitter() { return hh; }
    double get_entropy() { return -entropy / total_packets + log(total_packets) / log(2); }
};

#endif