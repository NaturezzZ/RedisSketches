#ifndef _A_SKETCH_H_
#define _A_SKETCH_H_

#include "util/MurmurHash2.h"
#include "util/hash_table.h"
#include "cm/cm.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <string.h>
#include <math.h>

struct filter_node
{
    char *key;
    size_t key_len;
    uint32_t old_val;
    uint32_t new_val;
};

class A_Sketch
{
public:
    uint32_t filter_limit;
    filter_node *filter;
    uint32_t w, d;
    uint32_t *counters;
    hash_table *hh;
    double entropy;
    uint32_t hh_threshold;
    int total_packets;

    void Create(uint32_t _filter_limit, uint32_t _w, uint32_t _d, int hh_mod, int _hh_threshold);
    void Destroy();

    long long insert(const char *key, size_t key_len);
    uint32_t query(const char *key, size_t key_len);
    void adjust(int pos);
    hash_table *get_heavy_hitter() { return hh; }
    double get_entropy()
    {
        return -entropy / total_packets + log(total_packets) / log(2);
    }
};

#endif