#ifndef _ELASTIC_SKETCH_H_
#define _ELASTIC_SKETCH_H_

#include "heavy_part.h"
#include "light_part.h"
#include "util/hash_table.h"

class ElasticSketch
{
public:
    HeavyPart heavy_part;
    LightPart light_part;
    uint32_t hh_threshold;

    ElasticSketch() {}
    ~ElasticSketch() {}
    void init(int heavy_num, int light_num, uint32_t _hh_threshold);
    void Destroy();

    long long insert(const char *key, size_t key_len, int f);

    int query(const char *key, size_t key_len);

    /* interface */
    int get_bucket_num();

    int get_cardinality();

    double get_entropy();

    hash_table *get_heavy_hitters();
};

#endif