#ifndef _MaxSketch_SKETCH_H_
#define _MaxSketch_SKETCH_H_

#include "util/MurmurHash2.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <string.h>

class max_sketch
{
public:
    uint32_t w, d;
    uint32_t *counters;

    void Create(uint32_t _w, uint32_t _d);
    void Destroy();
    max_sketch(uint32_t _w, uint32_t _d);
    ~max_sketch();
    long long insert(const char *key, size_t key_len, uint32_t value);
    uint32_t query(const char *key, size_t key_len);
};

#endif