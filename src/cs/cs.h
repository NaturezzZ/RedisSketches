#ifndef _COUNT_SKETCH_H_
#define _COUNT_SKETCH_H_

#include "util/MurmurHash2.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <string.h>
#include <algorithm>

class Count_Sketch
{
public:
    uint32_t w, d;
    int *counters;

    void Create(uint32_t _w, uint32_t _d);
    void Destroy();
    Count_Sketch(uint32_t _w, uint32_t _d);
    ~Count_Sketch();
    long long insert(const char *key, size_t key_len, int increment);
    int query(const char *key, size_t key_len);
};

#endif