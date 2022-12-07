#ifndef _HalfCU_SKETCH_H_
#define _HalfCU_SKETCH_H_

#include "util/MurmurHash2.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <string.h>

class half_CU_Sketch
{
public:
    uint32_t w, d;
    uint32_t *counters;

    void Create(uint32_t _w, uint32_t _d);
    void Destroy();
    half_CU_Sketch(uint32_t _w, uint32_t _d);
    ~half_CU_Sketch();
    long long insert(const char *key, size_t key_len, uint32_t increment);
    uint32_t query(const char *key, size_t key_len);
};

#endif