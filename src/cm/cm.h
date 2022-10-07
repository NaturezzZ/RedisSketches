#ifndef _CM_SKETCH_H_
#define _CM_SKETCH_H_

#include "util/MurmurHash2.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <string.h>

class CM_Sketch
{
public:
    int total_packets;
    uint32_t w, d;
    uint32_t *counters;

    void Create(uint32_t _w, uint32_t _d);
    void Destroy();
    CM_Sketch(uint32_t _w, uint32_t _d);
    ~CM_Sketch();
    long long insert(const char *key, size_t key_len, uint32_t increment);
    uint32_t query(const char *key, size_t key_len);
    static uint32_t Query(void *o, const char *key, size_t key_len);
    int query_total_packets();
};

#endif