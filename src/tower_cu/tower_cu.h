#ifndef _TOWER_CU_SKETCH_H_
#define _TOWER_CU_SKETCH_H_

#include "util/MurmurHash2.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>
#include <string.h>

class TowerSketchCU
{
public:
    uint32_t d;
    uint32_t *w;
    uint32_t *A;
    uint32_t *hashseed;

public:
    TowerSketchCU(uint32_t _w, uint32_t _d);
    ~TowerSketchCU();
    void Create(uint32_t _w, uint32_t _d);
    void Destroy();
    long long insert(const char *key, size_t key_len, uint32_t increment);
    uint32_t query(const char *key, size_t key_len);
};
#endif