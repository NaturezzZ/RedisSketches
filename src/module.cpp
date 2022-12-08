#include "util/redismodule.h"
#include "util/version.h"
///*
#include "cm/rm_cm.h"
#include "cu/rm_cu.h"
#include "cs/rm_cs.h"
#include "half_cu/rm_half_cu.h"
#include "max_sketch/rm_max_sketch.h"
#include "tower_cm/rm_tower_cm.h"
#include "tower_cu/rm_tower_cu.h"
#include "elastic/rm_elastic.h"
#include "asketch/rm_asketch.h"
#include "waving/rm_waving.h"
#include "heavy_guardian/rm_heavy_guardian.h"
//*/
///*
#include "basic_sketch/basic_sketch.h"
#include "basic_cm/basic_cm.h"
//*/
//#include "test/rm_test.h"
#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>

#ifdef __cplusplus
extern "C"
{
#endif

    int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
    {
        if (RedisModule_Init(ctx, "RedisSketches", SKETCHES_MODULE_VERSION, REDISMODULE_APIVER_1) !=
            REDISMODULE_OK)
        {
            return REDISMODULE_ERR;
        }
        ///*
        CMModule_onLoad(ctx, argv, argc);
        CUModule_onLoad(ctx, argv, argc);
        CSModule_onLoad(ctx, argv, argc);
        TowerCMModule_onLoad(ctx, argv, argc);
        TowerCUModule_onLoad(ctx, argv, argc);
        ElasticModule_onLoad(ctx, argv, argc);
        ASketchModule_onLoad(ctx, argv, argc);
        WavingModule_onLoad(ctx, argv, argc);
        HGModule_onLoad(ctx, argv, argc);
        HalfCUModule_onLoad(ctx, argv, argc);
        MaxSketchModule_onLoad(ctx, argv, argc);
        //*/

        Basic_Sketch_Module_onLoad<basic_cm>(ctx, argv, argc);

        // TestModule_onLoad(ctx, argv, argc);

        return REDISMODULE_OK;
    }

#ifdef __cplusplus
}
#endif