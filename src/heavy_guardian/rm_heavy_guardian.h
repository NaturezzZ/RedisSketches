#ifndef _HEAVY_GUARDIAN_MODULE_H_
#define _HEAVY_GUARDIAN_MODULE_H_

#ifdef __cplusplus
extern "C"
{
#endif
#include "util.h"
#ifdef __cplusplus
};
#endif

#include "util/redismodule.h"
#include "util/version.h"
#include "heavy_guardian.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>

#define HG_ENC_VER 0
#define REDIS_MODULE_TARGET

int HGModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif