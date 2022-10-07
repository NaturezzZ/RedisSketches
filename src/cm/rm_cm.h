#ifndef _CM_MODULE_H_
#define _CM_MODULE_H_

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
#include "cm.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>

#define CM_ENC_VER 0
#define REDIS_MODULE_TARGET

int CMModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif