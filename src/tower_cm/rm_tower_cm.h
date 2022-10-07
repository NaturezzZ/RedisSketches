#ifndef _TOWER_CM_MODULE_H_
#define _TOWER_CM_MODULE_H_

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
#include "tower_cm.h"

#include <assert.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>

#define TOWER_CM_ENC_VER 0
#define REDIS_MODULE_TARGET

int TowerCMModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif