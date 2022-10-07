#include "rm_cs.h"

#define CALLOC(count, size) RedisModule_Calloc(count, size)
#define FREE(ptr) RedisModule_Free(ptr)

#define ERROR(x)                        \
    RedisModule_ReplyWithError(ctx, x); \
    return REDISMODULE_ERR;

typedef Count_Sketch SKETCH;

static RedisModuleType *SKETCHType;

static int GetKey(RedisModuleCtx *ctx, RedisModuleString *keyName, SKETCH **sketch, int mode)
{
    // All using this function should call RedisModule_AutoMemory to prevent memory leak
    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_CloseKey(key);
        ERROR("CS: key does not exist");
    }
    else if (RedisModule_ModuleTypeGetType(key) != SKETCHType)
    {
        RedisModule_CloseKey(key);
        ERROR(REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    *sketch = (SKETCH *)RedisModule_ModuleTypeGetValue(key);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

static int create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, SKETCH **sketch)
{
    long long w, d;
    if ((RedisModule_StringToLongLong(argv[2], &w) != REDISMODULE_OK) || w < 1)
    {
        ERROR("CS: invalid w");
    }
    if ((RedisModule_StringToLongLong(argv[3], &d) != REDISMODULE_OK) || d < 1)
    {
        ERROR("CS: invalid d");
    }
    *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    (*sketch)->Create(w, d);
    return REDISMODULE_OK;
}

static int Create_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 4)
    {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    SKETCH *sketch = NULL;
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_ReplyWithError(ctx, "CS: key already exists");
        goto final;
    }

    if (create(ctx, argv, argc, &sketch) != REDISMODULE_OK)
        goto final;

    if (RedisModule_ModuleTypeSetValue(key, SKETCHType, sketch) == REDISMODULE_ERR)
    {
        goto final;
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
final:
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

static int Insert_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    if (argc < 3)
        return RedisModule_WrongArity(ctx);

    SKETCH *sketch;
    if (GetKey(ctx, argv[1], &sketch, REDISMODULE_READ | REDISMODULE_WRITE) != REDISMODULE_OK)
    {
        return REDISMODULE_OK;
    }

    int itemCount = argc - 2;
    RedisModule_ReplyWithArray(ctx, itemCount);

    for (int i = 0; i < itemCount; ++i)
    {
        size_t itemlen;
        const char *item = RedisModule_StringPtrLen(argv[i + 2], &itemlen);
        long long result = sketch->insert(item, itemlen, 1);
        RedisModule_ReplyWithLongLong(ctx, result);
    }
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

static int Query_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc < 3)
        return RedisModule_WrongArity(ctx);

    SKETCH *sketch;
    if (GetKey(ctx, argv[1], &sketch, REDISMODULE_READ) != REDISMODULE_OK)
        return REDISMODULE_ERR;

    size_t itemlen;
    long long res;
    RedisModule_ReplyWithArray(ctx, argc - 2);
    for (int i = 2; i < argc; ++i)
    {
        const char *item = RedisModule_StringPtrLen(argv[i], &itemlen);
        res = sketch->query(item, itemlen);
        RedisModule_ReplyWithLongLong(ctx, res);
    }

    return REDISMODULE_OK;
}

static int Info_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    if (argc != 2)
        return RedisModule_WrongArity(ctx);

    SKETCH *sketch = NULL;
    if (GetKey(ctx, argv[1], &sketch, REDISMODULE_READ) != REDISMODULE_OK)
    {
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, 2 * 2);
    RedisModule_ReplyWithSimpleString(ctx, "w");
    RedisModule_ReplyWithLongLong(ctx, sketch->w);
    RedisModule_ReplyWithSimpleString(ctx, "d");
    RedisModule_ReplyWithLongLong(ctx, sketch->d);

    return REDISMODULE_OK;
}

static void RdbSave(RedisModuleIO *io, void *obj)
{
    SKETCH *sketch = (SKETCH *)obj;
    RedisModule_SaveUnsigned(io, sketch->w);
    RedisModule_SaveUnsigned(io, sketch->d);
    RedisModule_SaveStringBuffer(io, (const char *)sketch->counters, sketch->w * sketch->d * sizeof(int));
}

static void *RdbLoad(RedisModuleIO *io, int encver)
{
    if (encver > CS_ENC_VER)
    {
        return NULL;
    }
    SKETCH *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    sketch->w = RedisModule_LoadUnsigned(io);
    sketch->d = RedisModule_LoadUnsigned(io);

    size_t counters_size;
    sketch->counters = (int *)RedisModule_LoadStringBuffer(io, &counters_size);
    assert(counters_size == sketch->w * sketch->d * sizeof(int));

    return sketch;
}

static void Free(void *value)
{
    SKETCH *sketch = (SKETCH *)value;
    sketch->Destroy();
    FREE(sketch);
}

static size_t MemUsage(const void *value)
{
    SKETCH *sketch = (SKETCH *)value;
    return sizeof(SKETCH) + sketch->w * sketch->d * sizeof(uint32_t);
}

int CSModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = RdbLoad,
                                 .rdb_save = RdbSave,
                                 .aof_rewrite = RMUtil_DefaultAofRewrite,
                                 .mem_usage = MemUsage,
                                 .free = Free};

    SKETCHType = RedisModule_CreateDataType(ctx, "CSSK-TYPE", CS_ENC_VER, &tm);
    if (SKETCHType == NULL)
        return REDISMODULE_ERR;

    RMUtil_RegisterWriteDenyOOMCmd(ctx, "cs.create", Create_Cmd);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "cs.insert", Insert_Cmd);
    RMUtil_RegisterReadCmd(ctx, "cs.query", Query_Cmd);
    RMUtil_RegisterReadCmd(ctx, "cs.info", Info_Cmd);

    return REDISMODULE_OK;
}