#include "rm_elastic.h"

#define CALLOC(count, size) RedisModule_Calloc(count, size)
#define FREE(ptr) RedisModule_Free(ptr)

#define ERROR(x)                        \
    RedisModule_ReplyWithError(ctx, x); \
    return REDISMODULE_ERR;

typedef ElasticSketch SKETCH;

static RedisModuleType *SKETCHType;

static int GetKey(RedisModuleCtx *ctx, RedisModuleString *keyName, SKETCH **sketch, int mode)
{
    // All using this function should call RedisModule_AutoMemory to prevent memory leak
    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_CloseKey(key);
        ERROR("Elastic: key does not exist");
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
    long long heavy_num, light_num, hh_threshold;
    if ((RedisModule_StringToLongLong(argv[2], &heavy_num) != REDISMODULE_OK) || heavy_num < 1)
    {
        ERROR("Elastic: invalid heavy_num");
    }
    if ((RedisModule_StringToLongLong(argv[3], &light_num) != REDISMODULE_OK) || light_num < 1)
    {
        ERROR("Elastic: invalid light_num");
    }
    if ((RedisModule_StringToLongLong(argv[4], &hh_threshold) != REDISMODULE_OK))
    {
        ERROR("Elastic: invalid heavy hitter threshold");
    }
    *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    (*sketch)->init(heavy_num, light_num, hh_threshold);
    return REDISMODULE_OK;
}

static int Create_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 5)
    {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    SKETCH *sketch = NULL;
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_ReplyWithError(ctx, "Elastic: key already exists");
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
    RedisModule_ReplyWithSimpleString(ctx, "heavy_num");
    RedisModule_ReplyWithLongLong(ctx, sketch->heavy_part.bucket_num);
    RedisModule_ReplyWithSimpleString(ctx, "light_num");
    RedisModule_ReplyWithLongLong(ctx, sketch->light_part.counter_num);

    return REDISMODULE_OK;
}

static int Heavy_Hitter_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 2)
        return RedisModule_WrongArity(ctx);

    SKETCH *sketch;
    if (GetKey(ctx, argv[1], &sketch, REDISMODULE_READ) != REDISMODULE_OK)
        return REDISMODULE_ERR;

    hash_table *hh = sketch->get_heavy_hitters();
    hh->reply(ctx);
    delete hh;

    return REDISMODULE_OK;
}

static int Entropy_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 2)
        return RedisModule_WrongArity(ctx);

    SKETCH *sketch;
    if (GetKey(ctx, argv[1], &sketch, REDISMODULE_READ) != REDISMODULE_OK)
        return REDISMODULE_ERR;

    RedisModule_ReplyWithDouble(ctx, sketch->get_entropy());

    return REDISMODULE_OK;
}

static void RdbSave(RedisModuleIO *io, void *obj)
{
    SKETCH *sketch = (SKETCH *)obj;
    RedisModule_SaveSigned(io, sketch->heavy_part.bucket_num);
    RedisModule_SaveStringBuffer(io, (const char *)sketch->heavy_part.buckets, sketch->heavy_part.bucket_num * sizeof(Bucket));
    for (int i = 0; i < sketch->heavy_part.bucket_num; ++i)
        for (int j = 0; j < COUNTER_PER_BUCKET; ++j)
            if (sketch->heavy_part.buckets[i].key_len[j] != 0)
                RedisModule_SaveStringBuffer(io, (const char *)sketch->heavy_part.buckets[i].key[j], sketch->heavy_part.buckets[i].key_len[j] * sizeof(char));

    RedisModule_SaveUnsigned(io, sketch->light_part.counter_num);
    RedisModule_SaveStringBuffer(io, (const char *)sketch->light_part.counters, sketch->light_part.counter_num * sizeof(char));
    RedisModule_SaveStringBuffer(io, (const char *)sketch->light_part.mice_dist, 256 * sizeof(int));
    RedisModule_SaveUnsigned(io, sketch->hh_threshold);
}

static void *RdbLoad(RedisModuleIO *io, int encver)
{
    if (encver > ELASTIC_ENC_VER)
    {
        return NULL;
    }
    size_t tmp;
    SKETCH *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    sketch->heavy_part.bucket_num = RedisModule_LoadSigned(io);
    sketch->heavy_part.buckets = (Bucket *)RedisModule_LoadStringBuffer(io, &tmp);
    for (int i = 0; i < sketch->heavy_part.bucket_num; ++i)
        for (int j = 0; j < COUNTER_PER_BUCKET; ++j)
            if (sketch->heavy_part.buckets[i].key_len[j] != 0)
                sketch->heavy_part.buckets[i].key[j] = (char *)RedisModule_LoadStringBuffer(io, &tmp);

    sketch->light_part.counter_num = RedisModule_LoadUnsigned(io);
    sketch->light_part.counters = (uint8_t *)RedisModule_LoadStringBuffer(io, &tmp);
    sketch->light_part.mice_dist = (int *)RedisModule_LoadStringBuffer(io, &tmp);
    sketch->hh_threshold = RedisModule_LoadUnsigned(io);
    return sketch;
}

static void Free(void *value)
{
    SKETCH *sketch = (SKETCH *)value;
    sketch->Destroy();
    FREE(sketch);
}

int ElasticModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = RdbLoad,
                                 .rdb_save = RdbSave,
                                 .aof_rewrite = RMUtil_DefaultAofRewrite,
                                 .free = Free};

    SKETCHType = RedisModule_CreateDataType(ctx, "ELASTICSK", ELASTIC_ENC_VER, &tm);
    if (SKETCHType == NULL)
        return REDISMODULE_ERR;

    RMUtil_RegisterWriteDenyOOMCmd(ctx, "elastic.create", Create_Cmd);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "elastic.insert", Insert_Cmd);
    RMUtil_RegisterReadCmd(ctx, "elastic.query", Query_Cmd);
    RMUtil_RegisterReadCmd(ctx, "elastic.info", Info_Cmd);
    RMUtil_RegisterReadCmd(ctx, "elastic.heavy_hitter", Heavy_Hitter_Cmd);
    RMUtil_RegisterReadCmd(ctx, "elastic.entropy", Entropy_Cmd);

    return REDISMODULE_OK;
}