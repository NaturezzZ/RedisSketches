#include "rm_asketch.h"

#define CALLOC(count, size) RedisModule_Calloc(count, size)
#define FREE(ptr) RedisModule_Free(ptr)

#define ERROR(x)                        \
    RedisModule_ReplyWithError(ctx, x); \
    return REDISMODULE_ERR;

typedef A_Sketch SKETCH;

static RedisModuleType *SKETCHType;

static int GetKey(RedisModuleCtx *ctx, RedisModuleString *keyName, SKETCH **sketch, int mode)
{
    // All using this function should call RedisModule_AutoMemory to prevent memory leak
    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_CloseKey(key);
        ERROR("ASketch: key does not exist");
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
    long long filter, w, d, hh_mod, _hh_threshold;
    if ((RedisModule_StringToLongLong(argv[2], &filter) != REDISMODULE_OK) || w < 1)
    {
        ERROR("ASketch: invalid filter size");
    }
    if ((RedisModule_StringToLongLong(argv[3], &w) != REDISMODULE_OK) || w < 1)
    {
        ERROR("ASketch: invalid w");
    }
    if ((RedisModule_StringToLongLong(argv[4], &d) != REDISMODULE_OK) || d < 1)
    {
        ERROR("ASketch: invalid d");
    }
    if ((RedisModule_StringToLongLong(argv[5], &hh_mod) != REDISMODULE_OK))
    {
        ERROR("ASketch: invalid heavy hitter size");
    }
    if ((RedisModule_StringToLongLong(argv[6], &_hh_threshold) != REDISMODULE_OK))
    {
        ERROR("ASketch: invalid heavy hitter threshold");
    }
    *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    (*sketch)->Create(filter, w, d, hh_mod, _hh_threshold);
    return REDISMODULE_OK;
}

static int Create_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 7)
    {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    SKETCH *sketch = NULL;
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_ReplyWithError(ctx, "ASketch: key already exists");
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
        long long result = sketch->insert(item, itemlen);
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

    RedisModule_ReplyWithArray(ctx, 3 * 2);
    RedisModule_ReplyWithSimpleString(ctx, "filter size");
    RedisModule_ReplyWithLongLong(ctx, sketch->filter_limit);
    RedisModule_ReplyWithSimpleString(ctx, "w");
    RedisModule_ReplyWithLongLong(ctx, sketch->w);
    RedisModule_ReplyWithSimpleString(ctx, "d");
    RedisModule_ReplyWithLongLong(ctx, sketch->d);

    return REDISMODULE_OK;
}

static int Heavy_Hitter_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 2)
        return RedisModule_WrongArity(ctx);

    SKETCH *sketch;
    if (GetKey(ctx, argv[1], &sketch, REDISMODULE_READ) != REDISMODULE_OK)
        return REDISMODULE_ERR;

    hash_table *hh = sketch->get_heavy_hitter();
    hh->reply(ctx);

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
    RedisModule_SaveUnsigned(io, sketch->filter_limit);
    for (int i = 0; i < sketch->filter_limit; ++i)
    {
        filter_node *it = sketch->filter + i;
        RedisModule_SaveUnsigned(io, it->key_len);
        if (it->key_len)
        {
            RedisModule_SaveStringBuffer(io, (const char *)it->key, it->key_len);
            RedisModule_SaveUnsigned(io, it->old_val);
            RedisModule_SaveUnsigned(io, it->new_val);
        }
    }

    RedisModule_SaveUnsigned(io, sketch->w);
    RedisModule_SaveUnsigned(io, sketch->d);
    RedisModule_SaveStringBuffer(io, (const char *)sketch->counters, sketch->w * sketch->d * sizeof(uint32_t));

    RedisModule_SaveDouble(io, sketch->entropy);
    RedisModule_SaveSigned(io, sketch->total_packets);
    RedisModule_SaveUnsigned(io, sketch->hh_threshold);
    if (sketch->hh)
    {
        RedisModule_SaveSigned(io, sketch->hh->mod);
        sketch->hh->save(io);
    }
    else
        RedisModule_SaveSigned(io, 0);
}

static void *RdbLoad(RedisModuleIO *io, int encver)
{
    if (encver > ASKETCH_ENC_VER)
    {
        return NULL;
    }
    SKETCH *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    size_t tmp;

    sketch->filter_limit = RedisModule_LoadUnsigned(io);
    sketch->filter = (filter_node *)CALLOC(sketch->filter_limit, sizeof(filter_node));
    for (int i = 0; i < sketch->filter_limit; ++i)
    {
        filter_node *it = sketch->filter + i;
        it->key_len = RedisModule_LoadUnsigned(io);
        if (it->key_len)
        {
            it->key = RedisModule_LoadStringBuffer(io, &tmp);
            it->old_val = RedisModule_LoadUnsigned(io);
            it->new_val = RedisModule_LoadUnsigned(io);
        }
    }

    sketch->w = RedisModule_LoadUnsigned(io);
    sketch->d = RedisModule_LoadUnsigned(io);
    sketch->counters = (uint32_t *)RedisModule_LoadStringBuffer(io, &tmp);

    sketch->entropy = RedisModule_LoadDouble(io);
    sketch->total_packets = RedisModule_LoadSigned(io);
    sketch->hh_threshold = RedisModule_LoadUnsigned(io);
    int hh_mod = RedisModule_LoadSigned(io);
    if (hh_mod)
    {
        sketch->hh = new hash_table(hh_mod);
        sketch->hh->load(io);
    }

    return sketch;
}

static void Free(void *value)
{
    SKETCH *sketch = (SKETCH *)value;
    sketch->Destroy();
    FREE(sketch);
}

int ASketchModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = RdbLoad,
                                 .rdb_save = RdbSave,
                                 .aof_rewrite = RMUtil_DefaultAofRewrite,
                                 .free = Free};

    SKETCHType = RedisModule_CreateDataType(ctx, "AugmentSK", ASKETCH_ENC_VER, &tm);
    if (SKETCHType == NULL)
        return REDISMODULE_ERR;

    RMUtil_RegisterWriteDenyOOMCmd(ctx, "asketch.create", Create_Cmd);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "asketch.insert", Insert_Cmd);
    RMUtil_RegisterReadCmd(ctx, "asketch.query", Query_Cmd);
    RMUtil_RegisterReadCmd(ctx, "asketch.info", Info_Cmd);
    RMUtil_RegisterReadCmd(ctx, "asketch.heavy_hitter", Heavy_Hitter_Cmd);
    RMUtil_RegisterReadCmd(ctx, "asketch.entropy", Entropy_Cmd);

    return REDISMODULE_OK;
}