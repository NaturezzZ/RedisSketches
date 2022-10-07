#include "rm_waving.h"

#define CALLOC(count, size) RedisModule_Calloc(count, size)
#define FREE(ptr) RedisModule_Free(ptr)

#define ERROR(x)                        \
    RedisModule_ReplyWithError(ctx, x); \
    return REDISMODULE_ERR;

typedef Waving_Sketch SKETCH;

static RedisModuleType *SKETCHType;

static int GetKey(RedisModuleCtx *ctx, RedisModuleString *keyName, SKETCH **sketch, int mode)
{
    // All using this function should call RedisModule_AutoMemory to prevent memory leak
    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_CloseKey(key);
        ERROR("Waving Sketch: key does not exist");
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
    long long l, d, c, hh_mod, hh_threshold;
    if ((RedisModule_StringToLongLong(argv[2], &l) != REDISMODULE_OK) || l < 1)
    {
        ERROR("Waving Sketch: invalid l");
    }
    if ((RedisModule_StringToLongLong(argv[3], &d) != REDISMODULE_OK) || d < 1)
    {
        ERROR("Waving Sketch: invalid d");
    }
    if ((RedisModule_StringToLongLong(argv[4], &c) != REDISMODULE_OK) || d < 1)
    {
        ERROR("Waving Sketch: invalid c");
    }
    if ((RedisModule_StringToLongLong(argv[5], &hh_mod) != REDISMODULE_OK))
    {
        ERROR("Waving Sketch: invalid heavy hitter size");
    }
    if ((RedisModule_StringToLongLong(argv[6], &hh_threshold) != REDISMODULE_OK))
    {
        ERROR("Waving Sketch: invalid heavy hitter threshold");
    }
    *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    (*sketch)->Create(l, d, c, hh_mod, hh_threshold);
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
        RedisModule_ReplyWithError(ctx, "Waving Sketch: key already exists");
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

    RedisModule_ReplyWithArray(ctx, 2 * 2);
    RedisModule_ReplyWithSimpleString(ctx, "l");
    RedisModule_ReplyWithLongLong(ctx, sketch->l);
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
    RedisModule_SaveSigned(io, sketch->l);
    RedisModule_SaveSigned(io, sketch->d);
    RedisModule_SaveSigned(io, sketch->c);
    RedisModule_SaveStringBuffer(io, (const char *)sketch->counters, sketch->l * sketch->c * sizeof(int));
    RedisModule_SaveStringBuffer(io, (const char *)sketch->key_lens, sketch->l * sketch->d * sizeof(size_t));
    RedisModule_SaveStringBuffer(io, (const char *)sketch->flags, sketch->l * sketch->d * sizeof(bool));
    RedisModule_SaveStringBuffer(io, (const char *)sketch->frequencies, sketch->l * sketch->d * sizeof(int));
    for (int i = 0; i < sketch->l * sketch->d; ++i)
        if (sketch->key_lens[i])
            RedisModule_SaveStringBuffer(io, (const char *)sketch->keys[i], sketch->key_lens[i] * sizeof(char));

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
    if (encver > WAVING_ENC_VER)
    {
        return NULL;
    }
    SKETCH *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    size_t tmp;
    sketch->l = RedisModule_LoadSigned(io);
    sketch->d = RedisModule_LoadSigned(io);
    sketch->c = RedisModule_LoadSigned(io);
    sketch->counters = (int *)RedisModule_LoadStringBuffer(io, &tmp);
    sketch->key_lens = (size_t *)RedisModule_LoadStringBuffer(io, &tmp);
    sketch->flags = (bool *)RedisModule_LoadStringBuffer(io, &tmp);
    sketch->frequencies = (int *)RedisModule_LoadStringBuffer(io, &tmp);

    sketch->keys = (char **)CALLOC(sketch->l, sizeof(char *));
    for (int i = 0; i < sketch->l; ++i)
        if (sketch->key_lens[i])
            sketch->keys[i] = (char *)RedisModule_LoadStringBuffer(io, &tmp);

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

int WavingModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = RdbLoad,
                                 .rdb_save = RdbSave,
                                 .aof_rewrite = RMUtil_DefaultAofRewrite,
                                 .free = Free};

    SKETCHType = RedisModule_CreateDataType(ctx, "Waving-SK", WAVING_ENC_VER, &tm);
    if (SKETCHType == NULL)
        return REDISMODULE_ERR;

    RMUtil_RegisterWriteDenyOOMCmd(ctx, "waving.create", Create_Cmd);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "waving.insert", Insert_Cmd);
    RMUtil_RegisterReadCmd(ctx, "waving.query", Query_Cmd);
    RMUtil_RegisterReadCmd(ctx, "waving.info", Info_Cmd);
    RMUtil_RegisterReadCmd(ctx, "waving.heavy_hitter", Heavy_Hitter_Cmd);
    RMUtil_RegisterReadCmd(ctx, "waving.entropy", Entropy_Cmd);

    return REDISMODULE_OK;
}