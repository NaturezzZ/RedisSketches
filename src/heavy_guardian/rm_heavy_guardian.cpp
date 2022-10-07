#include "rm_heavy_guardian.h"

#define CALLOC(count, size) RedisModule_Calloc(count, size)
#define FREE(ptr) RedisModule_Free(ptr)

#define ERROR(x)                        \
    RedisModule_ReplyWithError(ctx, x); \
    return REDISMODULE_ERR;

typedef Heavy_Guardian SKETCH;

static RedisModuleType *SKETCHType;

static int GetKey(RedisModuleCtx *ctx, RedisModuleString *keyName, SKETCH **sketch, int mode)
{
    // All using this function should call RedisModule_AutoMemory to prevent memory leak
    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_CloseKey(key);
        ERROR("Heavy Guardian: key does not exist");
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
    long long M, G, ct, hh_mod, hh_threshold;
    double HK_b;
    if ((RedisModule_StringToLongLong(argv[2], &M) != REDISMODULE_OK) || M < 1)
    {
        ERROR("Heavy Guardian: invalid M");
    }
    if ((RedisModule_StringToLongLong(argv[3], &G) != REDISMODULE_OK) || G < 1)
    {
        ERROR("Heavy Guardian: invalid G");
    }
    if ((RedisModule_StringToLongLong(argv[4], &ct) != REDISMODULE_OK) || ct < 1)
    {
        ERROR("Heavy Guardian: invalid ct");
    }
    if ((RedisModule_StringToDouble(argv[5], &HK_b) != REDISMODULE_OK) || HK_b < 1)
    {
        ERROR("Heavy Guardian: invalid HK_b");
    }
    if ((RedisModule_StringToLongLong(argv[6], &hh_mod) != REDISMODULE_OK))
    {
        ERROR("Heavy Guardian: invalid heavy hitter size");
    }
    if ((RedisModule_StringToLongLong(argv[7], &hh_threshold) != REDISMODULE_OK))
    {
        ERROR("Heavy Guardian: invalid heavy hitter threshold");
    }
    *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    (*sketch)->Create(M, G, ct, HK_b, hh_mod, hh_threshold);
    return REDISMODULE_OK;
}

static int Create_Cmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 8)
    {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = (RedisModuleKey *)RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    SKETCH *sketch = NULL;
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY)
    {
        RedisModule_ReplyWithError(ctx, "Heavy Guardian: key already exists");
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

    RedisModule_ReplyWithArray(ctx, 4 * 2);
    RedisModule_ReplyWithSimpleString(ctx, "M");
    RedisModule_ReplyWithLongLong(ctx, sketch->M);
    RedisModule_ReplyWithSimpleString(ctx, "G");
    RedisModule_ReplyWithLongLong(ctx, sketch->G);
    RedisModule_ReplyWithSimpleString(ctx, "ct");
    RedisModule_ReplyWithLongLong(ctx, sketch->ct);
    RedisModule_ReplyWithSimpleString(ctx, "HK_b");
    RedisModule_ReplyWithDouble(ctx, sketch->HK_b);

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
    RedisModule_SaveSigned(io, sketch->M);
    RedisModule_SaveSigned(io, sketch->G);
    RedisModule_SaveSigned(io, sketch->ct);
    RedisModule_SaveDouble(io, sketch->HK_b);
    RedisModule_SaveStringBuffer(io, (const char *)sketch->HK, sketch->M * sketch->G * sizeof(HG_node));
    RedisModule_SaveStringBuffer(io, (const char *)sketch->ext, sketch->M * sketch->ct * sizeof(uint8_t));

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
    if (encver > HG_ENC_VER)
    {
        return NULL;
    }
    SKETCH *sketch = (SKETCH *)CALLOC(1, sizeof(SKETCH));
    size_t tmp;

    sketch->M = RedisModule_LoadSigned(io);
    sketch->G = RedisModule_LoadSigned(io);
    sketch->ct = RedisModule_LoadSigned(io);
    sketch->HK_b = RedisModule_LoadDouble(io);
    sketch->HK = (HG_node *)RedisModule_LoadStringBuffer(io, &tmp);
    sketch->ext = (uint8_t *)RedisModule_LoadStringBuffer(io, &tmp);

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

int HGModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = RdbLoad,
                                 .rdb_save = RdbSave,
                                 .aof_rewrite = RMUtil_DefaultAofRewrite,
                                 .free = Free};

    SKETCHType = RedisModule_CreateDataType(ctx, "HEAVYGUAR", HG_ENC_VER, &tm);
    if (SKETCHType == NULL)
        return REDISMODULE_ERR;

    RMUtil_RegisterWriteDenyOOMCmd(ctx, "heavy_guardian.create", Create_Cmd);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "heavy_guardian.insert", Insert_Cmd);
    RMUtil_RegisterReadCmd(ctx, "heavy_guardian.query", Query_Cmd);
    RMUtil_RegisterReadCmd(ctx, "heavy_guardian.info", Info_Cmd);
    RMUtil_RegisterReadCmd(ctx, "heavy_guardian.heavy_hitter", Heavy_Hitter_Cmd);
    RMUtil_RegisterReadCmd(ctx, "heavy_guardian.entropy", Entropy_Cmd);

    return REDISMODULE_OK;
}