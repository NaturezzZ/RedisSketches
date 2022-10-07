#include "basic_sketch/basic_sketch.h"

#include <assert.h>

#define MIN(x, y) ((x) < (y) ? (x) : (y))

class basic_cm : public basic_sketch
{
protected:
    uint32_t w, d;
    uint32_t *counters;

public:
    using basic_sketch::operator new;
    using basic_sketch::operator new[];
    using basic_sketch::operator delete;
    using basic_sketch::operator delete[];
    basic_cm(int argc, basic_sketch_string *argv)
    {
        if (argc != 2)
        {
            w = 1000;
            d = 3;
        }
        else
        {
            w = argv[0].to_int();
            d = argv[1].to_int();
        }
        counters = (uint32_t *)CALLOC(w * d, sizeof(uint32_t));
    }
    basic_cm(const basic_sketch_string &s)
    {
        size_t tmp = 0;
        const char *ss = s.c_str();

        memcpy(&w, ss + tmp, sizeof(uint32_t));
        tmp += sizeof(uint32_t);

        memcpy(&d, ss + tmp, sizeof(uint32_t));
        tmp += sizeof(uint32_t);

        counters = (uint32_t *)CALLOC(w * d, sizeof(uint32_t));
        memcpy(counters, ss + tmp, w * d * sizeof(uint32_t));
    }
    ~basic_cm()
    {
        FREE(counters);
    }
    basic_sketch_string *to_string()
    {
        char *s = (char *)CALLOC(2 + w * d, sizeof(uint32_t));
        size_t tmp = 0;

        memcpy(s + tmp, &w, sizeof(uint32_t));
        tmp += sizeof(uint32_t);

        memcpy(s + tmp, &d, sizeof(uint32_t));
        tmp += sizeof(uint32_t);

        memcpy(s + tmp, counters, w * d * sizeof(uint32_t));
        tmp += w * d * sizeof(uint32_t);

        basic_sketch_string *bs = new basic_sketch_string(s, tmp);
        delete s;

        return bs;
    }
    basic_sketch_reply *insert(const int &argc, const basic_sketch_string *argv)
    {
        basic_sketch_reply *result = new basic_sketch_reply;
        for (int c = 0; c < argc; ++c)
            for (uint32_t i = 0; i < d; ++i)
            {
                uint32_t *now_counter = counters + i * w + HASH(argv[c].c_str(), argv[c].len(), i) % w;
                if (*now_counter !=UINT32_MAX)
                    *now_counter += 1;
            }

        result->push_back("OK");
        return result;
    }
    basic_sketch_reply *query(const int &argc, const basic_sketch_string *argv)
    {
        basic_sketch_reply *result = new basic_sketch_reply;
        for (int c = 0; c < argc; ++c)
        {
            uint32_t ans = UINT32_MAX;
            for (uint32_t i = 0; i < d; ++i)
            {
                ans = MIN(ans, counters[i * w + HASH(argv[c].c_str(), argv[c].len(), i) % w]);
            }
            result->push_back((long long)ans);
        }
        return result;
    }

    static basic_sketch_reply *Insert(void *o, const int &argc, const basic_sketch_string *argv)
    {
        return ((basic_cm *)o)->insert(argc, argv);
    }
    static basic_sketch_reply *Query(void *o, const int &argc, const basic_sketch_string *argv)
    {
        return ((basic_cm *)o)->query(argc, argv);
    }

    static int command_num() { return 2; }
    static basic_sketch_string command_name(int index)
    {
        basic_sketch_string tmp[] = {"insert", "query"};
        return tmp[index];
    }
    static basic_sketch_func command(int index)
    {
        basic_sketch_func tmp[] = {(basic_cm::Insert), (basic_cm::Query)};
        return tmp[index];
    }
    static basic_sketch_string class_name() { return "basic_cm"; }
    static int command_type(int index)
    {
        int tmp[] = {0, 1};
        return tmp[index];
    }
    static char *type_name() { return "BASIC_CMS"; }
};