// Code in ..\ffi-library\src\pull_buffer.rs
#include <inttypes.h>
#include "Callback.h"

struct PULL_PARSER {};
struct PARSER_READER {};
typedef int (*cb_t) (PARSER_READER*);

extern "C" {
    void register_parse_callback(PULL_PARSER*, cb_t);
    PULL_PARSER* pull_parser_new();
    int pull_parse_token(const PULL_PARSER*);
    void pull_parser_free(PULL_PARSER*);
    uint32_t get_buffer(PARSER_READER*, uint16_t, uint8_t*);
}

#define CHECKERROR(error, message, param) if (error == 0) {printf(message, param);} else {return error;}

int pull_callback(PARSER_READER* reader)
{
    uint16_t len = 0;
    uint32_t error = get_buffer(reader, sizeof(len), (uint8_t*)&len);
    CHECKERROR(error, "Length %d\n", len);
    uint8_t type = 0;
    error = get_buffer(reader, sizeof(type), &type);
    CHECKERROR(error, "Type %d\n", type);

    if (type == 1)
    {
        ENVCHANGE EnvChange;
        EnvChange.bEnvType = type;
        uint8_t dbLen = 0;
        error = get_buffer(reader, sizeof(dbLen), &dbLen);
        CHECKERROR(error, "DB length %d\n", dbLen);
        EnvChange.cwchDBNameNew = dbLen;
        error = get_buffer(reader, dbLen * sizeof(wchar_t), (uint8_t*)&EnvChange.wszDBNameNew);
        CHECKERROR(error, "DB name %s\n", "");
        EnvChange.wszDBNameNew[dbLen] = L'\0';
        CHECKERROR(error, "DB name %S\n", EnvChange.wszDBNameNew);
    }
    else
    {
        // Here would be the code to handle the data and parse it to ENVCHANGE.
        // Similar to what is in BATCHCTX::ProcessEnvChange.
        // https://sqlclientdrivers.visualstudio.com/msoledbsql/_git/msoledbsql?path=/Sql/Ntdbms/sqlncli/tds/Parse.cpp
        printf("Pull callback does not support type %d, yet.\n", type);
    }

    return 0;
}

int run_pull_buffer(void) {
    PULL_PARSER* parser = pull_parser_new();
    register_parse_callback(parser, pull_callback);
    int result = pull_parse_token(parser);
    pull_parser_free(parser);
    return result;
}
