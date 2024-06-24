// Code in ..\ffi-library\src\push_buffer.rs
#include <stdio.h>
#include <inttypes.h>

struct PUSH_PARSER {};
typedef int (*cb_t) (uint16_t, const uint8_t*);

extern "C" {
    void register_env_callback(PUSH_PARSER*, cb_t);
    PUSH_PARSER* push_parser_new();
    int push_parse_token(const PUSH_PARSER*);
    void push_parser_free(PUSH_PARSER*);
}

int push_callback(uint16_t len, const uint8_t* data)
{
    printf("Push callback received a buffer with %d bytes.\n", len);
    // Here would be the code to handle the data and parse it to ENVCHANGE.
    // Similar to what is in BATCHCTX::ProcessEnvChange.
    // https://sqlclientdrivers.visualstudio.com/msoledbsql/_git/msoledbsql?path=/Sql/Ntdbms/sqlncli/tds/Parse.cpp
    return 0;
}

int run_push_buffer(void) {
    PUSH_PARSER* parser = push_parser_new();
    register_env_callback(parser, push_callback);
    int result = push_parse_token(parser);
    push_parser_free(parser);
    return result;
}
