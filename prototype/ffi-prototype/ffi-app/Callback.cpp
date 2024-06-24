// Code in ..\ffi-library\src\callback.rs
#include <inttypes.h>
#include "Callback.h"

struct CALLBACK_PARSER {};
typedef int (*cb_t) (ENVCHANGE*);

extern "C" {
    void register_callback(CALLBACK_PARSER*, cb_t);
    CALLBACK_PARSER* callback_parser_new();
    int callback_parse_token(const CALLBACK_PARSER*);
    void callback_parser_free(CALLBACK_PARSER*);
}

int callback(ENVCHANGE* envChange)
{
    printf("Callback called with ENVCHANGE.\n");
    printf("Change type %d\n", envChange->bEnvType);
    switch (envChange->bEnvType)
    {
    case 1:
        printf("Database %S\n", envChange->wszDBNameNew);
        break;
    case 7:
        printf("Collation wcid %d\n", envChange->dwWCID);
        printf("Collation sortid %d\n", envChange->bSortid);
        break;
    case 4:
        printf("Package size %d\n", envChange->usPacketSizeNew);
        break;
    default:
        printf("Callback does not handle env type %d, yet.\n", envChange->bEnvType);
        break;
    }

    return 0;
}

int run_callback(void) {
    CALLBACK_PARSER* parser = callback_parser_new();
    register_callback(parser, callback);
    int result = callback_parse_token(parser);
    callback_parser_free(parser);
    return result;
}
