#pragma once
#include <Windows.h>
#include <stdio.h>

#define SYSNAMELEN              128
#define MAX_CHARSET_NAME        30          // Max number of characters in a character set name
#define MAX_PROTOCOL_PREFIX_LEN   7
#define MAX_PROT_SRVINSTANCE_LEN  (MAX_PROTOCOL_PREFIX_LEN+FILENAME_MAX+1)          // net_protocol:Server\Instance or protocol:NetAddr
const USHORT MAX_ALTERNATE_SERVER_LENGTH_IN_CHAR = 1024;

typedef struct _ENVCHANGE
{
    BYTE    bEnvType;
    union
    {
        struct // DBNAME
        {
            WORD    cwchDBNameNew;
            WCHAR   wszDBNameNew[SYSNAMELEN + 1];
        };
        struct // LANGUAGE
        {
            WORD    cwchLangNew;
            WCHAR   wszLangNew[SYSNAMELEN + 1];
        };
        struct // PACKETSIZE
        {
            DWORD  usPacketSizeNew;
        };
        LONG    lUnicodeCompareStyle;
        LONG    lUnicodeLCID;
        struct // CHARSET
        {
            WORD    cwchCharSetNew;
            WCHAR   wszCharSetNew[MAX_CHARSET_NAME + 1];
            BOOL    fCaseSensitive;
        };
        struct // COLLATION
        {
            DWORD   dwWCID;
            BYTE    bSortid;
        };
        struct // TRANSACTION
        {
            ULONGLONG ulXactID;
        };
        struct // LOG SHIPPING
        {
            WORD    cwchPartnerNode;
            WCHAR   wszPartnerNode[MAX_PROT_SRVINSTANCE_LEN];
        };
        struct // ROUTING
        {
            USHORT  usRoutingDataLength;                      // Routing data value length.
            BYTE    bProtocol;                                // Protocol
            USHORT  usProtocolProperty;                       // Protocol property
            USHORT  cwchAlternateServer;                      // Alternate server length in characters
            WCHAR   wszAlternateServer[MAX_ALTERNATE_SERVER_LENGTH_IN_CHAR + 1]; // Alternate server
        };
    };
} ENVCHANGE;
