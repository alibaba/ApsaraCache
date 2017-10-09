#ifndef __MEMCACHED_BINARY_DEFINE_H
#define __MEMCACHED_BINARY_DEFINE_H
#include <stdint.h>

#ifdef __cplusplus
extern "C" 
{
#endif

/* memcached expire time type */
typedef unsigned int rel_time_t;
#define MEMCACHED_MAX_EXP_TIME 0xFFFFFFFF
/* For sasl, we only support PLAIN check */
/* PLAIN should contain \0 */
#define SASL_PLAIN  "PLAIN"
#define SASL_PLAIN_LEN sizeof(SASL_PLAIN)
#define REALTIME_MAXDELTA 30*24*3600
#define MAX_MEMCACHED_READ_REQUEST_LENGTH_DEFAULT (5 << 20)
#define MAX_MEMCACHED_ALLOW_REQUEST_LENGTH_DEFAULT (4 << 20)
#define MAX_MEMCACHED_ALLOW_REQUEST_LENGTH_PLUS (1 << 20)
#define MAX_MEMCACHED_ALLOW_REQUEST_LENGTH (128 << 20)
#define MAX_ERROR_LEN 64
#define MIN_ERROR_LEN 32
#define MAX_HEADER_LEN 320
#define MEMCACHED_KEY_MAX_LENGTH 250
#define MAX_SASL_MECH_LEN 32

/* the data stored in the robj->ptr just like the follow struct */
typedef struct memcachedValueItem {
    uint64_t cas;
    uint32_t flags;
    char data[];
} memcachedValueItem;

#define MEMCACHED_VALUE_ITEM_HEAD_LEN  (size_t)(((memcachedValueItem *)16)->data - 16)
#define MINUS_MEMCACHED_VALUE_ITEM_HEAD_LEN (-1 * (int)MEMCACHED_VALUE_ITEM_HEAD_LEN)
#define getMemcachedValueCas(o) (((memcachedValueItem*)(o->ptr))->cas)
#define getMemcachedValueFlags(o) ((memcachedValueItem*)(o->ptr))->flags
#define getMemcachedValuePtr(o) ((memcachedValueItem*)(o->ptr))->data

#define setMemcachedValueCas(o, cas) \
do { \
 ((memcachedValueItem*)(o->ptr))->cas = cas;\
} while (0)


#define setMemcachedValueFlags(o, flags) \
do { \
    ((memcachedValueItem*)(o->ptr))->flags = flags; \
} while (0)

/* Define memcached reply wrapper macro
 * decide if we should reply package */
#define addReplyMemcached(c, obj) \
do {\
    if (c->flags & CLIENT_NO_REPLY) break;\
    addReply(c, obj);\
} while(0)

#define addReplyStringMemcached(c, buf, len) \
do { \
    if (c->flags & CLIENT_NO_REPLY) break;\
    addReplyString(c, buf, len);\
} while (0)
#define addReplySdsMemcached(c, s) \
do { \
    if (c->flags & CLIENT_NO_REPLY) { \
        sdsfree(s);\
        break;\
    } \
    addReplySds(c, s);\
} while (0)

/* Definition of the legal "magic" used in packet. */
typedef enum memcachedBinaryMagic {
    MEMCACHED_BINARY_REQ = 0x80, // Request packet for this protocol version.
    MEMCACHED_BINARY_RES = 0x81  // Response packet for this protocol version.
} memcachedBinaryMagic;

/* Definition of the valid response status numbers. */

typedef enum memcachedBinaryResponseStatus {
    MEMCACHED_BINARY_RESPONSE_SUCCESS = 0x00, //  No error
    MEMCACHED_BINARY_RESPONSE_KEY_ENOENT = 0x01, //  Key not found
    MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS = 0x02, //  Key exists
    MEMCACHED_BINARY_RESPONSE_E2BIG = 0x03, // Value too large
    MEMCACHED_BINARY_RESPONSE_EINVAL = 0x04, //  Invalid arguments
    MEMCACHED_BINARY_RESPONSE_NOT_STORED = 0x05, //  Item not stored
    MEMCACHED_BINARY_RESPONSE_DELTA_BADVAL = 0x06, //  Incr/Decr on non-numeric value.
    MEMCACHED_BINARY_RESPONSE_AUTH_ERROR = 0x20, //  Authentication error
    MEMCACHED_BINARY_RESPONSE_AUTH_CONTINUE = 0x21, //  Authentication continue
    MEMCACHED_BINARY_RESPONSE_UNKNOWN_COMMAND = 0x81, // Unknown command 
    MEMCACHED_BINARY_RESPONSE_ENOMEM = 0x82, // Out of memory 
    MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR = 0x84, // internal error 
    MEMCACHED_BINARY_RESPONSE_TEMPORARY_FAILURE_ERROR = 0x86 // temporary failure 
} memcachedBinaryResponseStatus;

/* Definition of the different command opcodes. */

typedef enum memcachedBinaryCommand {
    MEMCACHED_BINARY_CMD_GET = 0x00, // get 
    MEMCACHED_BINARY_CMD_SET = 0x01, // set 
    MEMCACHED_BINARY_CMD_ADD = 0x02, // add 
    MEMCACHED_BINARY_CMD_REPLACE = 0x03, // replace
    MEMCACHED_BINARY_CMD_DELETE = 0x04, // delete
    MEMCACHED_BINARY_CMD_INCREMENT = 0x05, // increment
    MEMCACHED_BINARY_CMD_DECREMENT = 0x06, // decrement
    MEMCACHED_BINARY_CMD_QUIT = 0x07, // quit
    MEMCACHED_BINARY_CMD_FLUSH = 0x08, // flush
    MEMCACHED_BINARY_CMD_GETQ = 0x09, // getq
    MEMCACHED_BINARY_CMD_NOOP = 0x0a, // no-op
    MEMCACHED_BINARY_CMD_VERSION = 0x0b, // version
    MEMCACHED_BINARY_CMD_GETK = 0x0c, // getk 
    MEMCACHED_BINARY_CMD_GETKQ = 0x0d, // getkq, getk quiet
    MEMCACHED_BINARY_CMD_APPEND = 0x0e, // append
    MEMCACHED_BINARY_CMD_PREPEND = 0x0f, // prepend
    MEMCACHED_BINARY_CMD_STAT = 0x10, // stat (not support this time) 
    MEMCACHED_BINARY_CMD_SETQ = 0x11, // set quiet
    MEMCACHED_BINARY_CMD_ADDQ = 0x12, // add quiet
    MEMCACHED_BINARY_CMD_REPLACEQ = 0x13, // replace quiet
    MEMCACHED_BINARY_CMD_DELETEQ = 0x14, // delete quiet
    MEMCACHED_BINARY_CMD_INCREMENTQ = 0x15, // increment quiet
    MEMCACHED_BINARY_CMD_DECREMENTQ = 0x16, // decrement quiet
    MEMCACHED_BINARY_CMD_QUITQ = 0x17, // quit quiet
    MEMCACHED_BINARY_CMD_FLUSHQ = 0x18, // flush quiet
    MEMCACHED_BINARY_CMD_APPENDQ = 0x19, // append quiet
    MEMCACHED_BINARY_CMD_PREPENDQ = 0x1a, // prepend quiet
    MEMCACHED_BINARY_CMD_TOUCH = 0x1c, // touch 
    MEMCACHED_BINARY_CMD_GAT = 0x1d, // gat
    MEMCACHED_BINARY_CMD_GATQ = 0x1e, //gat quiet
    MEMCACHED_BINARY_CMD_SASL_LIST_MECHS = 0x20, // sasl list mechs
    MEMCACHED_BINARY_CMD_SASL_AUTH = 0x21, // sasl auth
    MEMCACHED_BINARY_CMD_SASL_STEP = 0x22, // sasl step
    MEMCACHED_BINARY_CMD_CAS = 0x48, // cas
    MEMCACHED_BINARY_CMD_FAKE = 0xff // fake command
} memcachedBinaryCommand;

/* Definition of the binary datatypes,now there is only one data type, raw type: 0x00; */

typedef enum memcachedBinaryDatatypes {
    MEMCACHED_BINARY_RAW_BYTES = 0x00
} memcachedBinaryDatatypes;

/* Definition of the header structure for a request packet */
/* Byte/     0       |       1       |       2       |       3       |
 *    /              |               |               |               |
 *   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
 *   +---------------+---------------+---------------+---------------+
 *  0| Magic         | Opcode        | Key length                    |
 *   +---------------+---------------+---------------+---------------+
 *  4| Extras length | Data type     | vbucket id                    |
 *   +---------------+---------------+---------------+---------------+
 *  8| Total body length                                             |
 *   +---------------+---------------+---------------+---------------+
 * 12| Opaque                                                        |
 *   +---------------+---------------+---------------+---------------+
 * 16| CAS                                                           |
 *   |                                                               |
 *   +---------------+---------------+---------------+---------------+
 * Total 24 bytes */

typedef union memcachedBinaryRequestHeader {
    struct {
        uint8_t magic; // magic number
        uint8_t opcode;
        uint16_t keylen;
        uint8_t extlen;
        uint8_t datatype;
        uint16_t reserved;
        uint32_t bodylen;
        uint32_t opaque;
        uint64_t cas;
    } request;
    uint8_t bytes[24]; // 24 bytes  
} memcachedBinaryRequestHeader;

/* Definition of the header structure for a response. */
/*  Byte/     0       |       1       |       2       |       3       |
 *     /              |               |               |               |
 *    |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
 *    +---------------+---------------+---------------+---------------+
 *   0| Magic         | Opcode        | Key Length                    |
 *    +---------------+---------------+---------------+---------------+
 *   4| Extras length | Data type     | Status                        |
 *    +---------------+---------------+---------------+---------------+
 *   8| Total body length                                             |
 *    +---------------+---------------+---------------+---------------+
 *  12| Opaque                                                        |
 *    +---------------+---------------+---------------+---------------+
 *  16| CAS                                                           |
 *    |                                                               |
 *    +---------------+---------------+---------------+---------------+
 *  Total 24 bytes */

typedef union memcachedBinaryResponseHeader {
    struct {
        uint8_t magic;
        uint8_t opcode;
        uint16_t keylen;
        uint8_t extlen;
        uint8_t datatype;
        uint16_t status;
        uint32_t bodylen;
        uint32_t opaque;
        uint64_t cas;
    } response;
    uint8_t bytes[24]; // 24 bytes
} memcachedBinaryResponseHeader;

/* Definition of a request-packet containing no extras */

typedef union memcachedBinaryRequestNoExtras {
    struct {
        memcachedBinaryRequestHeader header;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryRequestHeader)];
} memcachedBinaryRequestNoExtras;

/* Definition of a response-packet containing no extras */
typedef union memcachedBinaryResponseNoExtras{
    struct {
        memcachedBinaryResponseHeader header;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryResponseHeader)];
} memcachedBinaryResponseNoExtras;

/* request-packet have no extras commands are:
 * Get, Get Quietly, Get Key, Get Key Quietly
 * Delete
 * quit
 * noop
 * version
 * Append, Prepend
 * stat */

/* response-packet that have no extras commands are: 
 * Set, Add, Replace
 * Delete
 * Increment, Decrement
 * quit
 * Flush
 * noop
 * version
 * Append, Prepend
 * stat */

/* Definition of the packet used by the get, getq, getk and getkq command. */
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestGet;
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestGetq;
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestGetk;
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestGetkq;

/* Definition of the packet returned from a successful get, getq, getk and
 * getkq. */
typedef union memcachedBinaryResponseGet {
    struct {
        memcachedBinaryResponseHeader header;
        struct {
            uint32_t flags;
        } body;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryResponseHeader) + 4];
} memcachedBinaryResponseGet;

typedef memcachedBinaryResponseGet memcachedBinaryResponseGetq;
typedef memcachedBinaryResponseGet memcachedBinaryResponseGetk;
typedef memcachedBinaryResponseGet memcachedBinaryResponseGetkq;

/* Definition of the packet used by the delete command */
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestDelete;

/* Definition of the packet returned by the delete command */
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseDelete;

/* Definition of the packet used by the flush command
 * Please note that the expiration field is optional, so remember to see
 * check the header.bodysize to see if it is present. */
typedef union memcachedBinaryRequestFlush {
    struct {
        memcachedBinaryRequestHeader header;
        struct {
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryRequestHeader) + 4];
} memcachedBinaryRequestFlush;

/* Definition of the packet returned by the flush command */
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseFlush;

/* Definition of the packet used by set, add and replace */
typedef union memcachedBinaryRequestSet {
    struct {
        memcachedBinaryRequestHeader header;
        struct {
            uint32_t flags;
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryRequestHeader) + 8];
} memcachedBinaryRequestSet;

typedef memcachedBinaryRequestSet memcachedBinaryRequestAdd;
typedef memcachedBinaryRequestSet memcachedBinaryRequestReplace;

/* Definition of the packet returned by set, add and replace */
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseSet;
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseAdd;
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseReplace;

/* Definition of the noop packet */
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestNoop;

/* Definition of the packet returned by the noop command */
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseNoop;

/* Definition of the structure used by the increment and decrement command. */
typedef union memcachedBinaryRequestIncr {
    struct {
        memcachedBinaryRequestHeader header;
        struct {
            uint64_t delta;
            uint64_t initial;
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryRequestHeader) + 20];
} memcachedBinaryRequestIncr;
typedef memcachedBinaryRequestIncr memcachedBinaryRequestDecr;

/* Definition of the response from an incr or decr command
 * command. */
typedef union memcachedBinaryResponseIncr {
    struct {
        memcachedBinaryResponseHeader header;
        struct {
            uint64_t value;
        } body;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryResponseHeader) + 8];
} memcachedBinaryResponseIncr;

typedef memcachedBinaryResponseIncr memcachedBinaryResponseDecr;

/* Definition of the quit command */
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestQuit;

/* Definition of the packet returned by the quit command */
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseQuit;

/* Definition of the packet used by append and prepend command */
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestAppend;
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestPrepend;

/* Definition of the packet returned from a successful append or prepend */
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseAppend;
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponsePrepend;

/* Definition of the packet used by the version command */
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestVersion;

/* Definition of the packet returned from a successful version command */
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseVersion;


/* Definition of the packet used by the stats command. */
typedef memcachedBinaryRequestNoExtras memcachedBinaryRequestStats;

/* Definition of the packet returned from a successful stats command */
typedef memcachedBinaryResponseNoExtras memcachedBinaryResponseStats;

/* Definition of the packet used by the touch command. */
typedef union memcachedBinaryRequestTouch {
    struct {
        memcachedBinaryRequestHeader header;
        struct {
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryRequestHeader) + 4];
} memcachedBinaryRequestTouch;

/* Definition of the packet returned from the touch command */
typedef memcachedBinaryResponseGet memcachedBinaryResponseTouch;

/* Definition of the packet used by the GAT(Q) command. */
typedef union memcachedBinaryRequestGat {
    struct {
        memcachedBinaryRequestHeader header;
        struct {
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(memcachedBinaryRequestHeader) + 4];
} memcachedBinaryRequestGat;

typedef memcachedBinaryRequestGat memcachedBinaryRequestGatq;

/* Definition of the packet returned from the GAT(Q) */
typedef memcachedBinaryResponseGet memcachedBinaryResponseGat;
typedef memcachedBinaryResponseGet memcachedBinaryResponseGatq;

#ifdef __cplusplus
}
#endif
#endif
