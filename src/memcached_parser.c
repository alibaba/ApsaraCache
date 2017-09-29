#include "server.h"
#include "endianconv.h"

extern void addCommandStats(client *c);
extern void setProtocolError(const char *errstr, client *c, int pos);
extern int time_independent_strcmp(char *a, char *b);
extern int processInlineBuffer(client *c);
extern int  processMultibulkBuffer(client *c);
extern sds sdsfromMemcachedStringbuffer(const char *buf, size_t len, size_t cas_flag_size);
extern struct memcachedCommand memcachedCommandTable[MEMCACHED_TEXT_REQUEST_NUM + MEMCACHED_BINARY_REQUEST_NUM]; 

/* redis shared objects */
extern struct sharedObjectsStruct shared;
/* memcached ascii shared objects */
struct sharedMemcachedObjectsStruct memcached_shared;

/* get return object */
#define shared(c, obj, ret) \
do { \
    switch(c->reqtype) { \
        case PROTO_REQ_INLINE: \
        case PROTO_REQ_MULTIBULK: \
             ret = shared.obj;\
             break;\
        default: \
            ret = memcached_shared.obj; \
            break;\
    } \
} while (0)

static void addMemcachedBinaryHeader(client *c, uint16_t err, uint8_t hdr_len, uint16_t key_len, uint32_t body_len, char *ptr);
robj* createMemcachedBinaryResponse(client *c, void *d, int hlen, int keylen, int dlen);
/* this function is used to create memcached binary shared objects with errors*/
void replyMemcachedBinaryError(client *c, memcachedBinaryResponseStatus err,  const char *errstr);
/*
 * create memcached ascii shared objects
 */
static void createMemcachedAsciiSharedObjects(void) {
    memcached_shared.crlf = makeObjectShared(createObject(OBJ_STRING,sdsnew("\r\n")));
    memcached_shared.ok = makeObjectShared(createObject(OBJ_STRING,sdsnew("OK\r\n")));
    memcached_shared.err = makeObjectShared(createObject(OBJ_STRING,sdsnew("ERROR\r\n")));
    memcached_shared.norw = makeObjectShared(createObject(OBJ_STRING,sdsnew("ERROR DISABLE You can't write or read against a disable instance\r\n")));
    memcached_shared.noautherr = makeObjectShared(createObject(OBJ_STRING,sdsnew("ERROR no auth\r\n")));
    memcached_shared.oomerr = makeObjectShared(createObject(OBJ_STRING,sdsnew("SERVER_ERROR out of memory storing object\r\n")));
    memcached_shared.interr = makeObjectShared(createObject(OBJ_STRING,sdsnew("SERVER_ERROR server writing or opnening AOF error\r\n")));
    memcached_shared.bgsaveerr = makeObjectShared(createObject(OBJ_STRING,sdsnew("SERVER_ERROR server background save err\r\n")));
    memcached_shared.noreplicaserr = makeObjectShared(createObject(OBJ_STRING,sdsnew("SERVER_ERROR server has not enough replicases\r\n")));
    memcached_shared.roslaveerr = makeObjectShared(createObject(OBJ_STRING,sdsnew("SERVER_ERROR write read only save error\r\n")));
    memcached_shared.roerr = makeObjectShared(createObject(OBJ_STRING,sdsnew("ERROR READONLY You can't write against a read only instance\r\n")));
    memcached_shared.masterdownerr = makeObjectShared(createObject(OBJ_STRING,sdsnew("SERVER_ERROR the master server is gone awaye\r\n")));
    memcached_shared.loadingerr = makeObjectShared(createObject(OBJ_STRING,sdsnew("SERVER_ERROR server is loading data now\r\n")));
    memcached_shared.version = makeObjectShared(createObject(OBJ_STRING,sdsnew("VERSION " MEMCACHED_VERSION "\r\n")));
    memcached_shared.vercmd = makeObjectShared(createObject(OBJ_STRING,sdsnew("version")));
    memcached_shared.quitcmd = makeObjectShared(createObject(OBJ_STRING,sdsnew("quit")));
    memcached_shared.flushallcmd = makeObjectShared(createObject(OBJ_STRING,sdsnew("FLUSHALL")));
    memcached_shared.expirecmd = makeObjectShared(createObject(OBJ_STRING,sdsnew("EXPIRE")));
    memcached_shared.persistcmd = makeObjectShared(createObject(OBJ_STRING,sdsnew("PERSIST")));
    memcached_shared.noopcmd = makeObjectShared(createObject(OBJ_STRING,sdsnew("no-op")));
    memcached_shared.badfmt = makeObjectShared(createObject(OBJ_STRING,sdsnew("CLIENT_ERROR bad command line format\r\n")));
    memcached_shared.stored = makeObjectShared(createObject(OBJ_STRING,sdsnew("STORED\r\n")));
    memcached_shared.too_large = makeObjectShared(createObject(OBJ_STRING,sdsnew("SERVER_ERROR object too large for cache\r\n")));
    memcached_shared.ex = makeObjectShared(createObject(OBJ_STRING,sdsnew("EX")));
    memcached_shared.px = makeObjectShared(createObject(OBJ_STRING,sdsnew("PX")));
    memcached_shared.end = makeObjectShared(createObject(OBJ_STRING,sdsnew("END\r\n")));
    memcached_shared.invalidexp = makeObjectShared(createObject(OBJ_STRING,sdsnew("CLIENT_ERROR invalid exptime argument\r\n")));
    memcached_shared.touched = makeObjectShared(createObject(OBJ_STRING,sdsnew("TOUCHED\r\n")));
    memcached_shared.not_found = makeObjectShared(createObject(OBJ_STRING,sdsnew("NOT_FOUND\r\n")));
    memcached_shared.exists = makeObjectShared(createObject(OBJ_STRING,sdsnew("EXISTS\r\n")));
    memcached_shared.not_stored = makeObjectShared(createObject(OBJ_STRING,sdsnew("NOT_STORED\r\n")));
    memcached_shared.invalid_delta = makeObjectShared(createObject(OBJ_STRING,sdsnew("CLIENT_ERROR invalid numeric delta argument\r\n")));
    memcached_shared.invalid_numeric = makeObjectShared(createObject(OBJ_STRING,sdsnew("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")));
    memcached_shared.deleted = makeObjectShared(createObject(OBJ_STRING,sdsnew("DELETED\r\n")));
    memcached_shared.setcmd = makeObjectShared(createObject(OBJ_STRING,sdsnew("SET")));
    memcached_shared.sasl_list_cmd = makeObjectShared(createObject(OBJ_STRING,sdsnew("sasl_list_mechs")));
    memcached_shared.invalid_pw_or_uname = makeObjectShared(createObject(OBJ_STRING,sdsnew("ERROR invalid password or user name\r\n")));
    memcached_shared.not_in_white_list_err = makeObjectShared(createObject(OBJ_STRING,sdsnew("ERROR illegal address\r\n")));
}

/* Note if *Q request met error, we cann't use 
 * setProtocolError or else it will block */
static void memcachedSetProtocolError(client *c, int pos) {
    if (c->flags & CLIENT_NO_REPLY) {
        // close connect immediately
        freeClient(c);
    } else {
        setProtocolError("ocs protocol error", c, pos);
    }
}

/* create memcached ascii and binary shared objects */
void createMemcachedSharedObjects(void) {
    createMemcachedAsciiSharedObjects();
}

robj* createMemcachedBinaryResponse(client *c, void *d, int hlen, int keylen, int dlen) {
    dlen = dlen < 0 ? 0 : dlen;
    robj* biob = createStringObject(NULL, sizeof(memcachedBinaryResponseHeader) + dlen);
    addMemcachedBinaryHeader(c, 0, hlen, keylen, dlen, biob->ptr);
    if (d != NULL && dlen > 0) {
        memcpy((char *)biob->ptr + sizeof(memcachedBinaryResponseHeader), d, dlen);
    }
    return biob;
}

void addMemcachedBinaryHeader(client *c, uint16_t err, uint8_t hdr_len, uint16_t key_len, uint32_t body_len, char *ptr) {
    memcachedBinaryResponseHeader* header;

    header = (memcachedBinaryResponseHeader*)ptr;

    header->response.magic = (uint8_t)MEMCACHED_BINARY_RES;
    header->response.opcode = c->binary_header.request.opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = (uint8_t)hdr_len;
    header->response.datatype = (uint8_t)MEMCACHED_BINARY_RAW_BYTES;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = htonl(c->binary_header.request.opaque);
    header->response.cas = htonll(c->binary_header.request.cas);
}

void replyMemcachedBinaryResponse(client *c, void *d, int hlen, int keylen, int dlen) {
    robj *biob = createMemcachedBinaryResponse(c, d, hlen, keylen, dlen);
    addReplyMemcached(c, biob);
    decrRefCount(biob);
}

void replyMemcachedBinaryError(client *c, memcachedBinaryResponseStatus err,  const char *errstr) {
    size_t len;
    if (!errstr) {
        switch (err) {
            case MEMCACHED_BINARY_RESPONSE_ENOMEM:
                errstr = "Out of memory";
                break;
            case MEMCACHED_BINARY_RESPONSE_UNKNOWN_COMMAND:
                errstr = "Unknown command";
                break;
            case MEMCACHED_BINARY_RESPONSE_KEY_ENOENT:
                errstr = "Not found";
                break;
            case MEMCACHED_BINARY_RESPONSE_EINVAL:
                errstr = "Invalid arguments";
                break;
            case MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS:
                errstr = "Data exists for key.";
                break;
            case MEMCACHED_BINARY_RESPONSE_E2BIG:
                errstr = "Too large.";
                break;
            case MEMCACHED_BINARY_RESPONSE_DELTA_BADVAL:
                errstr = "Non-numeric server-side value for incr or decr";
                break;
            case MEMCACHED_BINARY_RESPONSE_NOT_STORED:
                errstr = "Not stored.";
                break;
            case MEMCACHED_BINARY_RESPONSE_AUTH_ERROR:
                errstr = "Auth failure.";
                break;
            case MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR:
                errstr = "Internal error.";
                break;
            case MEMCACHED_BINARY_RESPONSE_TEMPORARY_FAILURE_ERROR:
                errstr = "Temporary failure.";
                break;
            default:
                errstr = "UNHANDLED ERROR";
                break;
        }
    }

    len = strlen(errstr);
    len = len > MAX_ERROR_LEN ? MAX_ERROR_LEN : len; 
    char buf[sizeof(memcachedBinaryResponseHeader) + len];
    addMemcachedBinaryHeader(c, err, 0, 0, len, buf);
    if (len > 0) {
        char *start = buf + sizeof(memcachedBinaryResponseHeader);
        memcpy(start, errstr, len);
    }
    addReplyStringMemcached(c, buf, sizeof(memcachedBinaryResponseHeader) + len);
}

static void replyMemcachedQuitWrapper(client *c, const char *err) {
    UNUSED(err);
    if (c->reqtype == PROTO_REQ_ASCII) {
        addReplyMemcached(c, memcached_shared.ok);
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    } else { // binary
        if (!(c->flags & CLIENT_NO_REPLY)) {
            char buf[sizeof(memcachedBinaryResponseHeader)];
            addMemcachedBinaryHeader(c, 0, 0, 0, 0, buf);
            addReplyStringMemcached(c, buf, sizeof(memcachedBinaryResponseHeader));
            c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        } else {
            // close connect immediately
            freeClient(c);
        }
    }
}

/* Reply err information for writing or reading no rw permission redis instance */
static void replyRWErrorWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReplyError(c, (char *)err);
            break;
        case PROTO_REQ_ASCII:
            addReplyMemcached(c, memcached_shared.norw);
            break;
        default:
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR,  NULL);
    }
}

/* This function deals with unknown commands
 * and reply proper error informations */
static void replyUnknownErrWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReplyErrorFormat(c, "unknown command '%s'", err);
            break;
        case PROTO_REQ_BINARY:
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_UNKNOWN_COMMAND,  NULL);
            break;
        default:
            addReplyMemcached(c, memcached_shared.err);
    }
}

/* Reply no auth error */
static void replyNoAuthWrapper(client *c, const char *err) {
    UNUSED(err);
    robj *obj;
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
        case PROTO_REQ_ASCII:
            shared(c, noautherr, obj);
            addReplyMemcached(c, obj);
            break;
        default:
            /* If no authenticated, when the request is bianry, 
             * the connection will be closed after reply an error; 
             */
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_AUTH_ERROR, NULL);
            memcachedSetProtocolError(c, 0);
    }
}

/* Reply error information for out of memory */
static void replyOomWrapper(client *c, const char *err) {
    UNUSED(err);
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReply(c, shared.oomerr);
            break;
        case PROTO_REQ_ASCII:
            addReplyMemcached(c, memcached_shared.oomerr);
            break;
        default:
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_ENOMEM,  NULL);
    }
}

static void replyMemcachedMasterdownerrWrapper(client *c, const char *err) {
    UNUSED(err);
    if (c->reqtype == PROTO_REQ_ASCII) {
        addReplyMemcached(c, memcached_shared.masterdownerr);
    } else {
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR,  NULL);
    }
}

static void replyMemcachedBgsaveErrWrapper(client *c, const char *err) {
    UNUSED(err);
    if (c->reqtype == PROTO_REQ_ASCII) {
        addReplyMemcached(c, memcached_shared.bgsaveerr);
    } else {
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR,  NULL);
    }
}

static void replyMemcachedNoreplicaserrWrapper(client *c, const char *err) {
    UNUSED(err);
    if (c->reqtype == PROTO_REQ_ASCII) {
        addReplyMemcached(c, memcached_shared.noreplicaserr);
    } else {
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR,  NULL);
    }
}

/* Reply error information for slave is in bad condition */
static void replyRoslaveerrWrapper(client *c, const char *err) {
    UNUSED(err);
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReply(c, shared.roslaveerr);
            break;
        case PROTO_REQ_ASCII:
            addReplyMemcached(c, memcached_shared.roslaveerr);
            break;
        default:
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR,  NULL);
    }
}

static void replyMemcachedLoadingerrWrapper(client *c, const char *err) {
    UNUSED(err);
    if (c->reqtype == PROTO_REQ_ASCII) {
        addReplyMemcached(c, memcached_shared.loadingerr);
    } else {
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR,  NULL);
    }
}

/* Reply error information for writing an read only instance */
static void replyRoerrWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReplyError(c, (char*)err);
            break;
        case PROTO_REQ_ASCII:
            addReplyMemcached(c, memcached_shared.roerr);
            break;
        case PROTO_REQ_BINARY:
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_TEMPORARY_FAILURE_ERROR,  "READONLY You can't write against a read only instance.");
            break;
        default:
            memcachedSetProtocolError(c, 0);
    }
}

static void replyMemcachedServerErrWrapper(client *c, const char *err) {
    UNUSED(err);
    if (c->reqtype == PROTO_REQ_ASCII) {
        addReplyMemcached(c, memcached_shared.interr);
    } else {
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_INTERNAL_ERROR,  NULL);
    }
}

/* Reply error information for writing aof err */
static void replyServerWritingAOFErrWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReplySds(c, 
                    sdscatprintf(sdsempty(),"-MISCONF Errors writing to the AOF file: %s\r\n",
                        strerror(server.aof_last_write_errno)));
            break;
        default:
            replyMemcachedServerErrWrapper(c, err);
    }
}

/* Reply error information for openning aof err */
static void replyServerOpenningAOFErrWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReplySds(c, 
                     sdscatprintf(sdsempty(),"-MISCONF Errors opening AOF file\r\n"));
            break;
        default:
            replyMemcachedServerErrWrapper(c, err);
    }
}

/* Reply error information for back ground save error */
static void replyBgsaveErrWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReply(c, shared.bgsaveerr);
            break;
        default:
            replyMemcachedBgsaveErrWrapper(c, err);
    }
}

/* Reply error information for having no enough good slaves for replica */
static void replyNoreplicasErrWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReply(c, shared.noreplicaserr);
            break;
        default:
            replyMemcachedNoreplicaserrWrapper(c, err);
    }
}

/* Reply error information for master gone away */
static void replyMasterdownerrWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReply(c, shared.masterdownerr);
            break;
        default:
            replyMemcachedMasterdownerrWrapper(c, err);
    }
}

/* Reply error information for dealing with an loading data instance */
static void replyLoadingerrWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReply(c, shared.loadingerr);
            break;
        default:
            replyMemcachedLoadingerrWrapper(c, err);
    }
}

/* Reply to QUIT command */
static void replyQuitWrapper(client *c, const char *err) {
    switch (c->reqtype) {
        case PROTO_REQ_INLINE:
        case PROTO_REQ_MULTIBULK:
            addReply(c, shared.ok);
            c->flags |= CLIENT_CLOSE_AFTER_REPLY;
            break;
        default:
            replyMemcachedQuitWrapper(c, err);
    }
}

/* wrapper functions for dealing redis or memcached errors and informations */
void initHelpWrappers(struct redisServer *server, struct helpWrapper *wrappers) {
    UNUSED(server);
    wrappers->replyNoAuthErr = replyNoAuthWrapper;
    wrappers->replyUnknownCommandErr = replyUnknownErrWrapper;
    wrappers->replyDisableRWErr = replyRWErrorWrapper;
    wrappers->replyReadOnlyErr = replyRoerrWrapper;
    wrappers->replyRoslaveErr = replyRoslaveerrWrapper;
    wrappers->replyOomErr = replyOomWrapper;
    wrappers->replyWritingAofErr = replyServerWritingAOFErrWrapper;
    wrappers->replyOpenningAofErr = replyServerOpenningAOFErrWrapper;
    wrappers->replyBgsaveErr = replyBgsaveErrWrapper; //
    wrappers->replyNoreplicasErr = replyNoreplicasErrWrapper;
    wrappers->replyMasterdownErr = replyMasterdownerrWrapper;
    wrappers->replyLoadingErr = replyLoadingerrWrapper;
    wrappers->replyQuit = replyQuitWrapper;
}

typedef enum memcachedBinarySubState {
    MEMCACHED_BINARY_SUB_STATE_SET_HEADER,
    MEMCACHED_BINARY_SUB_STATE_CAS_HEADER,
    MEMCACHED_BINARY_SUB_STATE_SET_VALUE,
    MEMCACHED_BINARY_SUB_STATE_GET_KEY,
    MEMCACHED_BINARY_SUB_STATE_READING_STATE,
    MEMCACHED_BINARY_SUB_STATE_DEL_HEADER,
    MEMCACHED_BINARY_SUB_STATE_INCR_HEADER,
    MEMCACHED_BINARY_SUB_STATE_READ_FLUSH_EXPTIME,
    MEMCACHED_BINARY_SUB_STATE_READING_SASL_AUTH,
    MEMCACHED_BINARY_SUB_STATE_READING_SASL_AUTH_DATA,
    MEMCACHED_BINARY_SUB_STATE_READING_TOUCH_KEY,
    MEMCACHED_BINARY_SUB_STATE_READING_APPEND_HEADER
} memcachedBinarySubState;

typedef struct token_s {
    char *value;
    size_t length;
} token_t;

#define MAX_TOKENS 8
#define KEY_TOKEN 1
/* parse one item in querybuf, for example, the querybuf is like:
 * get key1 key2 key3 key4...\0
 * when parsed, the token's value will point to the key's address in querybuf 
 * and the length is the key's length
 */
static inline char* parseOneItem(char *querybuf, const size_t len, token_t *token, const char c) {
    char *s, *e;
    s = e = querybuf;
    size_t i = 0;
    token->length = 0;
    for (i = 0; i < len; i++) {
        if (*e == c) {
            if (e > s) {
                token->value = s;
                token->length = e - s;
                return e;
            }
            s = e + 1;
        }
        ++e;
    }
    
    if (e > s) {
        token->value = s;
        token->length = e - s;
    }
    return e;
}

/* This function is used to parse the reqeust separated by space;
 * the function parse at most max_tokens items at one time and the 
 * last item is just a mark.
 * For example, 
 * the request buffer is lke: get key1 key2 key3, after the function called the tokens seems like
 * [0].value point the get's address in the buffer, [1].value point the key1's address and so on, 
 * the [max_tokens - 1].value point NULL or the beginning address that we will parse the next time. */
static size_t tokenizeRequestBuffer(char *command, const int len, token_t *tokens, const size_t max_tokens) {
    size_t ntokens = 0;
    char *start = command;
    size_t left = len;
    char *endptr = command;
    while (1) {
        endptr = parseOneItem(endptr, left, &tokens[ntokens], ' '); 
        if (tokens[ntokens].length > 0) {
            ++ntokens;
        }
        
        if (*endptr == '\0') break;
        *endptr = '\0';
        ++endptr;
        if (ntokens + 1 == max_tokens) {
            break;
        }
        left = len - (endptr - start);
    }

    tokens[ntokens].value = (*endptr =='\0') ? NULL : endptr;
    tokens[ntokens++].length = 0;
    return ntokens;
}

/* 0 means never expire */
static inline rel_time_t realTime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime 
     * memcached treat minus time as invalid expire time, but ocs treat it is valid,
     * just like ocs here */
    if (exptime == 0) return 0; /* 0 means never expire */
    if (exptime < 0) return MEMCACHED_MAX_EXP_TIME; /* expire */

    if (exptime > REALTIME_MAXDELTA) {
        if (exptime <= server.unixtime) {
            return (rel_time_t) MEMCACHED_MAX_EXP_TIME;
        }
        return (rel_time_t)(exptime - server.unixtime);
    } else {
        return (rel_time_t)exptime;
    }
}

#define NO_REPLY "noreply"
#define NO_REPLY_LEN (sizeof(NO_REPLY) - 1)
static inline int setNoReplyMaybe(client *c, token_t *tokens, const size_t ntokens) {
    size_t no_reply_index;
    if (ntokens < 2) {
        return c->flags;
    }
    no_reply_index = ntokens - 2;
    c->flags &=(~CLIENT_NO_REPLY);
    if (tokens[no_reply_index].value &&
            tokens[no_reply_index].length == NO_REPLY_LEN && 
            !strncmp(tokens[no_reply_index].value, NO_REPLY, NO_REPLY_LEN)) {
        c->flags |= CLIENT_NO_REPLY;
    }
    return c->flags;
}

/* Create memcached string object just for adding MEMCACHED_VALUE_ITEM_HEAD_LEN space for 
 * storing cas and flags */
static robj *createMemcachedStringObject(const char *buf, size_t len) {
    robj *o = createObject(OBJ_STRING, sdsfromMemcachedStringbuffer(buf, len, MEMCACHED_VALUE_ITEM_HEAD_LEN));
    return o;
}

/*
 * parse the get or gets requests, the format of get and gets request are like
 * get key1 key2 key3 ...\r\n;
 * gets key1 key2 key3 ...\r\n;
 * when we meet a large key, just reply bad format, range the request buffer,
 * then deal next requests;
 * item_tokens contains the already parsed items, the last item is a mark that indicate if 
 * request buffer is parsed completely or not;
 * the last item's length is always 0, if its value == NULL, 
 * it means parsed completely or else not. 
 */
int parseMemcachedGetCommand(client *c, const size_t reqlen, void *item_tokens, const size_t item_ntokens, const int hint, const int pos) {
    UNUSED(hint);
    token_t *key_token = (token_t *)item_tokens, *tokens = key_token;
    size_t ntokens = item_ntokens, totol_tokens = ntokens - 1;/*the last item is only a mark*/
    if (c->argv) zfree(c->argv);
    c->argv = zmalloc(sizeof(robj*)*totol_tokens); 
    c->argc = 0;

    do {
        if (key_token->length > MEMCACHED_KEY_MAX_LENGTH) {
            addReplyMemcached(c, memcached_shared.badfmt);
            sdsrange(c->querybuf, pos, -1);
            return C_AGAIN;
        }

        while (key_token->length > 0) {
            c->argv[c->argc++] = createStringObject(key_token->value, key_token->length);   
            ++key_token;
        }

        if (key_token->value != NULL) {
            ntokens = tokenizeRequestBuffer(key_token->value, reqlen - (key_token->value - c->querybuf), tokens,  MAX_TOKENS);
            key_token = tokens;
            totol_tokens += (ntokens - 1);
            c->argv = zrealloc(c->argv, sizeof(robj*)*totol_tokens);
        }

    } while (key_token->value != NULL);

    sdsrange(c->querybuf, pos, -1);
    return C_OK;
}

/* Process request like: set key flags expire vlen [no reply]\r\n<data>\r\n */
/* Don't change this macro, it must larger than REDIS_SHARED_INTEGERS */
#define INITIAL_VAL 10000000
#define HAS_CAS_ITEM_NUM 6
#define NO_CAS_ITEM_NUM 5

int parseMemcachedUpdateCommand(client *c, const size_t len, void *item_tokens, const size_t ntokens, const int handle_cas, const int pos) {
    UNUSED(len);
    token_t* tokens = (token_t *)item_tokens;
    uint32_t flags;
    int32_t expire_time_int;
    int vlen, multibulklen = handle_cas ? HAS_CAS_ITEM_NUM : NO_CAS_ITEM_NUM;
    size_t need_len;
    uint64_t req_cas_id = 0;

    /* check parameters */
    if (((!handle_cas && ntokens != 6 ) || (handle_cas && ntokens != 8)) && ntokens != 7) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }
    /* check for no reply */
    setNoReplyMaybe(c, tokens, ntokens);    

    if (tokens[KEY_TOKEN].length > MEMCACHED_KEY_MAX_LENGTH) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (safe_strtoul(tokens[2].value, &flags) == C_ERR  || 
                safe_strtol(tokens[3].value, &expire_time_int) == C_ERR ||
                safe_strtol(tokens[4].value, (int32_t *)&vlen) == C_ERR) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (handle_cas) {
        if (safe_strtoull(tokens[5].value, &req_cas_id) == C_ERR) {
            addReplyMemcached(c, memcached_shared.badfmt);
            sdsrange(c->querybuf, pos, -1);
            return C_AGAIN;
        }
    }

    if (vlen < 0 || (tokens[KEY_TOKEN].length + vlen) > server.max_memcached_read_request_length) {
        // invalid vlen, nothing can be done, 
        // just close the connection;
        addReplyMemcached(c, memcached_shared.badfmt);
        memcachedSetProtocolError(c, pos);
        return C_ERR;
    }

    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*multibulklen);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    c->argv[c->argc++] = createStringObject(tokens[KEY_TOKEN].value, tokens[KEY_TOKEN].length);
    c->argv[c->argc++] = createStringObjectFromLongLong(flags); 
    c->argv[c->argc++] = createStringObjectFromLongLong(realTime(expire_time_int)); 
    if (handle_cas) {
        /* cas is uint64_t, createStringObjectFromLongLong use long long as its parameter type;
         * so it has the risk to overflow
         * we first create a little large number object and then change it; 
         * just a little tricky. 
         * Use a large number to avoid use sharded object integer */
        c->argv[c->argc++] = createStringObjectFromLongLong(INITIAL_VAL); 
        c->argv[c->argc - 1]->ptr = (void *)(unsigned long long)req_cas_id; 
    }

    need_len = pos + vlen + 2;
    if (need_len <= sdslen(c->querybuf)) { /* get data completely */
        c->argv[c->argc++] = createMemcachedStringObject(c->querybuf + pos, vlen);
        sdsrange(c->querybuf, need_len, -1);
        return C_OK;
    } 

    sdsrange(c->querybuf, pos, -1);
    c->bulklen = vlen;
    c->multibulklen = 1;
    return C_ERR;
}

/* process the decrement and the increment commmand
 * decr key value [noreply]\r\n
 * incr key value [noreply]\r\n */
int parseMemcachedArithmeticCommand(client *c, const size_t len, void *item_tokens, const size_t ntokens, const int hint, const int pos) {
    UNUSED(len);
    UNUSED(hint);
    token_t *tokens = (token_t *)item_tokens;
    uint64_t delta;
    if (ntokens != 4 && ntokens != 5) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    setNoReplyMaybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > MEMCACHED_KEY_MAX_LENGTH) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (safe_strtoull(tokens[2].value, &delta) == C_ERR) {
        addReplyMemcached(c, memcached_shared.invalid_delta);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*3);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    c->argv[c->argc++] = createStringObject(tokens[KEY_TOKEN].value, tokens[KEY_TOKEN].length);
    // tricky skill
    c->argv[c->argc++] = createStringObjectFromLongLong(INITIAL_VAL); 
    c->argv[c->argc - 1]->ptr = (void *)delta; 
    sdsrange(c->querybuf, pos, -1);
    return C_OK;
}

/*
 * parse the delete command
 * It's weird, delete may have 3 parameters, but one never be used; 
 */ 
int parseMemcachedDeleteCommand(client *c, const size_t len, void *item_tokens, const size_t ntokens, const int hint, const int pos) {
    UNUSED(len);
    UNUSED(hint);
    token_t *tokens = item_tokens;
    if (ntokens < 3 || ntokens > 5)  {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }
    
    if (ntokens > 3) {
        int hold_is_zero = strcmp(tokens[KEY_TOKEN+1].value, "0") == 0;
        int sets_noreply = setNoReplyMaybe(c, tokens, ntokens);
        int valid = (ntokens == 4 && (hold_is_zero || sets_noreply))
            || (ntokens == 5 && hold_is_zero && sets_noreply);
        if (!valid) {
            addReplyMemcached(c, memcached_shared.badfmt);
            sdsrange(c->querybuf, pos, -1);
            return C_AGAIN;
        }
    }

    if (tokens[KEY_TOKEN].length > MEMCACHED_KEY_MAX_LENGTH) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*2);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    c->argv[c->argc++] = createStringObject(tokens[KEY_TOKEN].value, tokens[KEY_TOKEN].length);
    sdsrange(c->querybuf, pos, -1);
    return C_OK;
}

/* process the minfo command:
 * minfo [section]\r\n
 */
int parseMemcachedMinfoCommand(client *c, const size_t len, void *item_tokens, const size_t ntokens, const int hint, const int pos) {
    UNUSED(len);
    UNUSED(hint);
    char *section = NULL;
    size_t length = 0;
#define DEFAULT "default"

    token_t *tokens = item_tokens;
    if (ntokens != 2 && ntokens != 3)  {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }
    
    if (ntokens == 2) {
        section = DEFAULT;
        length = sizeof(DEFAULT) - 1;
    } else {
        section = tokens[1].value;
        length =  tokens[1].length;
    }

    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*2);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    c->argv[c->argc++] = createStringObject(section, length);
    sdsrange(c->querybuf, pos, -1);
    return C_OK;
}

/* process the mscan command:
 * mscan cursor COUNT count Match <byte>\r\n<data>\r\n
 */
int parseMemcachedMscanCommand(client *c, const size_t len, void *item_tokens, const size_t ntokens, const int hint, const int pos) {
    UNUSED(len);
    UNUSED(hint);
    token_t* tokens = (token_t *)item_tokens;
    int vlen;
    uint64_t cursor = 0, count = 0;
    size_t need_len;

    /* check parameters */
    if (ntokens != 7) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (safe_strtoull(tokens[1].value, &cursor) == C_ERR  || 
                strcasecmp(tokens[2].value, "count") || 
                safe_strtoull(tokens[3].value, &count) == C_ERR ||
                strcasecmp(tokens[4].value, "match") ||
                safe_strtol(tokens[5].value, (int32_t *)&vlen) == C_ERR ||
                vlen <= 0 || vlen > 1024) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*4);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    c->argv[c->argc++] = createStringObjectFromLongLong(cursor); 
    c->argv[c->argc++] = createStringObjectFromLongLong(count); 

    need_len = pos + vlen + 2;
    if (need_len <= sdslen(c->querybuf)) { /* get data completely */
        c->argv[c->argc++] = createMemcachedStringObject(c->querybuf + pos, vlen);
        sdsrange(c->querybuf, need_len, -1);
        return C_OK;
    } 

    sdsrange(c->querybuf, pos, -1);
    c->bulklen = vlen;
    c->multibulklen = 1;
    return C_ERR;
}

/*
 * process the touch command:
 * touch key expire [noreply]\r\n
 */
int parseMemcachedTouchCommand(client *c, const size_t len, void *item_tokens, const size_t ntokens, const int hint, const int pos) {
    UNUSED(len);
    UNUSED(hint);
    token_t *tokens = item_tokens;
    int32_t exptime_int = 0;
    if (ntokens != 4 && ntokens != 5) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }
    
    setNoReplyMaybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > MEMCACHED_KEY_MAX_LENGTH) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (safe_strtol(tokens[2].value, &exptime_int) == C_ERR) {
        addReplyMemcached(c, memcached_shared.invalidexp);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*3);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    c->argv[c->argc++] = createStringObject(tokens[KEY_TOKEN].value, tokens[KEY_TOKEN].length);
    c->argv[c->argc++] = createStringObjectFromLongLong(realTime(exptime_int)); 
    sdsrange(c->querybuf, pos, -1);
    return C_OK;
}

/*
 * process the flush_all command:
 * flush_all [expire] [noreply]\r\n
 */
int parseMemcachedFlushCommand(client *c, const size_t len, void *itokens, const size_t ntokens, const int hint, const int pos) {
    UNUSED(len);
    UNUSED(hint);
    token_t *tokens = itokens;
    int32_t exptime_int = 0;
    if (!(ntokens >=2 && ntokens <= 4)) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }
    
    setNoReplyMaybe(c, tokens, ntokens);

    if (ntokens != ((c->flags & CLIENT_NO_REPLY) ? 3 : 2)) {
        if (safe_strtol(tokens[1].value, &exptime_int) == C_ERR) {
            addReplyMemcached(c, memcached_shared.badfmt);
            sdsrange(c->querybuf, pos, -1);
            return C_AGAIN;
        }
    }

    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*2);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    c->argv[c->argc++] = createStringObjectFromLongLong(realTime(exptime_int)); 
    sdsrange(c->querybuf, pos, -1);
    return C_OK;
}

/*
 * process version and quit command
 */
int parseMemcachedVersionQuitCommand(client *c, const size_t len, void *item_tokens, const size_t ntokens, const int hint, const int pos) {
    UNUSED(len);
    UNUSED(hint);
    token_t *tokens = (token_t *)item_tokens;
    if (ntokens != 2) {
        // protocol error
        addReplyMemcached(c, memcached_shared.err);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    // add fake command
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *));
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    sdsrange(c->querybuf, pos, -1);
    return C_OK;
}

/*
 * process auth command
 * auth user password\r\n
 */
int parseMemcachedAuthCommand(client *c, const size_t len, void *item_tokens, const size_t ntokens, const int hint, const int pos) {
    UNUSED(len);
    UNUSED(hint);
    token_t *tokens = (token_t *)item_tokens;
    if (ntokens != 4) {
        addReplyMemcached(c, memcached_shared.badfmt);
        sdsrange(c->querybuf, pos, -1);
        return C_AGAIN;
    }

    // add fake command
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *) * 3);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(tokens[0].value, tokens[0].length);
    c->argv[c->argc++] = createStringObject(tokens[1].value, tokens[1].length);
    c->argv[c->argc++] = createStringObject(tokens[2].value, tokens[2].length);
    sdsrange(c->querybuf, pos, -1);
    return C_OK;
}

/* Ascii command include memcached text and redis inline command, 
 * We don't know which one is, just try to guess. */
static int dispatchMemcachedAsciiCommand(client *c, char *newline) {
    token_t tokens[MAX_TOKENS + 1], token;
    size_t ntokens, nulllen;
    int ret = C_AGAIN, pos = newline - c->querybuf + 1;

    if (newline - c->querybuf > 1 && *(newline - 1) == '\r') {
        newline--;
    }
    nulllen = newline - c->querybuf;

    /* first find the command */
    parseOneItem(c->querybuf, nulllen, &token, ' ');

    if (token.length > 0) {
        struct redisCommand *redis_cmd = NULL;
        sds cmd = sdsnewlen(token.value, token.length);
        /* first guss it's memcached text request */
        struct memcachedCommand *memcached_cmd = lookupMemcachedCommand(cmd);
        if (memcached_cmd == NULL) {
            redis_cmd = lookupCommand(cmd);
        }
        sdsfree(cmd);
        if (!memcached_cmd && !redis_cmd) {
            /* neither memcached text nor redis inline request */
            addReplyMemcached(c, memcached_shared.err);
            sdsrange(c->querybuf, pos, -1);
            return ret;
        }

        if (redis_cmd) {
            /* redis inline request */
            c->cmd = c->lastcmd = redis_cmd;
            c->reqtype = PROTO_REQ_INLINE;
            ret = processInlineBuffer(c);
            return ret;
        }
        
        /* deal memcached text request */
        c->cmd = c->lastcmd = (struct redisCommand *)memcached_cmd;
        *newline = '\0';    
        /* parse request line */
        ntokens = tokenizeRequestBuffer(c->querybuf, nulllen, tokens, MAX_TOKENS); 
        ret = memcached_cmd->proc(c, nulllen, tokens, ntokens, memcached_cmd->has_cas, pos);
    } else {
        /* we try to find a key in the request line, but find nothing.
         * for memcache just reply error */
        addReplyMemcached(c, memcached_shared.err);
        sdsrange(c->querybuf, pos, -1);
    }

    return ret;
}

/* parse the set request header */ 
#define BIN_REQ_SET_HEADER_LEN  sizeof(((memcachedBinaryRequestSet *)16)->bytes)
#define BIN_REQ_SET_EXTRA_LEN  sizeof(((memcachedBinaryRequestSet *)16)->message.body)

static void memcachedBinaryReadingSetRequest(client *c) {
    memcachedBinaryRequestSet *header = (memcachedBinaryRequestSet *)c->querybuf;
    uint8_t extlen = c->binary_header.request.extlen;
    uint16_t keylen =  c->binary_header.request.keylen;
    uint32_t bodylen =  c->binary_header.request.bodylen; 
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*5);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(c->cmd->name, strlen(c->cmd->name));
    c->argv[c->argc++] = createStringObject(c->querybuf + BIN_REQ_SET_HEADER_LEN,  keylen);
    c->argv[c->argc++] = createStringObjectFromLongLong(ntohl(header->message.body.flags)); 
    c->argv[c->argc++] = createStringObjectFromLongLong(realTime(ntohl(header->message.body.expiration))); 
    c->argv[c->argc++] = createMemcachedStringObject(c->querybuf + BIN_REQ_SET_HEADER_LEN + keylen, bodylen - (extlen + keylen)); 
}

/* parse append and prepend header */
#define BIN_REQ_APPEND_HEADER_LEN  sizeof(((memcachedBinaryRequestAppend *)16)->bytes)

static void memcachedBinaryReadingAppendRequest(client *c) {
    uint16_t keylen =  c->binary_header.request.keylen;
    uint32_t bodylen =  c->binary_header.request.bodylen; 
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*3);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(c->cmd->name, strlen(c->cmd->name));
    c->argv[c->argc++] = createStringObject(c->querybuf + BIN_REQ_APPEND_HEADER_LEN,  keylen);
    c->argv[c->argc++] = createMemcachedStringObject(c->querybuf + BIN_REQ_APPEND_HEADER_LEN + keylen, bodylen - keylen); 
}

static void memcachedBinaryReadingGetRequest(client *c) {
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*2);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(c->cmd->name, strlen(c->cmd->name));
    c->argv[c->argc++] = createStringObject(c->querybuf + sizeof(memcachedBinaryRequestGet), c->binary_header.request.keylen);
}

#define BIN_REQ_TOUCH_HEADER_LEN  sizeof(((memcachedBinaryRequestTouch *)16)->bytes)
#define BIN_REQ_TOUCH_EXTRA_LEN  sizeof(((memcachedBinaryRequestTouch *)16)->message.body)
/* parse binary touch request. */
static void memcachedBinaryReadTouchRequest(client *c) {
    memcachedBinaryRequestTouch *header = (memcachedBinaryRequestTouch *)c->querybuf;
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*3);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(c->cmd->name, strlen(c->cmd->name));
    c->argv[c->argc++] = createStringObject(c->querybuf + BIN_REQ_TOUCH_HEADER_LEN, c->binary_header.request.keylen);
    c->argv[c->argc++] = createStringObjectFromLongLong(realTime(ntohl(header->message.body.expiration))); 
}

/*
 * parse binary delete reqeust
 * delete key
 */
static void memcachedBinaryReadingDelRequest(client *c) {
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *) * 2);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(c->cmd->name, strlen(c->cmd->name));
    c->argv[c->argc++] = createStringObject(c->querybuf + sizeof(memcachedBinaryRequestDelete), c->binary_header.request.keylen);
}

#define BIN_REQ_INCR_HEADER_LEN  sizeof(((memcachedBinaryRequestIncr *)16)->bytes)
static void memcachedBinaryReadingIncrDecrRequest(client *c) {
    memcachedBinaryRequestIncr *header = (memcachedBinaryRequestIncr *)c->querybuf;
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *)*5);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(c->cmd->name, strlen(c->cmd->name));
    c->argv[c->argc++] = createStringObject(c->querybuf + BIN_REQ_INCR_HEADER_LEN, c->binary_header.request.keylen);
    /* createStringObjectFromLongLong use long long as it's parameter
     * but we use uint64_t, so it has the risk to overflow
     * we do a tricky skill to avoid this */
    c->argv[c->argc++] = createStringObjectFromLongLong(INITIAL_VAL); 
    c->argv[c->argc - 1]->ptr = (void *)(ntohll(header->message.body.delta)); 
    c->argv[c->argc++] = createStringObjectFromLongLong(INITIAL_VAL); 
    c->argv[c->argc - 1]->ptr = (void *)(ntohll(header->message.body.initial)); 
    /* When the command is incr or decr, don't change the expiration time */
    c->argv[c->argc++] = createStringObjectFromLongLong(ntohl(header->message.body.expiration)); 
}

static void binaryReadFlushRequest(client *c) {
    time_t expiration = 0;
    if (c->binary_header.request.extlen > 0) {
        memcachedBinaryRequestFlush *header = (memcachedBinaryRequestFlush *)c->querybuf;
        expiration = realTime(ntohl(header->message.body.expiration));
    }

    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *) * 2);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(c->cmd->name, strlen(c->cmd->name));
    c->argv[c->argc++] = createStringObjectFromLongLong(expiration); 
}

/* get sasl key data, data may not exist */
static void memcachedBinaryCompleteSaslAuthData(client *c) {
    uint16_t keylen = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen - keylen;
    uint32_t multibulk = bodylen > 0 ? 3 : 2;
    if (c->argv) zfree(c->argv);    
    c->argv = zmalloc(sizeof(robj *) * multibulk);
    c->argc = 0;
    c->argv[c->argc++] = createStringObject(c->cmd->name, strlen(c->cmd->name));
    c->argv[c->argc++] = createStringObject(c->querybuf + sizeof(memcachedBinaryRequestHeader), keylen);
    if (bodylen > 0) {
        c->argv[c->argc++] = createStringObject(c->querybuf + sizeof(memcachedBinaryRequestHeader) + keylen, bodylen);
    }
}

static void memcachedBinaryParseBuffer(client *c, enum memcachedBinarySubState sub_state, uint8_t extlen) {
    UNUSED(extlen);
    switch (sub_state)
    {
       case MEMCACHED_BINARY_SUB_STATE_SET_HEADER:
           memcachedBinaryReadingSetRequest(c);
           break;
       case MEMCACHED_BINARY_SUB_STATE_GET_KEY:
           memcachedBinaryReadingGetRequest(c);
           break;
       case MEMCACHED_BINARY_SUB_STATE_READING_TOUCH_KEY:
           memcachedBinaryReadTouchRequest(c);
           break;
       case MEMCACHED_BINARY_SUB_STATE_DEL_HEADER:
           memcachedBinaryReadingDelRequest(c);
           break;
       case MEMCACHED_BINARY_SUB_STATE_INCR_HEADER:
           memcachedBinaryReadingIncrDecrRequest(c);
           break;
       case MEMCACHED_BINARY_SUB_STATE_READ_FLUSH_EXPTIME:
           binaryReadFlushRequest(c);
           break;
       case MEMCACHED_BINARY_SUB_STATE_READING_SASL_AUTH_DATA:
           memcachedBinaryCompleteSaslAuthData(c);
           break;
       case MEMCACHED_BINARY_SUB_STATE_READING_APPEND_HEADER:
           memcachedBinaryReadingAppendRequest(c);
           break;
       default:
           break;
    }
}

/*
 * change quiet command to its corresponding command
 */
static unsigned short changeQuietCmd2Cmd(client *c, unsigned short cmd) {
    c->flags |= CLIENT_NO_REPLY;
    switch (cmd) {
        case MEMCACHED_BINARY_CMD_SETQ:
            cmd = MEMCACHED_BINARY_CMD_SET;
            break;
        case MEMCACHED_BINARY_CMD_ADDQ:
            cmd = MEMCACHED_BINARY_CMD_ADD;
            break;
        case MEMCACHED_BINARY_CMD_REPLACEQ:
            cmd = MEMCACHED_BINARY_CMD_REPLACE;
            break;
        case MEMCACHED_BINARY_CMD_DELETEQ:
            cmd = MEMCACHED_BINARY_CMD_DELETE;
            break;
        case MEMCACHED_BINARY_CMD_INCREMENTQ:
            cmd = MEMCACHED_BINARY_CMD_INCREMENT;
            break;
        case MEMCACHED_BINARY_CMD_DECREMENTQ:
            cmd = MEMCACHED_BINARY_CMD_DECREMENT;
            break;
        case MEMCACHED_BINARY_CMD_QUITQ:
            cmd = MEMCACHED_BINARY_CMD_QUIT;
            break;
        case MEMCACHED_BINARY_CMD_FLUSHQ:
            cmd = MEMCACHED_BINARY_CMD_FLUSH;
            break;
        case MEMCACHED_BINARY_CMD_APPENDQ:
            cmd = MEMCACHED_BINARY_CMD_APPEND;
            break;
        case MEMCACHED_BINARY_CMD_PREPENDQ:
            cmd = MEMCACHED_BINARY_CMD_PREPEND;
            break;
        case MEMCACHED_BINARY_CMD_GETQ:
            cmd = MEMCACHED_BINARY_CMD_GET;
            break;
        case MEMCACHED_BINARY_CMD_GETKQ:
            cmd = MEMCACHED_BINARY_CMD_GETK;
            break;
        case MEMCACHED_BINARY_CMD_GATQ:
            cmd = MEMCACHED_BINARY_CMD_GAT;
            break;
        default:
            c->flags &= (~CLIENT_NO_REPLY);
    }
    return cmd;
}

/* Judge the request's validation and parse
 * it into argv format */
static int dispatchMemcachedBinaryCommand(client *c, char *newline) {
    UNUSED(newline); 
    int protocol_error = 0;
    unsigned short cmd;
    struct memcachedCommand* m_cmd;

    uint8_t extlen = c->binary_header.request.extlen;
    uint16_t keylen = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;
    cmd = c->binary_header.request.opcode;

    /* protocol error, close connection */
    if (keylen + extlen > bodylen) {
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_UNKNOWN_COMMAND, NULL);
        memcachedSetProtocolError(c, 0);
        return C_ERR;
    }
    
    if (cmd <= MEMCACHED_MAX_OPCODE) {
        m_cmd = &memcachedCommandTable[MEMCACHED_TEXT_REQUEST_NUM + cmd];
        c->cmd = c->lastcmd = (struct redisCommand *)m_cmd;
    }

    cmd = changeQuietCmd2Cmd(c, cmd);
    switch (cmd) {
        case MEMCACHED_BINARY_CMD_VERSION:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                if (c->argv) zfree(c->argv);    
                c->argv = zmalloc(sizeof(robj *));
                c->argc = 0;
                incrRefCount(memcached_shared.vercmd);
                c->argv[c->argc++] = memcached_shared.vercmd;
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_FLUSH:
            if (keylen == 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
                memcachedBinaryParseBuffer(c, MEMCACHED_BINARY_SUB_STATE_READ_FLUSH_EXPTIME, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_NOOP:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                if (c->argv) zfree(c->argv);    
                c->argv = zmalloc(sizeof(robj *));
                c->argc = 0;
                incrRefCount(memcached_shared.noopcmd);
                c->argv[c->argc++] = memcached_shared.noopcmd;
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_SET: /* FALLTHROUGH */
        case MEMCACHED_BINARY_CMD_ADD: /* FALLTHROUGH */
        case MEMCACHED_BINARY_CMD_REPLACE:
            if (extlen == 8 && keylen != 0 /* && must be bodylen >= (keylen + extlen) */) {
                memcachedBinaryParseBuffer(c, MEMCACHED_BINARY_SUB_STATE_SET_HEADER, 8);
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_GET:   /* FALLTHROUGH */
        case MEMCACHED_BINARY_CMD_GETK:
            if (extlen == 0 && bodylen == keylen && keylen > 0) {
                memcachedBinaryParseBuffer(c, MEMCACHED_BINARY_SUB_STATE_GET_KEY, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_DELETE:
            if (keylen > 0 && extlen == 0 && bodylen == keylen) {
                memcachedBinaryParseBuffer(c, MEMCACHED_BINARY_SUB_STATE_DEL_HEADER, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_INCREMENT:
        case MEMCACHED_BINARY_CMD_DECREMENT:
            if (keylen > 0 && extlen == 20 && bodylen == (keylen + extlen)) {
                memcachedBinaryParseBuffer(c, MEMCACHED_BINARY_SUB_STATE_INCR_HEADER, 20);
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_APPEND:
        case MEMCACHED_BINARY_CMD_PREPEND:
            if (keylen > 0 && extlen == 0) {
                memcachedBinaryParseBuffer(c, MEMCACHED_BINARY_SUB_STATE_READING_APPEND_HEADER, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_QUIT:
            if ((keylen == 0 && extlen == 0 && bodylen == 0)) {
                if (c->argv) zfree(c->argv);    
                c->argv = zmalloc(sizeof(robj *));
                c->argc = 0;
                incrRefCount(memcached_shared.quitcmd);
                c->argv[c->argc++] = memcached_shared.quitcmd;
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_SASL_LIST_MECHS:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                if (c->argv) zfree(c->argv);    
                c->argv = zmalloc(sizeof(robj *));
                c->argc = 0;
                incrRefCount(memcached_shared.sasl_list_cmd);
                c->argv[c->argc++] = memcached_shared.sasl_list_cmd;
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_SASL_AUTH:
        /* case MEMCACHED_BINARY_CMD_SASL_STEP: */
            if (extlen == 0 && keylen != 0) {
                memcachedBinaryParseBuffer(c, MEMCACHED_BINARY_SUB_STATE_READING_SASL_AUTH_DATA, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case MEMCACHED_BINARY_CMD_TOUCH:
        case MEMCACHED_BINARY_CMD_GAT:
            if (extlen == 4 && keylen != 0) {
                memcachedBinaryParseBuffer(c, MEMCACHED_BINARY_SUB_STATE_READING_TOUCH_KEY, 4);
            } else {
                protocol_error = 1;
            }
            break;
        default:
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_UNKNOWN_COMMAND, NULL);
            sdsrange(c->querybuf, bodylen + sizeof(memcachedBinaryRequestHeader), -1);
            return C_AGAIN;
    }

    if (protocol_error) {
        /* protocol error, close connection */
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_EINVAL, NULL);
        memcachedSetProtocolError(c, 0);
        return C_ERR;
    }
    sdsrange(c->querybuf, bodylen + sizeof(memcachedBinaryRequestHeader), -1);
    return C_OK;    
}

/* Release the free space of the robj, if need_cas_flag_header is true,
 * it means that we want MINUS_MEMCACHED_VALUE_ITEM_HEAD_LEN
 * available space for storing cas and flag, if it is false, 
 * just release all the free space. */
void releaseObjFreeSpace(robj *o, int need_cas_flag_header) {
    if (o == NULL) return;
    sds s = o->ptr;
    size_t len = sdslen(s), avail = sdsavail(s);
    if (len > 32 && avail > len / 10) {
        if (need_cas_flag_header) {
            if (avail > MEMCACHED_VALUE_ITEM_HEAD_LEN) {
                sdsIncrLen(s, MEMCACHED_VALUE_ITEM_HEAD_LEN);
                o->ptr = sdsRemoveFreeSpace(o->ptr);
                sdsIncrLen(o->ptr, MINUS_MEMCACHED_VALUE_ITEM_HEAD_LEN);
            }
        } else {
            o->ptr = sdsRemoveFreeSpace(o->ptr);
        }
    }
}

static int processMemcachedRequestBuffer(client *c) {
    char *newline = NULL;
    if (c->reqtype == PROTO_REQ_BINARY) {
        /* Check if we have the complete packet header */
        if (c->bulklen == -1) {
            if (sdslen(c->querybuf) < sizeof(memcachedBinaryRequestHeader)) {
                return C_ERR;
            }
            memcachedBinaryRequestHeader *req;
            req = (memcachedBinaryRequestHeader *)c->querybuf;
            c->binary_header = *req;
            /* Change network endian to host endian */
            c->binary_header.request.keylen = ntohs(req->request.keylen);
            c->binary_header.request.bodylen = ntohl(req->request.bodylen);
            c->binary_header.request.cas = ntohll(req->request.cas);
            c->binary_header.request.opaque = ntohl(req->request.opaque);
            if (c->binary_header.request.magic != MEMCACHED_BINARY_REQ ||
                    /* binprot supports 16bit keys, but internals are still 8bit */
                    c->binary_header.request.keylen > MEMCACHED_KEY_MAX_LENGTH ||
                    c->binary_header.request.bodylen >= (uint32_t)(server.max_memcached_read_request_length + c->binary_header.request.extlen)) {
                replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_EINVAL, NULL);
                memcachedSetProtocolError(c, 0);
                return C_ERR;
            }
            c->bulklen = c->binary_header.request.bodylen;
        }
        /* if we don't have completely data, just waiting read again */
        if (c->bulklen + (signed)sizeof(memcachedBinaryRequestHeader) > (signed)sdslen(c->querybuf)) {
            return C_ERR;
        }

        return dispatchMemcachedBinaryCommand(c, newline); 

    } else if (c->reqtype == PROTO_REQ_ASCII) {
        if (c->bulklen == -1) {
            newline = memchr(c->querybuf, '\n', sdslen(c->querybuf));
            if (!newline) {
                if (sdslen(c->querybuf) > 1024) {
                    /* we don't have a '\n' in the first k; there has to be a large multiget,
                     * if not we should just destroy the connection. */
                    char *ptr = c->querybuf;
                    while (*ptr == ' ') {
                        ++ptr;
                    }

                    if (ptr - c->querybuf > 100 || (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5))) {
                        memcachedSetProtocolError(c, 0);
                    }
                }
                return C_ERR;
            }

            return dispatchMemcachedAsciiCommand(c, newline); 

        } else {
            if (c->bulklen + 2 <= (signed)sdslen(c->querybuf)) {
                c->argv[c->argc++] = createMemcachedStringObject(c->querybuf, c->bulklen);
                sdsrange(c->querybuf, c->bulklen + 2, -1);
                return C_OK;
            }
        }
    }
    return C_ERR;
}

/* Process the request buffer, guess the protocol,
 * we treat the redis inline protocol as memcached text protocol first
 * then we use the lookup table to distinguish them */
int processMemcachedProtocolBuffer(client *c) {
    int ret = C_ERR;
    if (!c->reqtype) {
        if ((unsigned char)c->querybuf[0] == (unsigned char)MEMCACHED_BINARY_REQ) {
            c->reqtype = PROTO_REQ_BINARY;
        } else if (c->querybuf[0] == '*') {
            c->reqtype = PROTO_REQ_MULTIBULK;
        } else { 
            /* treat redis inline request as memcached text,
             * except requests from slave or from master,
             * they are just like each other:command  key\r\n */
            if (c->flags & (CLIENT_SLAVE | CLIENT_MASTER)) {
                c->reqtype = PROTO_REQ_INLINE;
            } else {
                c->reqtype = PROTO_REQ_ASCII;
            }
        }
    }

    if (c->reqtype == PROTO_REQ_BINARY || c->reqtype == PROTO_REQ_ASCII) {
        ret = processMemcachedRequestBuffer(c);
    } else {
        if (c->reqtype == PROTO_REQ_INLINE) {
            ret = processInlineBuffer(c);
        } else {
            ret = processMultibulkBuffer(c);    
        }
    } 
    return ret;
}

/* <cas:uint64><flags:uint32_t><data> */
size_t getMemcachedValueLength(robj *o) {
    memcachedValueItem *vptr = (memcachedValueItem *)o->ptr;
    return sdslen(o->ptr) - (vptr->data - (char*)o->ptr);
}

void convertMemcachedRawCmd2DelCmd(client *c) {
    robj *key = c->argv[1];
    dbDelete(c->db, key); 
    rewriteClientCommandVector(c, 2, shared.del, key);
    server.dirty++;
}

/* set cas flags to their place, value must have space for cas and flag. */
void setMemcachedCasFlag2Value(const uint64_t cas, const uint32_t flags, robj *value) {
    serverAssert(sdslen(value->ptr) >= MEMCACHED_VALUE_ITEM_HEAD_LEN);
    setMemcachedValueCas(value, cas);
    setMemcachedValueFlags(value, flags);
}

/* deal get, getq, gat, gatq, touch response */
#define BIN_GET_RES_HEADER_LEN sizeof(((memcachedBinaryResponseGet*)16)->bytes)
#define BIN_GET_RES_EXTRA_LEN sizeof(((memcachedBinaryResponseGet*)16)->message.body)
void replyMemcachedBinaryHeaderForGetOrTouch(client *c, char *key, const size_t nkey, 
        const size_t valuelen, const uint32_t flags, const int find) {
    memcachedBinaryResponseGet *rsp = NULL;
    if (find) {
        /* nkey must be less than 250. */
        size_t needlen = BIN_GET_RES_HEADER_LEN  + nkey;
        char buf[needlen];
        uint32_t bodylen = BIN_GET_RES_EXTRA_LEN + nkey + valuelen;
        addMemcachedBinaryHeader(c, 0, BIN_GET_RES_EXTRA_LEN, nkey, bodylen, buf);
        /* add the flags */
        rsp = (memcachedBinaryResponseGet *)buf;
        rsp->message.body.flags = htonl(flags);
        if (key && nkey > 0) {
            memcpy(buf + BIN_GET_RES_HEADER_LEN, key, nkey);
        }
        addReplyStringMemcached(c, buf, needlen);
    } else {
        if (key && nkey > 0) {
            size_t needlen = BIN_GET_RES_HEADER_LEN + nkey;
            char buf[needlen];
            addMemcachedBinaryHeader(c, MEMCACHED_BINARY_RESPONSE_KEY_ENOENT,
                    BIN_GET_RES_EXTRA_LEN, nkey, nkey + BIN_GET_RES_EXTRA_LEN, buf);
            /* add the flags */
            rsp = (memcachedBinaryResponseGet *)buf;
            rsp->message.body.flags = htonl(flags);
            memcpy((char *)buf + BIN_GET_RES_HEADER_LEN, key, nkey);
            addReplyStringMemcached(c, buf, needlen);
        } else {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_ENOENT,
                    NULL);
        }
    }
}

/* set key value ex seconds, just like redis setGenericCommand */
void memcachedSetGenericCommand(client *c, robj *key, robj *val, long long seconds, robj *ok_reply) {
    setKey(c->db,key,val);
    server.dirty++;
    /* set expire time when milliseconds more than zero */
    if (seconds > 0) setExpire(c, c->db, key, mstime() + seconds * 1000);
    addReplyMemcached(c, ok_reply);
}


/* Change client request array for aof and replication
 * set key flags expire value;
 * before the argv is like:<command><key><flags><expire><value>,then 
 * <set><key><value><ex><expiration> */
void changeMemcachedArgvAndSet(client *c, const uint64_t cas, const uint32_t flags, const long expire, robj *res) {
    /* Change the argv, just for aof and replication    
     * not use rewriteClientCommandVector, because the following exchange 
     * is more efficient */
    decrRefCount(c->argv[0]);
    c->argv[0] = memcached_shared.setcmd;
    incrRefCount(memcached_shared.setcmd);
    setMemcachedCasFlag2Value(cas, flags, c->argv[4]);
    decrRefCount(c->argv[2]);
    c->argv[2] = c->argv[4];
    if (expire == 0) {
        decrRefCount(c->argv[3]);
        c->argc = 3;
    } else {
        c->argv[4] = c->argv[3];
        incrRefCount(memcached_shared.ex);
        c->argv[3] = memcached_shared.ex;
    }   
    memcachedSetGenericCommand(c, c->argv[1], c->argv[2], expire, res);  
}

/* we store all data like:<cas><flags><data>
 * so keep numeric data the same */
static uint64_t incrDecrMemcachedSetValue(const uint64_t cas, const uint32_t flags, uint64_t initial, 
                             uint64_t delta, const int incr, robj *o) {
#define INCR_DECR_DATA_MAX_LEN 64
    char buf[INCR_DECR_DATA_MAX_LEN];
    size_t len, total_len;
    if (incr) {
        initial += delta;
    } else {
        initial = (initial <= delta) ? 0 : (initial - delta);
    }

    len = ull2string(buf, sizeof(buf), initial);
    total_len = len + MEMCACHED_VALUE_ITEM_HEAD_LEN;
    if (total_len > sdslen(o->ptr)) {
        o->ptr = sdsMakeRoomFor(o->ptr, total_len - sdslen(o->ptr));
        sdsIncrLen(o->ptr, total_len - sdslen(o->ptr));
    } else { /* storage is smaller */
        memset(getMemcachedValuePtr(o) + len, ' ', sdslen(o->ptr) - total_len);
    }

    setMemcachedValueCas(o, cas);
    setMemcachedValueFlags(o, flags);
    memcpy(getMemcachedValuePtr(o), buf, len);
    releaseObjFreeSpace(o, 0);
    return initial;
}

void incrDecrMemcachedCommand(client *c, int incr) {
    robj *key = c->argv[1], *val, *res, *expire_obj;
    long expire;
    uint64_t initial;

    val = lookupKeyWrite(c->db, key);
    if (!val) {
        if (c->reqtype == PROTO_REQ_ASCII) {
            /* not find the key, text request do nothing
             * just reply NOT_FOUND */
            addReplyMemcached(c, memcached_shared.not_found);
        } else { /* binary request will set the key */
            expire = (long)c->argv[4]->ptr;
            if (expire == MEMCACHED_MAX_EXP_TIME) {
                replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_ENOENT, NULL);
            } else {
                initial = (uint64_t)c->argv[3]->ptr;
                robj *o = createStringObject("", 0);
                incrDecrMemcachedSetValue(1, 0, initial, 0, 0, o); 
                initial = htonll(initial);
                c->binary_header.request.cas = 1;
                res = createMemcachedBinaryResponse(c, &initial, 0, 0, sizeof(initial));
                c->argv[4]->ptr = (void *)(long)realTime(expire);
                expire_obj = (expire == 0) ? NULL : c->argv[4];
                if (expire == 0) {
                    rewriteClientCommandVector(c, 3, memcached_shared.setcmd, key, o);
                } else {
                    rewriteClientCommandVector(c, 5, memcached_shared.setcmd, key, o, memcached_shared.ex, expire_obj);
                }
                memcachedSetGenericCommand(c, c->argv[1], c->argv[2], expire, res);  
                decrRefCount(res);
                decrRefCount(o);
            }
        }
        return;
    }

    /* find the key check if the key is numeric */
    if (safe_strtoull(getMemcachedValuePtr(val), &initial) == C_ERR) {
        if (c->reqtype == PROTO_REQ_ASCII) {
            addReplyMemcached(c, memcached_shared.invalid_numeric);
        } else {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_DELTA_BADVAL, NULL);
        }
        return;
    }
    /* it is numeric key, we can incr it */
    val = dbUnshareStringValue(c->db, key, val);
    uint64_t delta = (uint64_t)c->argv[2]->ptr;
    if (c->reqtype == PROTO_REQ_ASCII) {
        incrDecrMemcachedSetValue(getMemcachedValueCas(val) + 1, getMemcachedValueFlags(val), initial, delta, incr, val); 
        addReplyStringMemcached(c, getMemcachedValuePtr(val), getMemcachedValueLength(val));
        addReplyMemcached(c, memcached_shared.crlf);
    } else {
        /* first check cas */    
        if (c->binary_header.request.cas != 0 && c->binary_header.request.cas != getMemcachedValueCas(val)) {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS, NULL);
            return;
        }
        /* cas equal, can change */
        initial = incrDecrMemcachedSetValue(getMemcachedValueCas(val) + 1, getMemcachedValueFlags(val), initial, delta, incr, val); 
        initial = htonll(initial);
        c->binary_header.request.cas = getMemcachedValueCas(val);
        res = createMemcachedBinaryResponse(c, &initial, 0, 0, sizeof(initial));
        addReplyMemcached(c, res);
        decrRefCount(res);
    }
    server.dirty++;
    /* aof and replication */

    expire = getExpire(c->db, key);
    if (expire != -1) {
        expire = expire-mstime();
        if (expire <= 0) { /* the key have expired, just set a short expire time */
            expire = 1;
        }
    }
    /* change the request to : set key value ex [ex seconds] */
    if (expire == -1) {
        rewriteClientCommandVector(c, 3, memcached_shared.setcmd, key, val);
    } else {
        robj *expire_obj = createStringObjectFromLongLong(expire);
        rewriteClientCommandVector(c, 5, memcached_shared.setcmd, key, val, memcached_shared.px, expire_obj);
        decrRefCount(expire_obj);
    }
}

#define AUTHENTIC_STR "Authenticated"
#define AUTHENTIC_STR_LEN (sizeof(AUTHENTIC_STR) - 1) 
void memcachedBinaryCompleteSaslAuth(client *c) {
    robj *key = c->argv[1];

    if (sdslen(key->ptr) > MAX_SASL_MECH_LEN || strncmp(key->ptr, SASL_PLAIN, SASL_PLAIN_LEN)) {
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_EINVAL, NULL);
        return;
    }

    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    char *challenge = vlen == 0 ? NULL : c->argv[2]->ptr;
    char *passwd = NULL;
    if (vlen > 1) {
        passwd = memrchr(challenge + 1, '\0', vlen - 1);
    }

    int result=-1;

    switch (c->binary_header.request.opcode) {
        case MEMCACHED_BINARY_CMD_SASL_AUTH:
            if (passwd != NULL && (passwd < challenge + sdslen(c->argv[2]->ptr)) && !time_independent_strcmp(passwd + 1, server.requirepass)) {
                result = 0;
            }
            break;
        default:
            break;
    }

    switch(result) {
        case 0:
            c->authenticated = 1;
            replyMemcachedBinaryResponse(c, AUTHENTIC_STR, 0, 0, AUTHENTIC_STR_LEN);
            break;
        default:
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_AUTH_ERROR, NULL);
    }
}

