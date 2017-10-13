/*
 * Copyright (c) 2017, Alibaba Group Holding Limited
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:

 *  * Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *  * Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "server.h"
#include "slowlog.h"
/* we support only 16 text commands and 34 binary commands, 
 * this means some text commands don't need;
 * set opcode equal 0xff when the operation has no opcode */

#define MEMCACHED_COMMAND_FUNC(cmd) cmd##MemcachedCommand
#define MEMCACHED_CMD_DECLARE_HEAD(cmd)  #cmd, MEMCACHED_COMMAND_FUNC(cmd)
#define MEMCACHED_CMD_DECLARE_HEAD_NM(cmdnm, cmd)  #cmdnm, MEMCACHED_COMMAND_FUNC(cmd)
#define MEMCACHED_PROTOCOL_CODE(cmd) MEMCACHED_BINARY_CMD_##cmd

/* Procsss binary get and touch helper function */
extern int time_independent_strcmp(char *a, char *b);
extern void replyMemcachedBinaryHeaderForGetOrTouch(client *c, char *key, const size_t nkey, const size_t valuelen, const uint32_t flags, const int find);
extern size_t getMemcachedValueLength(robj *o);
extern void convertMemcachedRawCmd2DelCmd(client *c);
extern void incrDecrMemcachedCommand(client *c, int incr);
extern void replyMemcachedBinaryError(client *c, memcachedBinaryResponseStatus err,  const char *errstr);
extern robj* createMemcachedBinaryResponse(client *c, void *d, int hlen, int keylen, int dlen);
extern void changeMemcachedArgvAndSet(client *c, const uint64_t cas, const uint32_t flags, const long expire, robj *res);
extern void memcachedSetGenericCommand(client *c, robj *key, robj *val, long long seconds, robj *ok_reply);
extern void setMemcachedCasFlag2Value(const uint64_t cas, const uint32_t flags, robj *value);
extern void releaseObjFreeSpace(robj *o, int need_cas_flag_header);
extern sds sdsfromMemcachedSds(sds s, size_t cas_flag_size);
/* Write an binary response to client */
void replyMemcachedBinaryResponse(client *c, void *d, int hlen, int keylen, int dlen);
/* parse the version and quit request command */
extern int parseMemcachedVersionQuitCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos);

/* parse the touch request command */
extern int parseMemcachedTouchCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos);

/* parse the delete request command */
extern int parseMemcachedDeleteCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos);

/* parse the incr and decr request command */
extern int parseMemcachedArithmeticCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos);

/* parse set, add, append, prepend, cas request command */
extern int parseMemcachedUpdateCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int handle_cas, const int pos);

/* parse get request command */
extern int parseMemcachedGetCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos); 

/* parse flush request command */
extern int parseMemcachedFlushCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos); 

/* parse auth request command */
extern int parseMemcachedAuthCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos); 

/* parse minfo request command */
extern int parseMemcachedMinfoCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos); 

/* parse mscan request command */
extern int parseMemcachedMscanCommand(client *c, const size_t len, void *tokens, const size_t ntokens, const int hint, const int pos); 

/* noop operation is just like redis ping command,
 * it just check server's  condition */
static void noopMemcachedCommand(client *c);

/* getk just like get command, the difference between the two is that
 * get just return the key's value, but getk also return the key itself */
static void getkMemcachedCommand(client *c);

/* gat means get and touch;
 * the gat command makes get and touch be atomic */
static void gatMemcachedCommand(client *c);

/* sasl list mechs, just return PLAIN */
extern void memcachedBinaryCompleteSaslAuth(client *c);
void saslListMechsMemcachedCommand(client *c);
void saslAuthMemcachedCommand(client *c);
void saslStepMemcachedCommand(client *c);
/* get key value command */
static void getMemcachedCommand(client *c);

/* get key' value and it's cas */
static void getsMemcachedCommand(client *c);

/* cas means check and set */
static void casMemcachedCommand(client *c);

/* auth */
void authMemcachedCommand(client *c);

/* flush_all is a dangerous command, it always return OK after 
 * all data was flushed */
static void flushallMemcachedCommand(client *c);

/* set key value */
static void setMemcachedCommand(client *c);

/* set key value when key does't exist */
static void addMemcachedCommand(client *c);

/* Replace key's value when key exists */
static void replaceMemcachedCommand(client *c);

/* append key value */
static void appendMemcachedCommand(client *c);

/* prepend key value */
static void prependMemcachedCommand(client *c);

/* delete key */
static void deleteMemcachedCommand(client *c);

/* incr key value */
static void incrMemcachedCommand(client *c);

/* decr key value */
static void decrMemcachedCommand(client *c);

/* Return memcached' current version */
void versionMemcachedCommand(client *c);

/* change the key's expiration time */
static void touchMemcachedCommand(client *c);

struct memcachedCommand memcachedCommandTable[] = 
{
    { {MEMCACHED_CMD_DECLARE_HEAD(get),-2,"r",0,NULL,1,1,1,0,0}, parseMemcachedGetCommand, MEMCACHED_PROTOCOL_CODE(GET), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(gets),-2,"r",0,NULL,1,1,1,0,0},parseMemcachedGetCommand, MEMCACHED_PROTOCOL_CODE(FAKE), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(cas),-6,"wm",0,NULL,1,1,1,0,0}, parseMemcachedUpdateCommand, MEMCACHED_PROTOCOL_CODE(FAKE), 1},
    { {MEMCACHED_CMD_DECLARE_HEAD(auth),-3,"rF",0,NULL,1,1,1,0,0}, parseMemcachedAuthCommand, MEMCACHED_PROTOCOL_CODE(SASL_AUTH), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(flush_all, flushall),-1,"wm",0,NULL,1,1,1,0,0}, parseMemcachedFlushCommand, MEMCACHED_PROTOCOL_CODE(FLUSH), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(set),-5,"wm",0,NULL,1,1,1,0,0}, parseMemcachedUpdateCommand, MEMCACHED_PROTOCOL_CODE(SET), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(add),-5,"wmF",0,NULL,1,1,1,0,0}, parseMemcachedUpdateCommand, MEMCACHED_PROTOCOL_CODE(ADD), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(replace),-5,"wmF",0,NULL,1,1,1,0,0}, parseMemcachedUpdateCommand, MEMCACHED_PROTOCOL_CODE(REPLACE), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(append),-5,"wmF",0,NULL,1,1,1,0,0}, parseMemcachedUpdateCommand, MEMCACHED_PROTOCOL_CODE(APPEND), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(prepend),-5,"wmF",0,NULL,1,1,1,0,0}, parseMemcachedUpdateCommand, MEMCACHED_PROTOCOL_CODE(PREPEND), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(delete),-2,"w",0,NULL,1,1,1,0,0}, parseMemcachedDeleteCommand, MEMCACHED_PROTOCOL_CODE(DELETE), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(incr),-3,"wF",0,NULL,1,1,1,0,0}, parseMemcachedArithmeticCommand, MEMCACHED_PROTOCOL_CODE(INCREMENT), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(decr),-3,"wF",0,NULL,1,1,1,0,0}, parseMemcachedArithmeticCommand, MEMCACHED_PROTOCOL_CODE(DECREMENT), 0},
    { {"quit",NULL,1,"rF",0,NULL,1,-1,1,0,0}, parseMemcachedVersionQuitCommand, MEMCACHED_PROTOCOL_CODE(QUIT), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(version),1,"rF",0,NULL,1,1,1,0,0}, parseMemcachedVersionQuitCommand, MEMCACHED_PROTOCOL_CODE(VERSION), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(touch),-3,"w",0,NULL,1,-2,1,0,0}, parseMemcachedTouchCommand, MEMCACHED_PROTOCOL_CODE(TOUCH), 0},
    /* following is the memcached binary command*/
    { {MEMCACHED_CMD_DECLARE_HEAD(get),-2,"rb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(GET), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(set),-5,"wmb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(SET), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(add),-5,"wmFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(ADD), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(replace),-5,"wmFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(REPLACE), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(delete),-2,"wb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(DELETE), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(incr),-3,"wFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(INCREMENT), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(decr),-3,"wFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(DECREMENT), 0},
    { {"quit",NULL,1,"rFb",1,NULL,1,-1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(QUIT), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(flush_all, flushall),-1,"wb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(FLUSH), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(getq, get),-2,"rb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(GETQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(noop),1,"rFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(NOOP), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(version),1,"rFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(VERSION), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(getk),-2,"rb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(GETK), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(getkq, getk),-2,"rb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(GETKQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(append),-5,"wmFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(APPEND), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(prepend),-5,"wmFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(PREPEND), 0},
    { {"stat",NULL,1,"wmFb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(STAT), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(setq, set),-5,"wmb",0,NULL,1,-1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(SETQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(addq, add),-5,"wmbF",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(ADDQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(replaceq, replace),-5,"wmbF",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(REPLACEQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(deleteq, delete),-2,"wmb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(DELETEQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(incrq, incr),-3,"wmbF",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(INCREMENTQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(decrq, decr),-3,"wmbF",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(DECREMENTQ), 0},
    { {"quit",NULL,2,"rbF",1,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(QUITQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(flush_allq, flushall),-1,"bwF",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(FLUSHQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(appendq, append),-5,"bwF",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(APPENDQ), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(prependq, prepend),-5,"bwmF",0,NULL,1,2,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(PREPENDQ), 0},
    { {"verbosity",NULL,4,"bwms",0,NULL,1,2,1,0,0}, NULL, 0x1b, 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(touch),-3,"bw",0,NULL,1,-2,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(TOUCH), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD(gat),-3,"wb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(GAT), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(gatq, gat),-3,"wb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(GATQ), 0},
    { {"notsupport",NULL,3,"wb",0,NULL,1,1,1,0,0}, NULL, 0x1f, 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(sasl_list_mechs, saslListMechs),4,"rb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(SASL_LIST_MECHS), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(sasl_auth, saslAuth),4,"rb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(SASL_AUTH), 0},
    { {MEMCACHED_CMD_DECLARE_HEAD_NM(sasl_step, saslStep),4,"rb",0,NULL,1,1,1,0,0}, NULL, MEMCACHED_PROTOCOL_CODE(SASL_STEP), 0} 
};

/* process the memcached ascii get command */
void getMemcachedTextCommand(client *c, int with_cas) {
    int j;
    char header[MAX_HEADER_LEN] = {"VALUE "};
    unsigned int len;

    for (j = 1; j < c->argc; j++) {
        /* return format is:
         * VALUE key flags datalen[ <cas>]\r\n<data>\r\n */
        robj *o = lookupKeyRead(c->db, c->argv[j]);
        if (o) {
            len = 6;
            memcpy(header + len, c->argv[j]->ptr, sdslen(c->argv[j]->ptr));
            len += sdslen(c->argv[j]->ptr);
            header[len] = ' ';
            len += 1;
            uint32_t datalen = getMemcachedValueLength(o); 
            unsigned int vlen = ull2string(header + len, MAX_HEADER_LEN - len, getMemcachedValueFlags(o));
            len += vlen;
            header[len] = ' ';
            len += 1;
            vlen = ull2string(header + len, MAX_HEADER_LEN - len, datalen);
            len += vlen;
            if (with_cas) {
                header[len] = ' ';
                len += 1;
                vlen = ull2string(header + len, MAX_HEADER_LEN - len, getMemcachedValueCas(o));
                len += vlen;
            }
            header[len] = '\r';
            header[len+1] = '\n';
            len += 2;
            addReplyStringMemcached(c, header, len);
            addReplyStringMemcached(c, getMemcachedValuePtr(o), datalen);
            addReplyMemcached(c, memcached_shared.crlf);
        }
    }
    addReplyMemcached(c, memcached_shared.end);
}

void getMemcachedCommand(client *c) {
    if (c->reqtype == PROTO_REQ_ASCII) {
        getMemcachedTextCommand(c, 0);
    } else {
        robj *o = lookupKeyRead(c->db, c->argv[1]);

        if (o) {
            /* GetQ means if the key is found, then return the value, 
             * if not then nothing return */
            c->flags &= ~(CLIENT_NO_REPLY);
            size_t valuelen = getMemcachedValueLength(o);
            c->binary_header.request.cas = getMemcachedValueCas(o);
            replyMemcachedBinaryHeaderForGetOrTouch(c, NULL, 0, valuelen, getMemcachedValueFlags(o), 1);
            addReplyStringMemcached(c, getMemcachedValuePtr(o), valuelen);
        } else {
            replyMemcachedBinaryHeaderForGetOrTouch(c, NULL, 0, 0, 0, 0);
        }
    }
}

/* gets only have text request */
void getsMemcachedCommand(client *c) {
    getMemcachedTextCommand(c, 1);
}

/* cas means check and set
 * cas key flags expire bytes cas [noreply]\r\n<data>\r\n */
void casMemcachedCommand(client *c) {
    robj *expire_obj, *o, *key = c->argv[1], *val = c->argv[5];
    long flags = (long)c->argv[2]->ptr;
    long expire = (long)c->argv[3]->ptr;
    uint64_t ocas, cas = (unsigned long long)c->argv[4]->ptr;
    o = lookupKeyWrite(c->db, key); 
    if (!o) {
        addReplyMemcached(c, memcached_shared.not_found);
        return;
    }
    /* find key */
    ocas = getMemcachedValueCas(o);
    if (ocas != cas) {
        addReplyMemcached(c, memcached_shared.exists);
        return;
    }
    /* equal cas, so change the value and set cas
     * when cas equal, always return STORED */
    if (expire == MEMCACHED_MAX_EXP_TIME) {
        dbDelete(c->db, key);
        rewriteClientCommandVector(c, 2, shared.del, c->argv[1]);
        addReplyMemcached(c, memcached_shared.stored);
        server.dirty++;
    } else {
        setMemcachedCasFlag2Value(cas + 1, flags, val);
        expire_obj = (expire == 0) ? NULL : c->argv[3];
        if (expire == 0) {
            rewriteClientCommandVector(c, 3, memcached_shared.setcmd, key, val);
        } else {
            rewriteClientCommandVector(c, 5, memcached_shared.setcmd, key, val, memcached_shared.ex, expire_obj);
        }
        memcachedSetGenericCommand(c, c->argv[1], c->argv[2], expire, memcached_shared.stored);
    }
}

/* auth command
 * memcached is different from redis auth, which has user as its first parameter.
 * auth user password */
void authMemcachedCommand(client *c) {
    robj *passwd = c->argv[2];

    /* ignore password */
    if (c->authenticated) {
        addReplyMemcached(c, memcached_shared.ok);
        return;
    }

    if (server.requirepass != NULL && time_independent_strcmp(passwd->ptr, server.requirepass) != 0) {
        c->authenticated = 0;
        addReplyMemcached(c, memcached_shared.invalid_pw_or_uname);
        return;
    }

    c->authenticated = 1;
    addReplyMemcached(c, memcached_shared.ok);
}

/* flush all data
 * command format: flush_all  expire
 * we only support immediately flush, expire must bu zero */
void flushallMemcachedCommand(client *c) {
    long expire = (long)c->argv[1]->ptr;
    robj *res;

    if (expire != 0) {
        if (c->reqtype == PROTO_REQ_ASCII) {
            addReplyMemcached(c, memcached_shared.invalidexp);
        } else {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_EINVAL, NULL);
        }
        return;
    }
    /* flush all data */
    server.dirty += emptyDb(-1, EMPTYDB_ASYNC, NULL);
    if (c->reqtype == PROTO_REQ_ASCII) {
        res = memcached_shared.ok;
        incrRefCount(res);
    } else {
        res = createMemcachedBinaryResponse(c, NULL, 0, 0, 0);
    }
    addReplyMemcached(c,res);
    decrRefCount(res);

    if (server.rdb_child_pid != -1) {
        kill(server.rdb_child_pid,SIGUSR1);
        rdbRemoveTempFile(server.rdb_child_pid);
    }
    if (server.saveparamslen > 0) {
        /* Normally rdbSave() will reset dirty, but we don't want this here
         * as otherwise FLUSHALL will not be replicated nor put into the AOF. */
        int saved_dirty = server.dirty;
        flushAppendOnlyFile(1);
        struct rdbSaveInfo rdb_save_info = RDB_SAVE_INFO_INIT;
        rdbSave(server.rdb_filename, &rdb_save_info);
        server.dirty = saved_dirty;
    }
    server.dirty++;

    sds o = sdsnewlen(NULL, 200);
    sdsclear(o);
    o = catClientInfoString(o,c);
    serverLog(LL_NOTICE, "memcached flushall called by client %s", o);
    sdsfree(o);
    /* for replication and aof */
    rewriteClientCommandVector(c, 1, memcached_shared.flushallcmd);
}

/* add command just like set command only if the key doesn't exist
 * add key flags expire value */
void addMemcachedCommand(client *c) {
    robj *o, *res, *key = c->argv[1];
    long expire = (long)c->argv[3]->ptr;
    long flags = (long)c->argv[2]->ptr;
    uint64_t cas = 1;
    o = lookupKeyWrite(c->db, key);
    if (o) {
        /* If the key exists or key and value's length is more than 1M,
         * don't store the data;
         * Note that when do lookupKeyWrite or lookupKeyRead, if key exists, its' 
         * LRU have been setted, so nothing should be done, just return.
         * It's weired that when the sum of key and value's length greater than 1M, the error
         * code is MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS */
        if (c->reqtype == PROTO_REQ_ASCII) {
            addReplyMemcached(c, memcached_shared.not_stored);
        } else {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS, NULL);
        }
        /* no aof, no replication */
        return;
    }

    if (c->reqtype == PROTO_REQ_ASCII) {
        res = memcached_shared.stored;
        incrRefCount(res);
    } else {
        c->binary_header.request.cas = cas;
        res = createMemcachedBinaryResponse(c, NULL, 0, 0, 0);
    } 

    /* key not exists */
    if (expire == MEMCACHED_MAX_EXP_TIME) {
        /* don't do aof and replication */
        addReplyMemcached(c, res);
        decrRefCount(res);
        return;
    }
    changeMemcachedArgvAndSet(c, cas, flags, expire, res);
    decrRefCount(res);
}

/* set key flags expire value */
void setMemcachedCommand(client *c) {
    /* first check if the key exists, and set cas */
    uint64_t cas = 1;
    uint32_t flags, expire;
    flags = (long)c->argv[2]->ptr;
    expire = (long)c->argv[3]->ptr;
    robj *res;
    serverAssertWithInfo(c, NULL, c->argc == 5);

    robj *val = lookupKeyWrite(c->db, c->argv[1]);
    if (val != NULL) {
        cas = getMemcachedValueCas(val) + 1;
        /* binary command should check cas */
        if (c->reqtype == PROTO_REQ_BINARY && c->binary_header.request.cas != 0) {
            if (c->binary_header.request.cas != getMemcachedValueCas(val)) {
                replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS, NULL);
                return;
            }
        }
    } else {
        if (c->reqtype == PROTO_REQ_BINARY && c->binary_header.request.cas != 0) {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_ENOENT, NULL);
            return;
        }
    }

    if (c->reqtype == PROTO_REQ_ASCII) {
        res = memcached_shared.stored;
        incrRefCount(res);
    } else {
        c->binary_header.request.cas = cas;
        res = createMemcachedBinaryResponse(c, NULL, 0, 0, 0);
    }

    if (expire == MEMCACHED_MAX_EXP_TIME) {
        /* expired */
        addReplyMemcached(c, res);
        decrRefCount(res);
        if (!val) {
            /* key not exists, so no aof, no replication, 
             * nothing changed, just return */
            c->flags &= ~(CLIENT_FORCE_REPL | CLIENT_FORCE_AOF);
        } else {
            convertMemcachedRawCmd2DelCmd(c);
        }
        return;
    }

    /* change argv order for replication
     * this function will deal setq command, so change the command name */
    changeMemcachedArgvAndSet(c, cas, flags, expire, res);
    decrRefCount(res);
}

/* replace key flags expire value
 * replace is just like set only if the key exists */
void replaceMemcachedCommand(client *c) {
    robj *o, *res, *key = c->argv[1];
    long expire = (long)c->argv[3]->ptr;
    long flags = (long)c->argv[2]->ptr;
    uint64_t cas = 1;
    o = lookupKeyWrite(c->db, key);
    if (!o) {
        if (c->reqtype == PROTO_REQ_ASCII) {
            addReplyMemcached(c, memcached_shared.not_stored);
        } else {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_ENOENT, NULL);
        }
        /* no aof, no replication */
        return;
    } else {
        /* key exists, if command is binary, check cas */
        if (c->reqtype == PROTO_REQ_BINARY && c->binary_header.request.cas != 0) {
            if (c->binary_header.request.cas != getMemcachedValueCas(o)) {
                replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS, NULL);
                return;
            }
        }
    }

    cas = getMemcachedValueCas(o);
    ++cas;
    if (c->reqtype == PROTO_REQ_ASCII) {
        res = memcached_shared.stored;
        incrRefCount(res);
    } else {
        c->binary_header.request.cas = cas;
        res = createMemcachedBinaryResponse(c, NULL, 0, 0, 0);
    }

    /* key  exists */
    if (expire == MEMCACHED_MAX_EXP_TIME) {
        /* delete key, do aof, and replication */
        addReplyMemcached(c, res);
        decrRefCount(res);
        convertMemcachedRawCmd2DelCmd(c);
        return;
    }
    changeMemcachedArgvAndSet(c, cas, flags, expire, res);
    decrRefCount(res);
}

static inline sds parseMemcachedStringObject(sds s) {
    serverAssert(sdslen(s) >= MEMCACHED_VALUE_ITEM_HEAD_LEN);
    return sdsfromMemcachedSds(s, MEMCACHED_VALUE_ITEM_HEAD_LEN);
}

/* append text request format is: 
 * append key flags expire data
 * append binary request format is:
 * append key data 
 * it's weired the text and binary don't have the same parameters
 * the text append have expire parmeters, but was neglected. */
void appendMemcachedCommand(client *c) {
    robj*key = c->argv[1], *val, *o, *res;
    sds nsds;

    val = c->reqtype == PROTO_REQ_ASCII ? c->argv[4] : c->argv[2]; 
    serverAssert(sdslen(val->ptr) >= MEMCACHED_VALUE_ITEM_HEAD_LEN);
    o = lookupKeyWrite(c->db, key);
    if (!o) { /* the key don't exist, nothing changed, so reply immediately */
        if (c->reqtype == PROTO_REQ_ASCII) {
            addReplyMemcached(c, memcached_shared.not_stored);
        } else {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_NOT_STORED, NULL);
        }
        /* no aof, and replication */
        return;
    } else {
        /* key exists, if command is binary, check cas */
        if (c->reqtype == PROTO_REQ_BINARY && c->binary_header.request.cas != 0) {
            if (c->binary_header.request.cas != getMemcachedValueCas(o)) {
                replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS, NULL);
                return;
            }
        }
    }

    /* Find the key, check if the key was referenced by
     * others, if so create new one, if not use it */
    o = dbUnshareStringValue(c->db, key, o);
    nsds = parseMemcachedStringObject(val->ptr);
    o->ptr = sdscatsds(o->ptr, nsds);   
    /* Change the cas */
    uint64_t cas = getMemcachedValueCas(o) + 1; 
    setMemcachedValueCas(o, cas);    
    releaseObjFreeSpace(o, 0);
    server.dirty++;
    /* do replication and aof */
    if (c->reqtype == PROTO_REQ_ASCII) {
        res = memcached_shared.stored;
        incrRefCount(res);
    } else {
        c->binary_header.request.cas = cas;
        res = createMemcachedBinaryResponse(c, NULL, 0, 0, 0);
    }

    addReplyMemcached(c, res);
    decrRefCount(res);
    /* replication and aof;
     * text request has flags and expire, check them */
    long long expire = getExpire(c->db, key);
    if (expire != -1) {
        expire = expire-mstime();
        if (expire <= 0) { /* the key have expired, just set a very little time */
            expire = 1;
        }
    }

    /* change the request to : set key value ex [ex seconds] */
    if (expire == -1) {
        rewriteClientCommandVector(c, 3, memcached_shared.setcmd, key, o);
    } else {
        robj *expire_obj = createStringObjectFromLongLong(expire);
        rewriteClientCommandVector(c, 5, memcached_shared.setcmd, key, o, memcached_shared.px, expire_obj);
        decrRefCount(expire_obj);
    }
}

/* prepend text request format is:
 * prepend key flags expire data
 * prepend bianry request format is:
 * prepend key data */
void prependMemcachedCommand(client *c) {
    robj*key = c->argv[1], *val, *o, *res;
    sds nsds;

    val = c->reqtype == PROTO_REQ_ASCII ? c->argv[4] : c->argv[2]; 
    serverAssert(sdslen(val->ptr) >= MEMCACHED_VALUE_ITEM_HEAD_LEN);

    o = lookupKeyWrite(c->db, key);
    if (!o) { /* the key don't exist, nothing changed, so reply immediately */
        if (c->reqtype == PROTO_REQ_ASCII) {
            addReplyMemcached(c, memcached_shared.not_stored);
        } else {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_NOT_STORED, NULL);
        }
        /* no aof, no replication */
        return;
    } else {
        /* key exists, if command is binary, check cas */
        if (c->reqtype == PROTO_REQ_BINARY && c->binary_header.request.cas != 0) {
            if (c->binary_header.request.cas != getMemcachedValueCas(o)) {
                replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS, NULL);
                return;
            }
        }
    }
    size_t oldl = getMemcachedValueLength(o);
    /* find the key, check if the key was referenced by
     * others, if so create new one, if not use it */
    o = dbUnshareStringValue(c->db, key, o);
    /* first make room */
    nsds = parseMemcachedStringObject(val->ptr);
    o->ptr = sdsMakeRoomFor(o->ptr, sdslen(nsds));
    /* mov data */
    memmove(getMemcachedValuePtr(o) + sdslen(nsds), getMemcachedValuePtr(o), oldl);
    /* copy new data */
    memcpy(getMemcachedValuePtr(o), nsds, sdslen(nsds));
    /* add sds len */
    sdsIncrLen(o->ptr, sdslen(nsds));
    /* change the  cas */
    uint64_t cas = getMemcachedValueCas(o) + 1;
    setMemcachedValueCas(o, cas);    
    releaseObjFreeSpace(o, 0);
    server.dirty++;
    /* do replication and aof */
    if (c->reqtype == PROTO_REQ_ASCII) {
        res = memcached_shared.stored;
        incrRefCount(res);
    } else {
        c->binary_header.request.cas = getMemcachedValueCas(o);
        res = createMemcachedBinaryResponse(c, NULL, 0, 0, 0);
    }

    addReplyMemcached(c, res);
    decrRefCount(res);
    server.dirty++;
    long long expire = getExpire(c->db, key);
    if (expire != -1) {
        expire = expire-mstime();
        if (expire <= 0) { /* the key have expired, nothing to do */
            expire = 1;
        }
    }

    /* change the request to : set key value ex [ex seconds] */
    if (expire == -1) {
        rewriteClientCommandVector(c, 3, memcached_shared.setcmd, key, o);
    } else {
        robj *expire_obj = createStringObjectFromLongLong(expire);
        rewriteClientCommandVector(c, 5, memcached_shared.setcmd, key, o, memcached_shared.px, expire_obj);
        decrRefCount(expire_obj);
    }
}

/* delete key */
void deleteMemcachedCommand(client *c) {
    robj *key = c->argv[1], *o;
    o = lookupKeyWrite(c->db, key);
    if (!o) {
        if (c->reqtype == PROTO_REQ_ASCII) {
            addReplyMemcached(c, memcached_shared.not_found);
        } else {
            replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_ENOENT, NULL);
        }
        /* no aof, no replication */
        return;
    }

    if (c->reqtype == PROTO_REQ_BINARY && !(c->binary_header.request.cas == getMemcachedValueCas(o) 
                || c->binary_header.request.cas == 0)) {
        replyMemcachedBinaryError(c, MEMCACHED_BINARY_RESPONSE_KEY_EEXISTS, NULL);
        /* no aof, no replication */
        return;
    }

    robj *res;
    if (c->reqtype == PROTO_REQ_ASCII) {
        res = memcached_shared.deleted;
        incrRefCount(res);
    } else {
        res = createMemcachedBinaryResponse(c, NULL, 0, 0, 0);
    }

    addReplyMemcached(c, res);
    decrRefCount(res);
    /* replication and aof */
    convertMemcachedRawCmd2DelCmd(c);
}

/* incr text request format like:
 * incr <key> <value> [noreply]\r\n
 * the binary protocol request is:
 * incr key delta initial expiration */ 
void incrMemcachedCommand(client *c) {
    incrDecrMemcachedCommand(c, 1);
}

/* decr text request format like:
 * decr <key> <value> [noreply]\r\n
 * the binary protocol request is:
 * decr key delta initial expiration */ 
void decrMemcachedCommand(client *c) {
    incrDecrMemcachedCommand(c, 0);
}

/* return the memcached version that be used */
void versionMemcachedCommand(client *c) {
    if (c->reqtype == PROTO_REQ_ASCII) {
        addReplyMemcached(c, memcached_shared.version);
    } else {
        replyMemcachedBinaryResponse(c, MEMCACHED_VERSION, 0, 0, sizeof(MEMCACHED_VERSION) - 1);
    }
}

/* touch callback handler
 * touch key expire
 * touch don't modify the cas */
void touchMemcachedCommand(client *c) {
    long long expire;
    robj *o;
    expire = (long long)c->argv[2]->ptr;
    o = lookupKeyWrite(c->db, c->argv[1]);
    if (!o) {
        if (c->reqtype == PROTO_REQ_ASCII) {
            addReplyMemcached(c, memcached_shared.not_found);
        } else {
            replyMemcachedBinaryHeaderForGetOrTouch(c, NULL, 0, 0, 0, 0);
        }
        return;
    }
    /* delete the key immediately */
    if (expire == MEMCACHED_MAX_EXP_TIME) {
        convertMemcachedRawCmd2DelCmd(c);
    } else {
        if (expire == 0) { /* never expire */
            removeExpire(c->db, c->argv[1]);
            rewriteClientCommandVector(c, 2, memcached_shared.persistcmd, c->argv[1]);
            server.dirty++;
        } else {
            setExpire(c, c->db, c->argv[1], mstime() + expire * 1000);
            rewriteClientCommandVector(c, 3, memcached_shared.expirecmd, c->argv[1], c->argv[2]);
            server.dirty++;
        }
    }

    if (c->reqtype == PROTO_REQ_ASCII) {
        addReplyMemcached(c, memcached_shared.touched);
    } else {
        replyMemcachedBinaryHeaderForGetOrTouch(c, NULL, 0, 0, getMemcachedValueFlags(o), 1);
    }
}

/* getk is only used by memcached binary protocol
 * get key's value with key return */
void getkMemcachedCommand(client *c) {
    robj *o = lookupKeyRead(c->db, c->argv[1]);
    if (o) {
        memcachedValueItem *vptr = (memcachedValueItem *)o->ptr;
        c->flags &= ~(CLIENT_NO_REPLY);
        size_t valuelen = getMemcachedValueLength(o);
        c->binary_header.request.cas = getMemcachedValueCas(o);
        replyMemcachedBinaryHeaderForGetOrTouch(c, (char *)c->argv[1]->ptr, sdslen(c->argv[1]->ptr), valuelen, vptr->flags, 1);
        addReplyStringMemcached(c, vptr->data, valuelen);
    } else {
        replyMemcachedBinaryHeaderForGetOrTouch(c, (char *)c->argv[1]->ptr, sdslen(c->argv[1]->ptr), 0, 0, 0);
    }
}

/* noop is just like redis ping command */
void noopMemcachedCommand(client *c) {
    replyMemcachedBinaryResponse(c, NULL, 0, 0, 0);
}

/* gat means get and touch
 * gat don't modify the cas
 * gat key expire */
void gatMemcachedCommand(client *c) {
    unsigned long expire;
    robj *o;
    expire = (unsigned long)c->argv[2]->ptr;
    o = lookupKeyRead(c->db, c->argv[1]);
    if (!o) {
        replyMemcachedBinaryHeaderForGetOrTouch(c, NULL, 0, 0, 0, 0);
        return;
    }
    /* reply value */
    c->binary_header.request.cas = getMemcachedValueCas(o);
    replyMemcachedBinaryHeaderForGetOrTouch(c, NULL, 0, getMemcachedValueLength(o), getMemcachedValueFlags(o), 1);
    addReplyStringMemcached(c, getMemcachedValuePtr(o), getMemcachedValueLength(o));
    /* delete the key immediately */
    if (expire == MEMCACHED_MAX_EXP_TIME) {
        dbDelete(c->db, c->argv[1]);
        rewriteClientCommandVector(c, 2, shared.del, c->argv[1]);
    } else {
        if (expire == 0) { /* never expire */
            removeExpire(c->db, c->argv[1]);
            rewriteClientCommandVector(c, 2, memcached_shared.persistcmd, c->argv[1]);
        } else {
            setExpire(c, c->db, c->argv[1], mstime() + expire * 1000);
            rewriteClientCommandVector(c, 3, memcached_shared.expirecmd, c->argv[1], c->argv[2]);
        }
    }
    server.dirty++;
}

/* we just support PLAIN; */
void saslListMechsMemcachedCommand(client *c) {
    replyMemcachedBinaryResponse(c, (void *)SASL_PLAIN, 0, 0, SASL_PLAIN_LEN);
}

/* sasl auth */
void saslAuthMemcachedCommand(client *c) {
    memcachedBinaryCompleteSaslAuth(c);
}

/* sasl step */
void saslStepMemcachedCommand(client *c) {
    memcachedBinaryCompleteSaslAuth(c);
}
