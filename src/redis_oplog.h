/*
 * Copyright (c) 2017, Alibaba Group Holding Limited
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __REDIS_OPLOG_H
#define __REDIS_OPLOG_H

#define REDIS_SYNC_REPLY_SIZE 4096

#include "server.h"
#include "intset.h"

/* AOF index file */
#define AOF_INDEX_FILE "aof-inc.index"
#define REDIS_AOF_MIN_SIZE 52428800 // 50MB
#define REDIS_DEFAULT_AOF_MAX_SIZE 104857600 // 100MB
#define REDIS_CRON_BGSAVE_REWRITE_PERC  200
#define REDIS_CRON_BGSAVE_REWRITE_MIN_SIZE (512*1024*1024)
#define REDIS_RESTORE_TIMESTAMP_FILE "restore.timestamp"
#define REDIS_DEFAULT_AUTO_CRON_BGSAVE 1
/* in seconds, default 6 hours */
#define REDIS_DEFAULT_OPDEL_SOURCE_TIMEOUT 21600
#define REDIS_SERVERID 12345678 /* redis default server id */
#define REDIS_RDB_INDEX_TMP_FILENAME "tmp-rdb.index"
#define REDIS_RDB_INDEX_FILENAME "rdb.index"
#define REDIS_AOF_INDEX_TMP_FILENAME "tmp-aof-inc.index"
#define REDIS_INDEX_FILE_MAX_SIZE 1048576 /* in byte */
#define REDIS_AOF_INDEX_MAX_USED_PERCENTAGE 80 /* percentage */
#define REDIS_AOF_AUTO_PURGE_PERIOD 30000 /* in millisecond */
/* if maxmemory is not set and auto delete aof is set,
 * redis will keep at most 5GB aof log */
#define REDIS_DEFAULT_AOF_KEEPING_SIZE 5368709120
#define REDIS_DEFAULT_RETURNED_OPLOG_COUNT 50
/* aof psync timeout using sync write in milli sec */
#define AOF_PSYNC_SYNCWRITE_TIMEOUT 500
#define AOF_PSYNC_FORCE_FULL_RESYNC_CMD "*1\r\n$15\r\nforcefullresync\r\n"
#define AOF_PSYNC_FORCE_FULL_RESYNC_CMD_LEN 26
#define REDIS_MIN_SLAVE_OPID_TO_AOF_PSYNC 100
#define SYNCREPLOFFSET_CMD_BUF "*2\r\n$14\r\nsyncreploffset\r\n$%ld\r\n%s\r\n"

/* feed type */
#define REDIS_FEED_AOF      0
#define REDIS_FEED_SLAVES   1

#define REDIS_AOFPSYNC_AOF_READ 0
#define REDIS_OPGET_AOF_READ    1
#define REDIS_AOF_FILENAME_LEN 255
#define REDIS_OPGET_MAX_RETURN_SIZE 104857600

/* type of key del in oplog */
#define REDIS_OPLOG_DEL_BY_EVICTION_FLAG (1<<3)
#define REDIS_OPLOG_DEL_BY_EXPIRE_FLAG (1<<2)

#define REDIS_OPGET_MAX_COUNT 10000
#define REDIS_OPLOG_VERSION 1
#define REDIS_REPL_VERSION 2
#define REDIS_OPGET_READ_ONE_COMMAND_EVENT "opget_read_cmd"

/* error code */
#define REDIS_EMPTY_FILE         -2
#define REDIS_SYNCING_OPINFO_ERR -4
#define REDIS_NO_FIRST_OPID      -5

/* type of key del */
#define REDIS_DEL_BY_EVICTION 0
#define REDIS_DEL_BY_EXPIRE 1
#define REDIS_DEL_BY_CLIENT 2
#define REDIS_DEL_NONE 3 /* not del command */

/* For performance, always try manual align first
 * __attribute__((packed, aligned(1))) is to enhance compatability
 * between different platforms and save few bytes
 *
 * currently, aof format is not compatible between big endian and
 * little endian platforms
 *
 * ==============IMPORTANT=================
 * always add new member at the end to keep forwards compatibility
 * use version member to keep backwards compatibility*/
typedef struct redisOplogHeader {
    unsigned version:8; /* version of oplog */
    unsigned cmd_num:4; /* number of commands in one oplog, currently 2 or 3 */
    unsigned cmd_flag:4;
    unsigned dbid:16;
    int32_t timestamp;
    int64_t server_id;
    int64_t opid;
    int64_t src_opid; /* opid of source redis */
}__attribute__((packed, aligned(1))) redisOplogHeader;

typedef enum redisOplogCmdType {
    REDIS_OPLOG_OPINFO_CMD = 1,
    REDIS_OPLOG_MULTI_CMD,
    REDIS_OPLOG_EXEC_CMD,
    REDIS_OPLOG_OTHER_CMD
} redisOplogCmdType;

typedef struct redisOpdelSource {
    long long opid;             /* opid sent by opdel command
                                 * that redis can delete to */
    time_t last_update_time;    /* last update time */
} redisOpdelSource;

typedef struct bioFindOffsetRes {
    struct client *c;
    uint64_t client_id;
    char aof_filename[256];
    long long offset;
    int error;
    int fd;
    FILE *fp;
} bioFindOffsetRes;

typedef struct replConfigSavedInfo {
    int repl_stream_db;
    char replid[CONFIG_RUN_ID_SIZE+1];  /* My current replication ID. */
    long long repl_offset; /* Accept offsets up to this for replid2. */
    char replid2[CONFIG_RUN_ID_SIZE+1]; /* replid inherited from master*/
    long long second_replid_opid; /* Accept opid up to this for replid2. */
} replConfigSavedInfo;

typedef struct opGetClientState {
    long long next_start_opid; /* expected next start opid after last opget */
    bioFindOffsetRes *opget_aof_state; /* states of opget when reading aof */
    int use_matchdb; /* use matchdb option or not, 1 for yes, 0 for no*/
    intset *matchdbs; /* saved filter options of matching db, so we needn't
                       * to parse filter options every time */
    int use_matchid; /* use matchid option or not, 1 for yes, 0 for no*/
    intset *matchids; /* filter by server-id */
    int use_matchkey; /* use matchkey option or not, 1 for yes, 0 for no*/
    list *matchkeys; /* saved filter options of matching key */
    int oplog_count; /* oplog count returned to client per time */
    int wait_bio_res; /* is opget client waiting for bioFindOffsetByOpid
                       * result, 0 for no, 1 for yes */
} opGetClientState;

// aof binlog
int openAppendOnly(void);
void doStopAof(void);
void handleBioFileClose(void *arg1, void *arg2);
void handleBioFilePurge(void *arg1);
int aofSplit(int try_aof_delete, int force_split);
void aofFlushCommand(client *c);
void initExtraServerVars(void);
void createExtraSharedObjs(void);
void aofSplitCron(void);
int validateFileName(const char *s, size_t len);
void prepareForRdbSave(void);
void collectAofAndMemInfoAfterBgsave(void);
void cronBgsave(void);
void bgsaveNormalReply(client *c);
sds catCronBgsaveInfo(sds info);
sds catOplogInfo(sds info);
void loadDataFromRdbAndAof(void);
int loadOplogFromAppendOnlyFile(char *filename);
void addAofIncrementStatisticsBy(ssize_t nwritten);
void deleteAofIfNeeded();
int doPurgeAof(char *name);
void aofPurgeCron(void);
void purgeAofToCommand(client *c);
char *readAllContentsFromTextFile(char *filename);
list *readAofIndexIntoList();
int debugLoadDisk(client *c);
void opinfoCommand(struct client *c);
void opDelCommand(client *c);
sds feedAofFirstCmdWithOpinfo(struct redisCommand *cmd, int del_type,
                              int dictid);
void checkAndFeedAofWithOplogHeader(client *c, struct redisCommand *cmd,
                                    int dictid, int del_type);
void checkAndFeedSlaveWithOplogHeader(client *c, struct redisCommand *cmd, int dictid);
void revertOpidIfNeeded(void);
void activeFullResyncOnProtocolErr(client *c);
void setAutoSyncForIncrFsync(rio *rdb);
int checkIfNoNeedToFreeMem(client *c);
int writeAndFlushTmpRdbIndex(FILE *fp_tmp_rdb_index, char *filename);
int renameTmpRdbIndex(void);
void cleanTmpRdbIndex(FILE *fp_tmp_rdb_index);
void removeTmpRdbIndexIfNeeded(char *tmpfile);
void initFirstRdbIndex(void);
sds genFakeClientInfo(client *c, sds s);

// aof psync
void resetAofPsyncState();
int masterSlaveofCurrentSlaveCheck(char *slaveof_ip, int slaveof_port);
void restoreReplInfoFromConfig(void);
sds catExtraReplInfo(sds info);
void saveChangedReplInfoIntoConfig(void);
void initSavedReplInfoInConfig(void);
void startBgsaveAfterFullResync(void);
void forceSlavesFullResync();
char *sendPsyncCmdToMasterWithExtraInfo(int fd, char *psync_replid,
                                        char *psync_offset);
char *tryPsyncWithoutExtraInfoIfNeeded(int fd, char *reply);
char *slaveHandleReplyForReplVersion(char *reply);
void handleReplyForExtraFullResyncInfo(char *reply, char *offset);
void resetSlaveNextOpidAfterFullResync(void);
void setMasterAofPsyncingStateAfterFullResync(void);
void checkAndSaveNewReplId(char *reply, int psync_type_str_len,
                           char *old_replid);
int masterReplyAofPsyncingStateOnFullResync(char *buf, int size, int buf_len);
int slaveCheckAofPsyncingState(void);
int masterCheckAofPsyncingState(client *slave);
int handleReplyForAofPsync(char *reply, int fd);
int checkMasterSlaveSameServerId(client *c);
int masterReplyReplVersion(client *c);
void shiftReplIdAndSaveWhenRestart(void);
int masterTryAofPsyncCheck(client *c);
long long getMinimumOpidOfMaster(void);
int masterTryAofPsync(client *c);
void forceFullResyncCommand(client *c);
size_t getAppliedBytesSize(client *c, uint64_t prev_flags,
                           long long prev_reploff);
void aofPsyncReplOffPostSet(client *c, uint64_t prev_flags,
    size_t applied);
sds genSyncreploffsetCmdBuf(long long repl_offset);
void syncReplOffsetCommand(client *c);
char *fillReplIdWithFullResyncReqIfNeeded(char *psync_replid);
void sendSyncReplOffsetCommandToSlave(client *slave);
void doSendAofToSlave(aeEventLoop *el, int fd, void *privdata, int mask);
int aofReadingSetNextAof(bioFindOffsetRes *res, int type);
void bioFindOffsetByOpid(client *c, long long opid, uint64_t client_id);
void handleBioFindOffsetRes();
void handleBioFindOffsetResCron(void);
int ignoreNewLineOnPsyncing(client *slave);
void updateSlaveSyncedOpid(client *c, int arg_idx);
void masterSendAofToSlave();
int getLongLongFromCString(char *o, long long *target);

// aof binlog stream(On BLS)
void updateSrcServerIdOpid(long long src_server_id, long long applied_opid);
void updateAppliedInfoOnStartup(char *buf);
long long dictGetAppliedOpidBySrcServerId(long long src_server_id);
int checkOpapplyCmdIgnored(client *c);
void cleanOpGetClientState(opGetClientState *state, client *c);
void slaveUpdateAppliedInfoOnFullResync(char *applied_info);
int masterReplyAppliedInfoOnFullResync(char *buf, int size, int buf_len);
void initExtraClientState(client *c);
long long getSlaveCurOpid(client *slave);
long long getMinOpidOfSlaves(client **slave);
void sendOplogToClient(client *c);
void initOpGetClientState(opGetClientState **client_state);
void opGetCommand(client *c);
void syncstateCommand(client *c);
void opRestoreCommand(client *c);
int checkOpgetClientWaitingBioState(client *c);
void opApplyCommand(client *c);
void getOpidByAofCommand(client *c);

#endif
