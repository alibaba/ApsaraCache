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

#include "redis_oplog.h"
#include "bio.h"
#include "atomicvar.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>

extern void createDumpPayload(rio *payload, robj *o);

/********************************* aof binlog *********************************/
int openAppendOnly(void) {
    server.aof_last_fsync = server.unixtime;
    char *last_aof_filename = server.aof_filename;
    char buf[64] = {0};
    sprintf(buf, "appendonly-inc-%ld.aof", server.unixtime);
    server.aof_filename = zstrdup(buf);

    /* EINTR is sent when an asynchronous signal occurred and prevented
     * completion of system call, so we should try the system call again.
     * For example if open is interrupted by SIGINT the value of
     * shutdown_asap would be 1 and aof_buf would not be written to disk
     * on condition that don't call open again. */
    do {
        server.aof_fd = open(server.aof_filename,
                             O_WRONLY | O_APPEND | O_CREAT, 0644);
    } while((server.aof_fd == -1) && (errno == EINTR));

    if (server.aof_fd == -1) {
        serverLog(LL_WARNING,
                 "Redis needs to enable the AOF but can't "
                 "open the append only file: %s",
                 strerror(errno));
        zfree(last_aof_filename);
        return C_ERR;
    }

    if (last_aof_filename != NULL && strcmp(last_aof_filename,
                                            server.aof_filename)) {
        /* create aof index file if needed */
        int aof_inc_index_fd;

        do {
            aof_inc_index_fd = open(AOF_INDEX_FILE,
                                    O_WRONLY|O_APPEND|O_CREAT,0644);
        } while((aof_inc_index_fd == -1) && (errno == EINTR));

        if (aof_inc_index_fd == -1) {
            serverLog(LL_WARNING,"Redis needs to enable the AOF but can't "
                     "open the append only *index* file: %s, err_msg: %s",
                     AOF_INDEX_FILE, strerror(errno));
            close(server.aof_fd);
            server.aof_fd = -1;
            zfree(last_aof_filename);
            return C_ERR;
        }
        int buf_len = strlen(buf);
        buf[buf_len++] = '\n';
        if (write(aof_inc_index_fd,buf,buf_len) == -1) {
            serverLog(LL_WARNING,"Redis needs to enable the AOF but can't "
                     "write to aof index file: %s, err_msg: %s",
                     AOF_INDEX_FILE, strerror(errno));
            close(server.aof_fd);
            server.aof_fd = -1;
            close(aof_inc_index_fd);
            zfree(last_aof_filename);
            return C_ERR;
        }
        close(aof_inc_index_fd);

        /* if execute aofflush command multi times in one second
         * server may generate the same aof filename,
         * if it is the same, we don't need to update aof_current_size
         * otherwise we need to set aof_current_size to zero */
        server.aof_current_size = 0;
    }

    server.aof_state = AOF_ON;
    zfree(last_aof_filename);
    return C_OK;
}

void doStopAof(void) {
    if ((server.aof_fsync == AOF_FSYNC_BIO_WRITE)
        && (server.aof_queue != NULL)) {
        flushAppendOnlyFile(0);
    } else {
        flushAppendOnlyFile(1);
    }

    int oldfd = server.aof_fd;

    if(oldfd != -1) {
        if ((server.aof_fsync == AOF_FSYNC_BIO_WRITE) &&
            (server.aof_queue != NULL)) {
            aofQueuePushCloseFileReq(server.aof_queue, oldfd);
        } else {
            bioCreateBackgroundJob(BIO_CLOSE_FILE,
                                   (void *)(long)oldfd,
                                   (void *)(long)BIO_CLOSE_FD, NULL);
        }
    }

    server.aof_fd = -1;
    server.aof_state = AOF_OFF;
}

void handleBioFileClose(void *arg1, void *arg2) {
    if (arg2 == NULL) {
        serverLog(LL_WARNING, "close type not specified, "
                 "bio thread won't close any file");
    } else if ((long)arg2 == BIO_CLOSE_FD) {
        aof_fsync((long)arg1);
        close((long)arg1);
    } else if ((long)arg2 == BIO_CLOSE_FP) {
        aof_fsync(fileno((FILE *)arg1));
        fclose((FILE *)arg1);
    } else {
        serverLog(LL_WARNING, "unknow close type, "
                 "bio thread won't close any file");
    }
}

void handleBioFilePurge(void *arg1) {
    if(unlink((char *)arg1) < 0) {
        serverLog(LL_WARNING, "unlink %s failed: %s",
                 (char *)arg1, strerror(errno));
    } else {
        serverLog(LL_NOTICE, "unlink %s success", (char *)arg1);
    }
    zfree(arg1);
}

int aofSplit(int try_aof_delete, int force_split) {
    if(server.aof_state == AOF_ON) {
        if (!force_split && server.masterhost && server.master &&
            server.master->lastcmd &&
            server.master->lastcmd->proc == opinfoCommand) {
            return REDIS_SYNCING_OPINFO_ERR;
        }
        stopAppendOnly();
        if(startAppendOnly() == C_ERR) {
            serverLog(LL_WARNING,
                     "FATAL: open new aof or aof index file failed"
                     " when splitting aof file");
            server.aof_last_open_status = C_ERR;
            return C_ERR;
        }
        if (try_aof_delete) deleteAofIfNeeded();
        if (server.aof_last_open_status == C_ERR) {
            serverLog(LL_NOTICE,
                     "AOF open error looks solved, Redis can write again.");
            server.aof_last_open_status = C_OK;
        }
        return C_OK;
    }
    return C_OK;
}

void aofFlushCommand(client *c) {
    if (server.aof_state != AOF_ON) {
        addReplyError(c, "Aof is not open while doing aof flush");
    } else {
        int result = aofSplit(1, 0);
        if (result == REDIS_SYNCING_OPINFO_ERR) {
            addReplyError(c, "redis last syncing command is opinfo, "
                          "cannot split aof, try again");
            return;
        } else if (result == C_ERR) {
            addReplyError(c, "redis open new aof or aof index "
                          "file failed, try again");
            return;
        }
        addReply(c, shared.ok);
    }
}

void initSavedReplInfoInConfig(void) {
    server.rsi_config =
        (struct replConfigSavedInfo *)
        zmalloc(sizeof(struct replConfigSavedInfo));
    server.rsi_config->repl_stream_db = -1;
    server.rsi_config->replid[0] = '\0';
    server.rsi_config->repl_offset = 0;
    server.rsi_config->replid2[0] = '\0';
    server.rsi_config->second_replid_opid = -1;
}

void initExtraServerVars(void) {
    server.aof_max_size = REDIS_DEFAULT_AOF_MAX_SIZE;
    server.last_cron_bgsave_mem_use = 1;
    server.aof_inc_from_last_cron_bgsave = 0;
    server.cron_bgsave_rewrite_perc = REDIS_CRON_BGSAVE_REWRITE_PERC;
    server.cron_bgsave_rewrite_min_size = REDIS_CRON_BGSAVE_REWRITE_MIN_SIZE;
    server.auto_cron_bgsave_state = REDIS_DEFAULT_AUTO_CRON_BGSAVE;
    server.auto_purge_aof = 1;
    server.restore_timestamp = 0;
    server.aof_psync_state = 1;
    server.src_serverid_applied_opid_dict = dictCreate(&hashDictType, NULL);
    server.bio_find_offset_results = listCreate();
    server.bio_find_offset_res = NULL;
    server.aof_psync_slave_offset = NULL;
    server.do_aof_psync_send = 1;
    server.repl_master_aof_psyncing_state = 0;
    server.aof_psync_cur_reading_name = NULL;
    server.do_aof_psync_send = 1;
    server.opget_client_list = listCreate();
    server.opdel_source_opid_dict = dictCreate(&hashDictType, NULL);
    server.opdel_source_timeout = REDIS_DEFAULT_OPDEL_SOURCE_TIMEOUT;
    server.opget_max_count = REDIS_OPGET_MAX_COUNT;
    server.opget_master_min_slaves = 1;
    server.server_id = REDIS_SERVERID;
    server.next_opid = 1;
    server.min_valid_opid = 1;
    server.opinfoCommand = lookupCommandByCString("opinfo");
    server.pingCommand = lookupCommandByCString("ping");
    server.replconfCommand = lookupCommandByCString("replconf");
    server.rdb_save_incremental_fsync = REDIS_DEFAULT_RDB_SAVE_INCREMENTAL_FSYNC;
    server.aof_buf_limit = REDIS_AOF_BUF_QUEUE_DEFAULT_LIMIT;

    initSavedReplInfoInConfig();
}

void createExtraSharedObjs(void) {
    shared.opinfo = makeObjectShared(createStringObject("opinfo", 6));
    /* this will use zcalloc */
    shared.oplogheader = makeObjectShared(
        createStringObject(NULL, sizeof(redisOplogHeader)));
    shared.ignored = makeObjectShared(
        createObject(OBJ_STRING,sdsnew("+IGNORED\r\n")));
}

void aofSplitCron(void) {
    if (server.aof_current_size >= server.aof_max_size) {
        aofSplit(1, 0);
    }
}

int validateFileName(const char *s, size_t len) {
    for(unsigned int i = 0; i < len; i++) {
        if(*(s + i) == '\0' || *(s + i) == '/' || *(s + i) == ' ')
            return C_ERR;
    }
    return C_OK;
}

void bgsaveNormalReply(client *c) {
    addReplyMultiBulkLen(c, 10);
    // for compatible with controller backup_agent module
    addReplyBulkCString(c, "status");
    addReplyBulkCString(c, "Background saving started");
    addReplyBulkCString(c, "aof-name");
    addReplyBulkCString(c, (server.aof_state == AOF_OFF) ?
                        "" : server.aof_filename);
    addReplyBulkCString(c, "aof-position");
    addReplyBulkLongLong(c, (server.aof_state == AOF_OFF) ?
                         0 : server.aof_current_size);
    addReplyBulkCString(c, "opid");
    addReplyBulkLongLong(c, (server.aof_state == AOF_OFF) ?
                         -1 : server.next_opid);
    addReplyBulkCString(c, "source_server_id,applied_opid");
    addReplyMultiBulkLen(c,
                         dictSize(server.src_serverid_applied_opid_dict));

    /* add reply <src_serverid, applied_opid> */
    dictIterator *di = dictGetIterator(
        server.src_serverid_applied_opid_dict);
    dictEntry *de = NULL;
    while ((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        robj *val = dictGetVal(de);
        addReplyBulkSds(c, sdscatprintf(sdsempty(), "%ld,%ld",
                                        (long)key->ptr, (long)val->ptr));
    }
    dictReleaseIterator(di);
}

void prepareForRdbSave(void) {
    /* in one event loop, commands that modify data may be executed with bgsave,
     * so, when bgsave is executed, there still exists some data in aof buf
     * which are not flushed to appendonly file, and this may lead to a wrong
     * aof position reply returned by bgsave command. For safety,
     * we just flush appendonly file in force mode */
    if ((server.aof_fsync == AOF_FSYNC_BIO_WRITE) &&
        (server.aof_queue != NULL)) {
        flushAppendOnlyFile(0);
    } else {
        flushAppendOnlyFile(1);
    }

    /* Split aof before bgsave, so that the corresonding offset of every rdb
     * is the start of an aof, this is more convinient for backup
     * system to process.
     * When doing aofSplit(), redis won't try to delete aof
     * because deleteAofIfNeeded() may call rdbSaveBackground() */
    aofSplit(0, 0);

    server.last_bgsave_aof_total_size = server.aof_total_size;
}

void collectAofAndMemInfoAfterBgsave(void) {
    server.last_cron_bgsave_mem_use = zmalloc_used_memory();
    server.aof_inc_from_last_cron_bgsave =
        (off_t)(server.aof_total_size - server.last_bgsave_aof_total_size);
}

void cronBgsave(void) {
    /* Trigger an cron bgsave if needed */
    if (server.auto_cron_bgsave_state &&
        server.aof_state == AOF_ON &&
        server.rdb_child_pid == -1 &&
        server.aof_child_pid == -1 &&
        server.cron_bgsave_rewrite_perc &&
        server.aof_inc_from_last_cron_bgsave > server.cron_bgsave_rewrite_min_size) {
        long long base = server.last_cron_bgsave_mem_use ?
            server.last_cron_bgsave_mem_use : 1;
        long long growth = (server.aof_inc_from_last_cron_bgsave*100/base);
        if (growth >= server.cron_bgsave_rewrite_perc ||
            (server.maxmemory && (unsigned)server.aof_inc_from_last_cron_bgsave
             > server.maxmemory * server.cron_bgsave_rewrite_perc / 100)) {
            serverLog(LL_NOTICE,"Starting cron bgsave on %lld%% (%ld/%lld) aof growth",
                     growth, server.aof_inc_from_last_cron_bgsave, base);
            rdbSaveInfo rsi, *rsiptr;
            rsiptr = rdbPopulateSaveInfo(&rsi);
            rdbSaveBackground(server.rdb_filename, rsiptr);
        }
    }
}

sds catCronBgsaveInfo(sds info) {
    return sdscatprintf(info,
                        "last_cron_bgsave_mem_use:%zu\r\n"
                        "aof_inc_from_last_cron_bgsave:%lld\r\n",
                        server.last_cron_bgsave_mem_use,
                        (long long) server.aof_inc_from_last_cron_bgsave
        );
}

sds catOplogInfo(sds info) {
    info = sdscatprintf(info,
                        "current_opid:%lld\r\n"
                        "min_valid_opid:%lld\r\n"
                        "opapply_source_count:%lu\r\n",
                        server.next_opid-1,
                        server.min_valid_opid,
                        dictSize(server.src_serverid_applied_opid_dict));

    dictIterator *di = dictGetIterator(server.src_serverid_applied_opid_dict);
    dictEntry *de;
    int sourceid = 0;

    while((de = dictNext(di)) != NULL) {
        robj *val = dictGetVal(de);
        robj *key = de->key;
        info = sdscatprintf(info,
                            "opapply_source_%d:server_id=%ld,"
                            "applied_opid=%ld\r\n",
                            sourceid, (long)(key->ptr), (long)(val->ptr));
        sourceid++;
    }
    dictReleaseIterator(di);

    info = sdscatprintf(info,
                        "opdel_source_count:%lu\r\n",
                        dictSize(server.opdel_source_opid_dict));

    di = dictGetIterator(server.opdel_source_opid_dict);
    sourceid = 0;

    while((de = dictNext(di)) != NULL) {
        robj *val = dictGetVal(de);
        redisOpdelSource *source = val->ptr;
        robj *key = de->key;
        info = sdscatprintf(info,
                            "opdel_source_%d:source_name=%s,to_del_opid=%lld,"
                            "last_update_time=%ld\r\n",
                            sourceid, (char *)(key->ptr), source->opid,
                            source->last_update_time);
        sourceid++;
    }
    dictReleaseIterator(di);

    return info;
}

/* if rdb filename is read from rdb.index and not exist, redis exit */
void loadDataFromRdb(char *rdb_filename, int rdb_from_rdb_index) {
    long long start = ustime();
    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
    if (rdbLoad(rdb_filename, &rsi) == C_OK) {
        serverLog(LL_NOTICE,"DB loaded from rdb %s: %.3f seconds",
                 rdb_filename, (float)(ustime()-start)/1000000);

        /* Restore the replication ID / offset from the config file. */
        restoreReplInfoFromConfig();

        // /* Restore the replication ID / offset from the RDB file. */
        // if (server.masterhost &&
        //     rsi.repl_id_is_set &&
        //     rsi.repl_offset != -1 &&
        //     /* Note that older implementations may save a repl_stream_db
        //      * of -1 inside the RDB file. */
        //     rsi.repl_stream_db != -1)
        // {
        //     memcpy(server.replid,rsi.repl_id,sizeof(server.replid));
        //     server.master_repl_offset = rsi.repl_offset;
        //     /* If we are a slave, create a cached master from this
        //      * information, in order to allow partial resynchronizations
        //      * with masters. */
        //     replicationCacheMasterUsingMyself();
        //     selectDb(server.cached_master,rsi.repl_stream_db);
        // }
    } else if(rdb_from_rdb_index || (!rdb_from_rdb_index && errno != ENOENT)){
        serverLog(LL_WARNING,"Fatal error loading the DB: %s. Exiting.",
                 strerror(errno));
        exit(1);
    }
}

void cleaningOfGetAofFirstOpid(sds *argv, FILE *fp, int argc_free_to) {
    fclose(fp);
    for(int i = 0; i < argc_free_to; i++) {
        sdsfree(argv[i]);
    }
    zfree(argv);
}

long long getAofFirstOpid(char *filename) {
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        serverLog(LL_WARNING,
                 "getAofFirstOpid: Cannot open file: %s, %s",
                 filename, strerror(errno));
        return C_ERR;
    }

    struct redis_stat sb;
    if (redis_fstat(fileno(fp),&sb) == -1) {
        serverLog(LL_WARNING,
                 "getAofFirstOpid: Cannot stat file: %s, %s",
                 filename, strerror(errno));
        fclose(fp);
        return C_ERR;
    }

    off_t size = sb.st_size;
    if (size == 0) {
        serverLog(LL_VERBOSE, "getAofFirstOpid: Empty file: %s", filename);
        fclose(fp);
        // return value for empty file, this is not an error
        return REDIS_EMPTY_FILE;
    }

    int argc, j;
    unsigned long len;
    sds *argv = NULL;
    char buf[512];
    sds argsds;
    argc = j = 0;

    if (fgets(buf,sizeof(buf),fp) == NULL) {
        if (!feof(fp)) {
            goto readerr;
        }
    }
    if (buf[0] != '*') goto fmterr;
    if (buf[1] == '\0') goto readerr;
    argc = atoi(buf+1);
    if (argc < 1) goto fmterr;

    argv = zmalloc(sizeof(sds)*argc);
    bzero(argv, sizeof(sds)*argc);

    for (j = 0; j < argc; j++) {
        if (fgets(buf,sizeof(buf),fp) == NULL) {
            goto readerr;
        }
        if (buf[0] != '$') goto fmterr;
        len = strtol(buf+1,NULL,10);
        argsds = sdsnewlen(NULL,len);
        if (len && fread(argsds,len,1,fp) == 0) {
            sdsfree(argsds);
            goto readerr;
        }
        argv[j] = argsds;
        if (fread(buf,2,1,fp) == 0) {
            goto readerr; /* discard CRLF */
        }
    }

    if (!strcasecmp("opinfo", argv[0])) {
        redisOplogHeader *loaded_header = (redisOplogHeader *)argv[1];
        long long tmp_opid = loaded_header->opid;
        serverLog(LL_VERBOSE, "getAofFirstOpid: parsed first opid %ld in %s",
                 loaded_header->opid, filename);
        cleaningOfGetAofFirstOpid(argv, fp, argc);
        return tmp_opid;
    } else {
        serverLog(LL_WARNING, "getAofFirstOpid: no first opid in %s",
                 filename);
        cleaningOfGetAofFirstOpid(argv, fp, argc);
        return REDIS_NO_FIRST_OPID;
    }

readerr:
    serverLog(LL_WARNING, "getAofFirstOpid: read error with file: %s, %s",
             filename, strerror(errno));
    cleaningOfGetAofFirstOpid(argv, fp, argc);
    return C_ERR;
fmterr:
    serverLog(LL_WARNING, "getAofFirstOpid: file format error with file: %s",
             filename);
    cleaningOfGetAofFirstOpid(argv, fp, argc);
    return C_ERR;
}

/* Function called at startup to load RDB or AOF file in memory.
 *
 * ======   rdb.index format   ======
 * for first line, if aof is ON when saving rdb,
 * there are four fields separated by one space character:
 * rdb_filename aof_filename aof_file_offset server_next_opid
 * if aof is OFF when saving rdb, there is only one field:
 * rdb_filename
 * for second line, info of source server-id and applied opid
 * src_serverid1 applied_opid1 src_serverid2 applied_opid2 ......
 *
 * ====== aof-inc.index format ======
 * multi line, every line is a filename of an aof generated by redis server */
void loadDataFromRdbAndAof(void) {
    if(access(REDIS_RDB_INDEX_TMP_FILENAME, F_OK) != -1) {
        serverLog(LL_WARNING,
                 "FATAL: temp rdb.index file exist, "
                 "redis rarely crashed with an unconsistent state,"
                 " please repair this manually");
        exit(1);
    } else if(access(REDIS_RDB_INDEX_FILENAME, F_OK) == -1) {
        serverLog(LL_NOTICE,
                 "rdb.index file doesn't exist, will not recover data from aof");
        loadDataFromRdb(server.rdb_filename, 0);
        unlink(REDIS_RESTORE_TIMESTAMP_FILE);
    } else {
        serverLog(LL_NOTICE,
                 "rdb.index file exist, start loading data from disk");

        /* parse rdb.index */
        char *rbuffer = readAllContentsFromTextFile(REDIS_RDB_INDEX_FILENAME);
        if(NULL == rbuffer) {
            serverLog(LL_WARNING,
                     "FATAL: read all contents from %s failed",
                     REDIS_RDB_INDEX_FILENAME);
            exit(1);
        }
        char *occur = NULL;
        if((occur = strchr(rbuffer, '\n')) != NULL) *occur = '\0';
        updateAppliedInfoOnStartup(occur+1);

        char *rdb_filename = strtok(rbuffer, " ");
        char *aof_filename = strtok(NULL, " ");
        char *aof_position = strtok(NULL, " ");
        char *next_opid = strtok(NULL, " ");
        char *end_guard = strtok(NULL, " ");

        if(!((rdb_filename != NULL && aof_filename == NULL &&
              aof_position == NULL && next_opid == NULL && end_guard == NULL)
             || (rdb_filename != NULL && aof_filename != NULL &&
                 aof_position != NULL && next_opid != NULL &&
                 end_guard == NULL))) {
            serverLog(LL_WARNING,
                     "FATAL: %s file format error", REDIS_RDB_INDEX_FILENAME);
            exit(1);
        }
        if(access(rdb_filename, F_OK) == -1) {
            serverLog(LL_WARNING,
                     "FATAL: rdb file %s listed in index file doesn't exist",
                     rdb_filename);
            zfree(rbuffer);
            return;
        } else if(aof_filename && access(aof_filename, F_OK) == -1) {
            serverLog(LL_WARNING,
                     "FATAL: aof file %s listed in index file doesn't exist",
                     aof_filename);
            exit(1);
        } else if(aof_position) {
            char *endptr = NULL;
            long pos = strtol(aof_position, &endptr, 10);
            if(pos < 0 || *endptr != '\0' || errno == ERANGE) {
                serverLog(LL_WARNING,
                         "FATAL: aof position %s stored in index file invalid",
                         aof_position);
                exit(1);
            }
            server.aof_position_stored = pos;
        }

        if(next_opid) {
            char *endptr = NULL;
            long pos = strtol(next_opid, &endptr, 10);
            if(pos < 0 || *endptr != '\0' || errno == ERANGE) {
                serverLog(LL_WARNING,
                         "FATAL: next opid %s stored in rdb index file invalid",
                         next_opid);
                exit(1);
            }
            server.next_opid = pos;
            server.min_valid_opid = pos;
        }

        if(aof_filename && access(REDIS_RESTORE_TIMESTAMP_FILE, F_OK) != -1) {
            char *tbuffer =
                readAllContentsFromTextFile(REDIS_RESTORE_TIMESTAMP_FILE);
            if(tbuffer == NULL) {
                serverLog(LL_WARNING,
                         "FATAL: read all contents from %s failed",
                         REDIS_RESTORE_TIMESTAMP_FILE);
                exit(1);
            }
            char *occur = NULL;
            if((occur = strchr(tbuffer, '\n')) != NULL) *occur = '\0';
            char *endptr = NULL;
            long restore_timestamp = strtol(tbuffer, &endptr, 10);
            if(restore_timestamp < 0 || *endptr != '\0' || errno == ERANGE ||
               NULL == ctime(&restore_timestamp)) {
                serverLog(LL_WARNING,
                         "FATAL: timestamp %s stored in "
                         "restore.timestamp invalid",
                         tbuffer);
                exit(1);
            }
            serverLog(LL_NOTICE, "%s exist, redis will restore data"
                     " at specified timestamp: %ld",
                     REDIS_RESTORE_TIMESTAMP_FILE, restore_timestamp);
            server.restore_timestamp = restore_timestamp;
            zfree(tbuffer);
        }

        /* parse aof-inc.index */
        char *abuffer = NULL;
        list *aof_list = NULL;
        if(aof_filename) {
            abuffer = readAllContentsFromTextFile(AOF_INDEX_FILE);
            if(NULL == abuffer) {
                serverLog(LL_WARNING,
                         "FATAL: read all contents from %s failed",
                         AOF_INDEX_FILE);
                exit(1);
            }
            aof_list = listCreate();
            char *aof_item = strtok(abuffer, "\n");
            int found = 0;
            int first = 1;
            char *last_aof_item = NULL;
            while(aof_item != NULL) {
                if(access(aof_item, F_OK) == -1) {
                    serverLog(LL_WARNING,
                             "FATAL: aof file %s listed in index file"
                             " doesn't exist",
                             aof_item);
                    exit(1);
                }
                if(first) {
                    long long first_opid = getAofFirstOpid(aof_item);
                    if(first_opid > 0) {
                        server.min_valid_opid = first_opid;
                        first = 0;
                    }
                }
                if(!strcmp(aof_item, aof_filename)) found++;
                if(found) {
                    if(last_aof_item && !strcmp(aof_item, last_aof_item)) {
                        serverLog(LL_WARNING,
                                 "FATAL: repeat aof file %s found in"
                                 " aof-inc.index",
                                 aof_item);
                        exit(1);
                    }
                    listAddNodeTail(aof_list, aof_item);
                }
                last_aof_item = aof_item;
                aof_item = strtok(NULL, "\n");
            }
            if(found != 1) {
                serverLog(LL_WARNING,
                         "FATAL: %s appeared %d times in aof-inc.index file",
                         aof_filename, found);
                exit(1);
            }
        }

        /* start loading */
        long long total_start = ustime();
        loadDataFromRdb(rdb_filename, 1);
        server.last_cron_bgsave_mem_use = zmalloc_used_memory();

        if(aof_filename) {
            listIter li;
            listNode *ln;
            long long start = ustime();
            listRewind(aof_list, &li);
            while((ln = listNext(&li))) {
                long long every_aof_loading_start = ustime();
                if (loadAppendOnlyFile(ln->value) == C_OK) {
                    serverLog(LL_NOTICE,"DB loaded from append only "
                             "file %s: %.3f seconds", (char *)ln->value,
                             (float)(ustime()-every_aof_loading_start)/1000000);
                } else {
                    serverLog(LL_NOTICE,"zero size big aof loaded: %s",
                             (char *)ln->value);
                }
            }
            serverLog(LL_NOTICE,"DB loaded from all the append only "
                     "file: %.3f seconds",(float)(ustime()-start)/1000000);
        }

        serverLog(LL_NOTICE,"DB loaded from rdb/aof, total time: %.3f seconds",
                 (float)(ustime()-total_start)/1000000);

        /* clear */
        if(aof_list) listRelease(aof_list);
        aof_list = NULL;
        zfree(rbuffer);
        zfree(abuffer);
        unlink(REDIS_RESTORE_TIMESTAMP_FILE);
    }
}

int loadOplogFromAppendOnlyFile(char *filename) {
    struct client *fakeClient;
    FILE *fp = fopen(filename,"r");
    struct redis_stat sb;
    sb.st_size = 0;
    int old_aof_state = server.aof_state;
    long loops = 0;
    off_t valid_up_to = 0; /* Offset of the latest well-formed command loaded. */
    off_t aof_size_will_loaded = 0;
    struct redisCommand *aof_loaded_lastcmd = NULL;

    /* if the loading aof file is empty, just close the file and
     * return C_ERR to inform the caller that empty aof is loaded */
    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        fclose(fp);
        return C_ERR;
    }

    if (fp == NULL) {
        serverLog(LL_WARNING,
                 "Fatal error: can't open the append log file for reading: %s",
                 strerror(errno));
        exit(1);
    }

    if (server.aof_position_stored > sb.st_size) {
        serverLog(LL_WARNING,
                 "Fatal error: aof position recorded in rdb.index"
                 " is bigger than the size of %s",
                 filename);
        exit(1);
    }

    aof_size_will_loaded = sb.st_size - server.aof_position_stored;

    if (server.aof_position_stored != 0 &&
        fseeko(fp, server.aof_position_stored, SEEK_SET) != 0) {
        serverLog(LL_WARNING,"Fatal error: fseek file %s error: %s",
                 filename, strerror(errno));
        exit(1);
    }
    server.aof_position_stored = 0;

    /* Temporarily disable AOF, to prevent EXEC from feeding a MULTI
     * to the same file we're about to read. */
    server.aof_state = AOF_OFF;

    fakeClient = createFakeClient();
    startLoading(fp);
    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        /* Serve the clients from time to time */
        if (!(loops++ % 1000)) {
            loadingProgress(ftello(fp));
            processEventsWhileBlocked();
        }

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            if (feof(fp))
                break;
            else
                goto readerr;
        }
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;

        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            if (buf[0] != '$') goto fmterr;
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(NULL,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            argv[j] = createObject(OBJ_STRING,argsds);
            if (fread(buf,2,1,fp) == 0) {
                fakeClient->argc = j+1; /* Free up to j. */
                freeFakeClientArgv(fakeClient);
                goto readerr; /* discard CRLF */
            }
        }

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            serverLog(LL_WARNING,
                     "Unknown command '%s' reading the append only file",
                     (char*)argv[0]->ptr);
            exit(1);
        }

        if (cmd->proc == opinfoCommand) {
            redisOplogHeader *loaded_header = (redisOplogHeader *)argv[1]->ptr;
            /* if we need to restore data to specified timestamp with
             * server.restore_timestamp non zero and current loaded command's
             * execute time is later than the specified timestamp
             * we should stop loading */
            if (server.restore_timestamp != 0 &&
                loaded_header->timestamp > server.restore_timestamp) {
                /* after we restored data to specified timestamp,
                 * the data in aof after the opinfo command(included) is not
                 * needed so the last loaded aof should be truncated */
                if (truncate(filename, valid_up_to) == -1) {
                    serverLog(LL_WARNING,"Error truncating the AOF file %s: %s",
                             filename, strerror(errno));
                }
                freeFakeClientArgv(fakeClient);
                serverLog(LL_NOTICE, "restored data to timestamp %ld in aof %s",
                         server.restore_timestamp, filename);
                goto loaded_ok;
            }
        }

        aof_loaded_lastcmd = cmd;

        /* Run the command in the context of a fake client */
        cmd->proc(fakeClient);

        /* The fake client should not have a reply */
        serverAssert(fakeClient->bufpos == 0 &&
                     listLength(fakeClient->reply) == 0);
        /* The fake client should never get blocked */
        serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, log error and quit. */
    if (fakeClient->flags & CLIENT_MULTI) goto uxeof;

loaded_ok: /* DB loaded, cleanup and return C_OK to the caller. */
    fclose(fp);
    if (aof_loaded_lastcmd && aof_loaded_lastcmd->proc == opinfoCommand) {
        serverLog(LL_NOTICE,
                 "last loaded command from aof is opinfo(no real data), "
                 "revert server next opid: %lld",
                 server.next_opid);
        server.next_opid--;
    }
    freeFakeClient(fakeClient);
    server.aof_state = old_aof_state;
    stopLoading();
    server.aof_rewrite_base_size = server.aof_current_size;
    server.aof_inc_from_last_cron_bgsave += aof_size_will_loaded;
    return C_OK;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
        serverLog(LL_WARNING,
                 "Unrecoverable error reading the append only file: %s",
                 strerror(errno));
        exit(1);
    }

uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING,
                 "!!! Warning: short read while loading the AOF file !!!");
        serverLog(LL_WARNING,"!!! Truncating the AOF at offset %llu !!!",
                 (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(filename,valid_up_to) == -1) {
            if (valid_up_to == -1) {
                serverLog(LL_WARNING,"Last valid command offset is invalid");
            } else {
                serverLog(LL_WARNING,"Error truncating the AOF file: %s",
                         strerror(errno));
            }
        } else {
            serverLog(LL_WARNING,
                     "AOF loaded anyway because aof-load-truncated is enabled");
            goto loaded_ok;
            // /* Make sure the AOF file descriptor points to the end of the
            //  * file after the truncate call. */
            // if (server.aof_fd != -1 && lseek(server.aof_fd,0,SEEK_END) == -1) {
            //     serverLog(LL_WARNING,"Can't seek the end of the AOF file: %s",
            //              strerror(errno));
            // } else {
            //     serverLog(LL_WARNING,
            //              "AOF loaded anyway because aof-load-truncated is enabled");
            //     goto loaded_ok;
            // }
        }
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,
             "Unexpected end of file reading the append only file. "
             "You can: 1) Make a backup of your AOF file, then use "
             "./redis-check-aof --fix <filename>. 2) Alternatively you can set"
             " the 'aof-load-truncated' configuration option to yes and "
             "restart the server.");
    exit(1);

fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Bad file format reading the append only file: make a "
             "backup of your AOF file, then use "
             "./redis-check-aof --fix <filename>");
    exit(1);
}

void addAofIncrementStatisticsBy(ssize_t nwritten) {
    server.aof_total_size += nwritten;
    server.aof_inc_from_last_cron_bgsave += nwritten;
}

void deleteAofIfNeeded() {
    if (server.auto_purge_aof == 0 ||
        server.aof_state != AOF_ON) return;

    list *aof_list = NULL;
    listIter li;
    listNode *ln;
    /* the maximum size of aof logs which redis can keep at most */
    long long max_aof_keeping_size = 0;

    /* check if there is too many zero size aof and clean */
    struct stat aof_index_file_st;
    if (stat(AOF_INDEX_FILE, &aof_index_file_st) == 0 &&
        aof_index_file_st.st_size >
        (REDIS_INDEX_FILE_MAX_SIZE * REDIS_AOF_INDEX_MAX_USED_PERCENTAGE / 100)
        ) {
        /* try best to delete */
        doPurgeAof(server.aof_filename);
        /* recheck */
        if (stat(AOF_INDEX_FILE, &aof_index_file_st) == 0 &&
            aof_index_file_st.st_size >
            (REDIS_INDEX_FILE_MAX_SIZE *
             REDIS_AOF_INDEX_MAX_USED_PERCENTAGE / 100)) {
            /* start a bgsave to make sure that
             * current rdb won't corresponding to a very old aof */
            if (server.rdb_child_pid == -1 && server.aof_child_pid == -1) {
                rdbSaveBackground(server.rdb_filename, 0);
            }

        }
        return;
    }

    /* init */
    aof_list = readAofIndexIntoList();
    if (!aof_list) return;
    /* if only one aof left(the currently writing one), we can't delete aof */
    if (listLength(aof_list) < 2) {
        listRelease(aof_list);
        return;
    }
    listRewindTail(aof_list, &li);

    if (server.maxmemory) {
        max_aof_keeping_size = server.maxmemory *
            server.cron_bgsave_rewrite_perc / 100;
    } else {
        max_aof_keeping_size = REDIS_DEFAULT_AOF_KEEPING_SIZE;
    }

    /* get the aof name that will be the last one to be deleted */
    while ((ln = listNext(&li))) {
        struct stat st;
        off_t file_size;
        if (stat(ln->value, &st) != 0) { /* error with file stat call */
            if (errno != ENOENT) {
                serverLog(LL_WARNING,
                         "auto purge aof, get file size of %s failed: %s",
                         (char *)ln->value, strerror(errno));
            }
            /* if file stat failed, set file size to zero */
            file_size = 0;
        } else {
            file_size = st.st_size;
        }
        if ((max_aof_keeping_size -= file_size) < 0) break;
    }

    /* purge from the first one to the chosen one */
    if (ln) doPurgeAof(ln->value);

    /* clean */
    listRelease(aof_list);
}

/* get the earliest aof that opdel source are currently using */
char *getEarliestAofFromOpdelSrc(char *earliest_aof, list *aof_list) {
    if (dictSize(server.opdel_source_opid_dict) == 0 || !aof_list) {
        return NULL;
    }

    long long min_opid = LLONG_MAX;
    dictIterator *di = dictGetIterator(server.opdel_source_opid_dict);
    dictEntry *de;
    listIter li;
    listNode *ln;
    listRewind(aof_list, &li);
    strncpy(earliest_aof, server.aof_filename, REDIS_AOF_FILENAME_LEN);
    char *last_non_empty_aof = aof_list->head->value;

    /* get min opid */
    while((de = dictNext(di)) != NULL) {
        robj *val = dictGetVal(de);
        redisOpdelSource *source = val->ptr;
        if (source->opid < min_opid) min_opid = source->opid;
    }
    dictReleaseIterator(di);

    /* get earliest aof */
    while((ln = listNext(&li))) {
        long long first_opid = getAofFirstOpid(ln->value);
        if (first_opid > min_opid) {
            strncpy(earliest_aof,
                    last_non_empty_aof,
                    REDIS_AOF_FILENAME_LEN);
            break;
        }
        if (first_opid >= 1) last_non_empty_aof = ln->value;
    }

    return earliest_aof;
}

int doPurgeAof(char *name) {
    if (name == NULL) return -1;
    int use_aof_name_in_rdb_index = 1;
    int write_err = 0;
    char *aof_filename = NULL, *rbuffer;
    FILE *aof_inc_index_tmp_fp = NULL;

    /* parse rdb.index */
    rbuffer = readAllContentsFromTextFile(REDIS_RDB_INDEX_FILENAME);
    if (NULL == rbuffer) {
        serverLog(LL_WARNING,
                 "read all contents from %s failed, "
                 "purge aof will not consider aof name in rdb.index",
                 REDIS_RDB_INDEX_FILENAME);
        use_aof_name_in_rdb_index = 0;
    } else {
        char *occur = NULL;
        if((occur = strchr(rbuffer, '\n')) != NULL) *occur = '\0';

        char *rdb_filename = strtok(rbuffer, " ");
        aof_filename = strtok(NULL, " ");
        char *aof_position = strtok(NULL, " ");
        char *next_opid = strtok(NULL, " ");
        char *end_guard = strtok(NULL, " ");

        if(!(rdb_filename != NULL && aof_filename != NULL
             && aof_position != NULL && next_opid !=NULL
             && end_guard == NULL)) {
            serverLog(LL_WARNING,
                     "%s file format error, "
                     "purge aof will not consider aof name in rdb.index",
                     REDIS_RDB_INDEX_FILENAME);
            use_aof_name_in_rdb_index = 0;
        } else if (access(rdb_filename, F_OK) == -1) {
            serverLog(LL_WARNING,
                     "rdb file %s listed in index file doesn't exist, "
                     "purge aof will not consider aof name in rdb.index",
                     rdb_filename);
            use_aof_name_in_rdb_index = 0;
        } else if (aof_filename && access(aof_filename, F_OK) == -1) {
            serverLog(LL_WARNING,
                     "aof file %s listed in index file doesn't exist, "
                     "purge aof will not consider aof name in rdb.index",
                     aof_filename);
            use_aof_name_in_rdb_index = 0;
        } else if (aof_position) {
            char *endptr = NULL;
            long pos = strtol(aof_position, &endptr, 10);
            if(pos < 0 || *endptr != '\0' || errno == ERANGE) {
                serverLog(LL_WARNING,
                         "aof position %s stored in index file invalid, "
                         "purge aof will not consider aof name in rdb.index",
                         aof_position);
                use_aof_name_in_rdb_index = 0;
            }
        }
    }

    /* parse aof-inc.index */
    char *abuffer = NULL;
    list *aof_list = NULL;

    abuffer = readAllContentsFromTextFile(AOF_INDEX_FILE);
    if(NULL == abuffer) {
        serverLog(LL_WARNING,
                 "read all contents from %s failed", AOF_INDEX_FILE);
        goto purgeerr;
    }
    aof_list = listCreate();
    char *aof_item = strtok(abuffer, "\n");
    int found = 0;
    while(aof_item != NULL) {
        if(strcmp(name, aof_item) == 0) found = 1;
        listAddNodeTail(aof_list, aof_item);
        aof_item = strtok(NULL, "\n");
    }
    if(found != 1) {
        serverLog(LL_WARNING,
                 "%s doesn't exist in aof-inc.index file", name);
        goto purgeerr;
    }

    /* get earliest aof from opdel source */
    char aof_filename_buf[256] = {'\0'};
    char *earliest_aof = getEarliestAofFromOpdelSrc(aof_filename_buf, aof_list);

    /* del */
    listIter li;
    listNode *ln;
    listRewind(aof_list, &li);
    int purge_count = 0;
    while((ln = listNext(&li))) {
        /* conditions when we should stop deleting aof */
        if((aof_filename != NULL && use_aof_name_in_rdb_index &&
            !strcmp(ln->value, aof_filename)) /* for rdb */
           || !strcmp(ln->value,
                      server.aof_filename) /* for server currently writing aof */
           || (server.aof_psync_cur_reading_name &&
               !strcmp(ln->value,
                       server.aof_psync_cur_reading_name)) /* for aof psync */
           || (earliest_aof && !strcmp(ln->value,
                                       earliest_aof)) /* for opget clients */
            ) {
            break;
        }
        bioCreateBackgroundJob(BIO_AOF_PURGE,
                               (void *)zstrdup((char *)ln->value), NULL, NULL);
        purge_count++;
        if(!strcmp(ln->value, name)) {
            ln = listNext(&li);
            break;
        }
    }

    /* rewrite aof-inc.index */
    aof_inc_index_tmp_fp = fopen(REDIS_AOF_INDEX_TMP_FILENAME, "w");
    if(aof_inc_index_tmp_fp == NULL) {
        write_err = 1;
        goto purgeerr;
    }

    found = 0;
    server.min_valid_opid = server.next_opid;
    while(ln) {
        if(fprintf(aof_inc_index_tmp_fp, "%s\n", (char *)ln->value) < 0) {
            write_err = 1;
            goto purgeerr;
        }
        if (!found) {
            long long min_opid = getAofFirstOpid(ln->value);
            if (min_opid > 0) {
                found = 1;
                server.min_valid_opid = min_opid;
            }
        }
        ln = listNext(&li);
    }
    /* need to fflush first, otherwise if bio fsync is delayed too long and
     * next aof index update has come, aof index will be broken */
    if (fflush(aof_inc_index_tmp_fp) == EOF) {
        write_err = 1;
        goto purgeerr;
    }
    bioCreateBackgroundJob(BIO_CLOSE_FILE,
                           (void *)aof_inc_index_tmp_fp,
                           (void *)(long)BIO_CLOSE_FP, NULL);
    aof_inc_index_tmp_fp = NULL;

    /* Open file descriptors for oldpath are not affected when rename() */
    if (rename(REDIS_AOF_INDEX_TMP_FILENAME, AOF_INDEX_FILE) == -1) {
        serverLog(LL_WARNING,"Error moving temp aof-inc.index "
                 "file on the final destination: %s", strerror(errno));
        goto purgeerr;
    }

    if(aof_list) listRelease(aof_list);
    aof_list = NULL;
    zfree(rbuffer);
    zfree(abuffer);
    return purge_count;

purgeerr:
    /* clear */
    if (aof_list) listRelease(aof_list);
    aof_list = NULL;
    if (aof_inc_index_tmp_fp) fclose(aof_inc_index_tmp_fp);
    zfree(rbuffer);
    zfree(abuffer);
    if(write_err)
        serverLog(LL_WARNING,"Write error tmp-aof-inc.index: %s",
                 strerror(errno));
    return -1;
}

void aofPurgeCron(void) {
    run_with_period(REDIS_AOF_AUTO_PURGE_PERIOD) deleteAofIfNeeded();
}

/* purgeaofto aof_file_name */
void purgeAofToCommand(client *c) {
    if (server.aof_state != AOF_ON) {
        addReplyError(c, "redis aof is off");
    }

    int ret = doPurgeAof(c->argv[1]->ptr);
    if (ret < 0)
        addReplyErrorFormat(c, "purge aof to %s failed",
                            (char *)c->argv[1]->ptr);
    else
        addReplyLongLong(c, ret);
}

char *readAllContentsFromTextFile(char *filename) {
    FILE *fp = fopen(filename, "r");
    if(fp == NULL) {
        serverLog(LL_WARNING,
                 "Open %s failed: %s", filename, strerror(errno));
        return NULL;
    }

    fseek(fp, 0, SEEK_END);
    size_t fsize = ftell(fp);
    size_t n = 0;
    fseek(fp, 0, SEEK_SET);  //same as rewind(f)
    if(fsize > REDIS_INDEX_FILE_MAX_SIZE) {
        serverLog(LL_WARNING, "Expected text file %s is too big", filename);
        fclose(fp);
        return NULL;
    }

    char *buffer = zmalloc(fsize + 1);
    if((n = fread(buffer, 1, fsize, fp)) != fsize) {
        serverLog(LL_WARNING,
                 "Read all contents(%ld bytes) from %s failed, read(%ld): %s",
                 fsize, filename, n, strerror(errno));
        zfree(buffer);
        fclose(fp);
        return NULL;
    }

    fclose(fp);
    buffer[fsize] = 0;
    return buffer;
}

list *readAofIndexIntoList(){
    list *aof_list = listCreate();
    if(!aof_list) {
        serverLog(LL_WARNING, "create list failed for getting aof list");
        return NULL;
    }
    listSetFreeMethod(aof_list, zfree);

    char *aof_index_buffer = readAllContentsFromTextFile(AOF_INDEX_FILE);
    if (aof_index_buffer == NULL) {
        listRelease(aof_list);
        return NULL;
    }

    char *aof_item = strtok(aof_index_buffer, "\n");
    while(aof_item != NULL) {
        listAddNodeTail(aof_list, zstrdup(aof_item));
        aof_item = strtok(NULL, "\n");
    }

    zfree(aof_index_buffer);
    return aof_list;
}

int checkIfNoNeedToFreeMem(client *c) {
    if (server.masterhost ||
        (c && c->cmd && !(c->cmd->flags & CMD_WRITE))) {
        return C_OK;
    }

    return C_ERR;
}

int debugLoadDisk(client *c) {
    if (!strcasecmp(c->argv[1]->ptr, "loaddisk")) {
        flushAppendOnlyFile(1);
        emptyDb(-1, EMPTYDB_NO_FLAGS, NULL);
        loadDataFromDisk();
        server.dirty = 0; /* Prevent AOF / replication */
        serverLog(LL_WARNING,"RDB and Append Only File loaded by DEBUG LOADADISK");
        addReply(c, shared.ok);
        return C_OK;
    } else {
        return C_ERR;
    }
}

void opDelCommand(client *c) {
    long long opid;
    dictEntry *dentry;
    redisOpdelSource *source;
    robj *tmp;

    if (getLongLongFromObjectOrReply(c, c->argv[2],
                                     &opid, NULL) != C_OK) return;

    if (opid == -1) {
        dictDelete(server.opdel_source_opid_dict, c->argv[1]);
        addReply(c, shared.ok);
        return;
    }

    if (opid < 1) {
        addReplyError(c, "invalid opid");
        return;
    }

    if (strchr(c->argv[1]->ptr, ',') != NULL ||
        strchr(c->argv[1]->ptr, '=') != NULL) {
        addReplyError(c, "',' or '=' in opdel source name");
        return;
    }

    if ((dentry = dictFind(server.opdel_source_opid_dict, c->argv[1]))) {
        /* already existed */
        tmp = dictGetVal(dentry);
    } else {
        /* first create */
        tmp = createStringObject(NULL, sizeof(redisOpdelSource));
        dictAdd(server.opdel_source_opid_dict, c->argv[1], tmp);
        incrRefCount(c->argv[1]);
    }

    source = tmp->ptr;
    source->opid = opid;
    source->last_update_time = server.unixtime;

    addReply(c, shared.ok);
}

void opinfoCommand(struct client *c) {
    /* only master client, fake client and opapply client is
     * allowed to execute opinfo */
    if (!(c->flags & (CLIENT_MASTER |
                      REDIS_OPAPPLY_CLIENT |
                      REDIS_FAKECLIENT))) {
        addReplyError(c,
                      "opinfo command can not be called in common connection");
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        return;
    }

    redisOplogHeader *header = (redisOplogHeader *)c->argv[1]->ptr;

    if (c->flags & REDIS_OPAPPLY_CLIENT) {
        /* ignore oplog whose serverid is the same with current redis's or
         * whose opid is less than or equal to last oplog's,
         * which means the oplog has been applied before */
        if (header->server_id == server.server_id ||
            header->opid <=
            dictGetAppliedOpidBySrcServerId(header->server_id)) {
            c->flags |= REDIS_OPAPPLY_IGNORE_CMDS;
        } else {
            c->flags &= ~REDIS_OPAPPLY_IGNORE_CMDS;
            c->opapply_src_opid = header->opid;
            c->opapply_src_serverid = header->server_id;
            selectDb(c, header->dbid);
        }
    } else { /* for aof loading or slave replication */
        server.next_opid = header->opid + 1;
        /* update src server -> applied opid */
        if (server.server_id != header->server_id) {
            updateSrcServerIdOpid(header->server_id, header->src_opid);
        }
        server.dirty++;
        selectDb(c, header->dbid);
    }

    addReply(c, shared.ok);
}

void checkAndSetDelType(struct redisCommand *cmd, redisOplogHeader *header,
                        int del_type) {
    if (cmd->proc != delCommand) return;
    if (del_type == REDIS_DEL_BY_EVICTION) {
        header->cmd_flag |= REDIS_OPLOG_DEL_BY_EVICTION_FLAG;
    } else if (del_type == REDIS_DEL_BY_EXPIRE) {
        header->cmd_flag |= REDIS_OPLOG_DEL_BY_EXPIRE_FLAG;
    }
}

void initOplogHeader(client *c, struct redisCommand *cmd,
                     redisOplogHeader *header, int dictid,
                     int cmd_num, int del_type) {
    if (c && c->flags & REDIS_OPAPPLY_CLIENT) {
        header->server_id = c->opapply_src_serverid;
        header->src_opid = c->opapply_src_opid;
    } else {
        header->server_id = server.server_id;
        header->src_opid = -1;
    }
    header->opid = server.next_opid++;
    header->timestamp = server.unixtime;
    header->version = REDIS_OPLOG_VERSION;
    header->dbid = dictid;
    header->cmd_num = cmd_num;
    header->cmd_flag = 0;
    checkAndSetDelType(cmd, header, del_type);
}

void feedSlavesOrAofWithOplogHeader(client *c, struct redisCommand *cmd,
                                    list *slaves, int dictid,
                                    int cmd_num, int feedtype, int del_type) {
    if(server.masterhost != NULL) return;

    robj *argv[2] = {0};
    argv[0] = shared.opinfo;
    argv[1] = shared.oplogheader;

    if (REDIS_FEED_SLAVES == feedtype) {
        replicationFeedSlaves(c, server.opinfoCommand, slaves, dictid, argv, 2);
    } else if (REDIS_FEED_AOF == feedtype) {
        redisOplogHeader *header;
        header = (redisOplogHeader *)argv[1]->ptr;
        initOplogHeader(c, cmd, header, dictid, cmd_num, del_type);
        feedAppendOnlyFile(c, server.opinfoCommand, dictid, argv, 2,
                           REDIS_DEL_NONE);
    }
}

sds feedAofFirstCmdWithOpinfo(struct redisCommand *cmd, int del_type, int dictid) {
    /* to ensure that first command is opinfo after aof split */
    if (server.aof_current_size == 0 &&
        sdslen(server.aof_buf) == 0 &&
        server.masterhost != NULL &&
        server.next_opid > 1 &&
        cmd->proc != opinfoCommand) {
        redisOplogHeader header;
        header.server_id = server.server_id;
        header.src_opid = -1;
        header.opid = server.next_opid-1; /* rollback opid */
        header.timestamp = server.unixtime;
        header.version = REDIS_OPLOG_VERSION;
        header.dbid = dictid;
        header.cmd_num = 3;
        checkAndSetDelType(cmd, &header, del_type);
        server.aof_buf = sdscatprintf(server.aof_buf,
                                      "*2\r\n$6\r\nopinfo\r\n$%lu\r\n",
                                      sizeof(header));
        server.aof_buf = sdscatlen(server.aof_buf, &header, sizeof(header));
        server.aof_buf = sdscatlen(server.aof_buf, "\r\n", 2);
    }

    return server.aof_buf;
}

int checkSetCmdWithPxExOption(client *c, struct redisCommand *cmd) {
    if (cmd->proc == setCommand && c->argc > 3) {
        int i;
        for (i = 3; i < c->argc; i ++) {
            if (!strcasecmp(c->argv[i]->ptr, "ex") ||
                !strcasecmp(c->argv[i]->ptr, "px")) {
                return C_OK;
            }
        }
    }

    return C_ERR;
}

void checkAndFeedAofWithOplogHeader(client *c, struct redisCommand *cmd,
                                    int dictid, int del_type) {
    if ((c && (c->flags & CLIENT_MULTI) && cmd->proc == multiCommand) ||
        (c && !(c->flags & CLIENT_MULTI) && cmd->proc != opinfoCommand &&
         cmd->proc != execCommand) ||
        (!c && cmd->proc != opinfoCommand)) {
        int8_t cmd_num = 2;
        if (cmd->proc == setexCommand || cmd->proc == psetexCommand ||
            checkSetCmdWithPxExOption(c, cmd) == C_OK) {
            cmd_num = 3;
        } else if (cmd->proc == multiCommand) {
            cmd_num = 0;
        }
        feedSlavesOrAofWithOplogHeader(c, cmd, NULL, dictid, cmd_num,
                                       REDIS_FEED_AOF, del_type);
    }
}

void checkAndFeedSlaveWithOplogHeader(client *c, struct redisCommand *cmd,
                                      int dictid) {
    if ((c && (c->flags & CLIENT_MULTI) && cmd->proc == multiCommand) ||
        (c && !(c->flags & CLIENT_MULTI) && cmd->proc != opinfoCommand &&
         cmd->proc != execCommand && cmd->proc != publishCommand) ||
        (!c && cmd->proc != opinfoCommand && cmd->proc != pingCommand)) {
        if (server.aof_state != AOF_OFF) {
            feedSlavesOrAofWithOplogHeader(c, cmd, server.slaves, dictid,
                                           0, REDIS_FEED_SLAVES,
                                           REDIS_DEL_NONE);
        }
    }
}

void revertOpidIfNeeded(void) {
    /* if opinfo command is the last command received by slave
     * it is an invalid oplog header, we should revert server.next_opid */
    if (server.master->lastcmd &&
        server.master->lastcmd->proc == opinfoCommand &&
        server.next_opid > 1) {
        serverLog(LL_NOTICE,
                 "last received command from master is opinfo(no real data), "
                 "revert server next opid: %lld", server.next_opid);
        server.next_opid--;
    }
}

void activeFullResyncOnProtocolErr(client *c){
    /* if bad protocol cmds received from master, slave go full resync */
    if (c->flags & REDIS_PROTOCOL_ERROR) {
        server.next_opid = LLONG_MIN;
        if (server.master) {
            server.master->read_reploff = server.master->reploff = LLONG_MAX;
        }
        serverLog(LL_WARNING,
                 "bad protocol command received from master, go full resync");
    }
}

void setAutoSyncForIncrFsync(rio *rdb) {
    if (server.rdb_save_incremental_fsync)
        rioSetAutoSync(rdb, REDIS_RDB_AUTOSYNC_BYTES);
}

int rdbSaveAppliedInfoToRdbIndex(FILE *fp) {
    dictIterator *di = dictGetIterator(server.src_serverid_applied_opid_dict);
    dictEntry *de = NULL;
    while ((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        robj *val = dictGetVal(de);
        if(fprintf(fp, "%ld %ld ", (long)key->ptr, (long)val->ptr) < 0) {
            serverLog(LL_WARNING, "Writing %s error: %s",
                     REDIS_RDB_INDEX_TMP_FILENAME, strerror(errno));
            dictReleaseIterator(di);
            return C_ERR;
        }
    }
    dictReleaseIterator(di);
    if (fputs("\n", fp) < 0) {
        serverLog(LL_WARNING, "Writing %s error: %s",
                 REDIS_RDB_INDEX_TMP_FILENAME, strerror(errno));
        return C_ERR;
    }

    return C_OK;
}

int writeAndFlushTmpRdbIndex(FILE *fp_tmp_rdb_index, char *filename) {
    if (!fp_tmp_rdb_index) {
        serverLog(LL_WARNING, "Failed opening %s for saving: %s",
                 REDIS_RDB_INDEX_TMP_FILENAME, strerror(errno));
        return C_ERR;
    }

    if(server.aof_state != AOF_OFF) {
        if(fprintf(fp_tmp_rdb_index, "%s %s %ld %lld\n", \
                   filename, server.aof_filename, \
                   server.aof_current_size,
                   (server.next_opid < 1 ? 0 : server.next_opid)) < 0) {
            serverLog(LL_WARNING, "Writing %s error: %s",
                     REDIS_RDB_INDEX_TMP_FILENAME, strerror(errno));
            return C_ERR;
        }
        serverLog(LL_NOTICE,
                 "BGSAVE done, write rdb.index, rdb name: %s, aof name: %s, "
                 "aof offset: %ld, next opid: %lld",
                 filename, server.aof_filename, \
                 server.aof_current_size, server.next_opid);
        if (rdbSaveAppliedInfoToRdbIndex(fp_tmp_rdb_index) != C_OK) {
            return C_ERR;
        }
    } else {
        if(fprintf(fp_tmp_rdb_index, "%s\n", filename) < 0) {
            serverLog(LL_WARNING, "Writing %s error: %s",
                     REDIS_RDB_INDEX_TMP_FILENAME, strerror(errno));
            return C_ERR;
        }
    }

    if (fflush(fp_tmp_rdb_index) == EOF) return C_ERR;
    if (fsync(fileno(fp_tmp_rdb_index)) == -1) return C_ERR;
    if (fclose(fp_tmp_rdb_index) == EOF) return C_ERR;

    return C_OK;
}

int renameTmpRdbIndex(void) {
    if (rename(REDIS_RDB_INDEX_TMP_FILENAME, REDIS_RDB_INDEX_FILENAME) == -1) {
        serverLog(LL_WARNING,
                 "Error moving temp rdb index file on the final"
                 " destination: %s",
                 strerror(errno));
        return C_ERR;
    }

    return C_OK;
}

void cleanTmpRdbIndex(FILE *fp_tmp_rdb_index) {
    if(fp_tmp_rdb_index) {
        fclose(fp_tmp_rdb_index);
    }

    if(access(REDIS_RDB_INDEX_TMP_FILENAME, F_OK) != -1) {
        unlink(REDIS_RDB_INDEX_TMP_FILENAME);
    }
}

void removeTmpRdbIndexIfNeeded(char *tmpfile) {
    if (access(tmpfile, F_OK) != -1) {
        unlink(REDIS_RDB_INDEX_TMP_FILENAME);
    } else if (access(REDIS_RDB_INDEX_TMP_FILENAME, F_OK) != -1) {
        rename(REDIS_RDB_INDEX_TMP_FILENAME, REDIS_RDB_INDEX_FILENAME);
    }
}

void initFirstRdbIndex(void) {
    if (access(REDIS_RDB_INDEX_FILENAME, F_OK) == -1) {
        rdbSaveBackground(server.rdb_filename, NULL);
    }
}

sds genFakeClientInfo(client *c, sds s) {
    return sdscatfmt(s, "db=%i multi=%i cmd=%s type=fakeclient",
                     (c->db ? c->db->id : -1),
                     ((c->flags & CLIENT_MULTI) ? c->mstate.count : -1),
                     (c->lastcmd ? c->lastcmd->name : "NULL"));
}

/********************************** aof psync ********************************/
void resetAofPsyncState() {
    /* clean aof psync state */
    if (server.aof_psync_slave_offset->fd > 0) {
        close(server.aof_psync_slave_offset->fd);
    }
    server.do_aof_psync_send = 1;
    server.aof_psync_cur_reading_name = NULL;
    zfree(server.aof_psync_slave_offset);
    /* aof_psync_slave_offset is NULL shows that
     * current aof psync for one slave is done */
    server.aof_psync_slave_offset = NULL;
}

int masterSlaveofCurrentSlaveCheck(char *slaveof_ip, int slaveof_port) {
    listIter li;
    listNode *ln;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        char ip[NET_IP_STR_LEN];
        client *slave = listNodeValue(ln);
        if (anetPeerToString(slave->fd,ip,sizeof(ip),NULL) != -1 &&
            slave->slave_listening_port &&
            slaveof_port == slave->slave_listening_port &&
            !strcasecmp(slaveof_ip, ip)) {
            return C_ERR;
        }
    }

    return C_OK;
}

void restoreReplInfoFromConfig(void) {
    serverLog(LL_NOTICE, "restoring repl info from config, replid:%s, "
              "repl_offset:%lld, repl_stream_db:%d, replid2:%s, "
              "second_replid_opid:%lld",
              server.rsi_config->replid, server.rsi_config->repl_offset,
              server.rsi_config->repl_stream_db, server.rsi_config->replid2,
              server.rsi_config->second_replid_opid);

    if (strlen(server.rsi_config->replid)) {
        memcpy(server.replid, server.rsi_config->replid,
               sizeof(server.replid));
    }

    if (server.masterhost &&
        server.rsi_config->repl_offset != -1 &&
        server.rsi_config->repl_stream_db != -1) {
        server.master_repl_offset = server.rsi_config->repl_offset;
        /* If we are a slave, create a cached master from this
         * information, in order to allow partial resynchronizations
         * with masters. */
        replicationCacheMasterUsingMyself();
        selectDb(server.cached_master,server.rsi_config->repl_stream_db);
    }

    if (server.rsi_config->second_replid_opid > 1) {
        memcpy(server.replid2, server.rsi_config->replid2,
               sizeof(server.replid2));
        server.second_replid_opid = server.rsi_config->second_replid_opid;
    }
}

void saveChangedReplInfoIntoConfig(void) {
    initSavedReplInfoInConfig();

    /* replid, current ancestor */
    if (!server.masterhost) {
        if (server.repl_backlog) {
            server.rsi_config->repl_stream_db = server.slaveseldb;
        }
    } else if (server.master) {
        server.rsi_config->repl_stream_db = server.master->db->id;
    }
    if(strlen(server.replid)) {
        memcpy(server.rsi_config->replid, server.replid,
               sizeof(server.replid));
    }
    server.rsi_config->repl_offset = server.master_repl_offset;

    /* replid2, ancestor before */
    if(strlen(server.replid2)) {
        memcpy(server.rsi_config->replid2, server.replid2,
               sizeof(server.replid2));
    }
    server.rsi_config->second_replid_opid = server.next_opid - 1;

    /* save it into config */
    rewriteConfig(server.configfile);
    serverLog(LL_NOTICE, "repl info saved into config, "
              "repl_stream_db:%d, "
              "replid:%s, "
              "repl_offset:%lld, "
              "replid2:%s, "
              "second_replid_opid:%lld",
              server.rsi_config->repl_stream_db, server.rsi_config->replid,
              server.rsi_config->repl_offset, server.rsi_config->replid2,
              server.rsi_config->second_replid_opid);
}

sds catExtraReplInfo(sds info) {
    info = sdscatprintf(info,
                        "aof_psyncing_state:%d\r\n"
                        "aof_psync_reading_filename:%s\r\n"
                        "aof_psync_reading_offset:%ld\r\n"
                        "next_opid:%lld\r\n"
                        "second_replid_opid:%lld\r\n",
                        (slaveCheckAofPsyncingState() == C_OK ? 1 : 0),
                        (server.aof_psync_slave_offset ?
                         server.aof_psync_slave_offset->aof_filename : "NULL"),
                        (server.aof_psync_slave_offset ?
                         server.aof_psync_slave_offset->c->repldboff : -1),
                        server.next_opid,
                        server.second_replid_opid);
    return info;
}

void startBgsaveAfterFullResync(void) {
    if (server.rdb_child_pid == -1) {
        /* after full resync with master,
         * we need to start a bgsave in case of data loss */
        int retry = 10;
        while (retry-- && rdbSaveBackground(server.rdb_filename, 0)
               == C_ERR) {
            serverLog(LL_WARNING,
                      "Failed start bgsave! Trying it again in one second.");
            sleep(1);
        }
        if (!retry) {
            serverLog(LL_WARNING,"FATAL: this slave instance finished the "
                      "synchronization with its master, "
                      "but bgsave can't be started. Exiting now.");
            exit(1);
        }
    }
}

void discardAofPsyncForSlaves() {
    listIter li;
    listNode *ln;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = listNodeValue(ln);
        syncWrite(slave->fd, AOF_PSYNC_FORCE_FULL_RESYNC_CMD,
                  AOF_PSYNC_FORCE_FULL_RESYNC_CMD_LEN,
                  AOF_PSYNC_SYNCWRITE_TIMEOUT);
    }
}

void forceSlavesFullResync() {
    /* Don't allow our chained slaves to aof psync
     * if we need a full resync from our master */
    discardAofPsyncForSlaves();
}

char *fillReplIdWithFullResyncReqIfNeeded(char *psync_replid) {
    if (strlen(psync_replid) == 0) {
        psync_replid = "?";
    }

    return psync_replid;
}

char *sendPsyncCmdToMasterWithExtraInfo(int fd, char *psync_replid,
                                        char *psync_offset) {
    char next_opid[32];
    char server_id[32];
    char repl_version[32];
    sds reply;
    long long tmp_slave_next_opid = server.next_opid;

    /* init */
    server.repl_master_initial_opid = -1;
    server.repl_version = 0;
    server.repl_master_aof_psyncing_state = 0;
    server.repl_master_initial_applied_info[0] = '\0';
    if (tmp_slave_next_opid <= REDIS_MIN_SLAVE_OPID_TO_AOF_PSYNC) {
        /* invalid slave next opid to force master start a fullresync with me */
        tmp_slave_next_opid = LLONG_MIN;
        serverLog(LL_NOTICE, "slave opid too small, go fullresync");
    }

    snprintf(next_opid, sizeof(next_opid),"%lld", tmp_slave_next_opid);
    snprintf(server_id,sizeof(server_id),"%lld", server.server_id);
    snprintf(repl_version, sizeof(repl_version), "%d", REDIS_REPL_VERSION);

    /* Issue the PSYNC command */
    reply = sendSynchronousCommand(SYNC_CMD_WRITE,
                                   fd, "PSYNC", psync_replid, psync_offset,
                                   next_opid, server_id, repl_version,
                                   "??", NULL);
    serverLog(LL_NOTICE,
             "slave syschronously send command to master: "
             "(PSYNC master_replid psync_offset slave_next_opid"
             " server_id repl_version current_master_runid)"
             "(PSYNC %s %s %s %s %s %s)",
             psync_replid, psync_offset, next_opid, server_id,
             repl_version, "??");

    return reply;
}

char *tryPsyncWithoutExtraInfoIfNeeded(int fd, char *reply) {
    /* for compatibility */
    if (!strncmp(reply,"-ERR",4)) {
        char *psync_replid;
        char psync_offset[32];

        if (server.cached_master) {
            psync_replid = server.cached_master->replid;
            psync_replid = fillReplIdWithFullResyncReqIfNeeded(psync_replid);
            snprintf(psync_offset,sizeof(psync_offset),"%lld",
                     server.cached_master->reploff+1);
        } else {
            psync_replid = "?";
            memcpy(psync_offset,"-1",3);
        }

        serverLog(LL_NOTICE,
                 "Error reply to PSYNC from master: %s, retry without opid",
                 reply);
        return sendSynchronousCommand(SYNC_CMD_FULL, fd, "PSYNC", psync_replid,
                                       psync_offset, NULL);
    }

    return reply;
}

char *slaveHandleReplyForReplVersion(char *reply) {
    if (!strncmp(reply, "+REPL_VERSION", 13)) {
        char *repl_version = NULL;
        char *reply_after_repl_version = NULL;

        repl_version = strchr(reply,' ');
        if (repl_version) {
            repl_version++;
            server.repl_version = strtol(repl_version, NULL, 10);
            /* for compatibility with old version, not used */
            char *current_master_runid = NULL;
            if (server.repl_version >= 2 &&
                (current_master_runid = strchr(repl_version, ' '))) {
                current_master_runid++;
                reply_after_repl_version =
                    strchr(current_master_runid, ' ') + 1;
            } else {
                reply_after_repl_version = strchr(repl_version, ' ') + 1;
            }
        }
        sds tmp = sdsnew(reply_after_repl_version);
        sdsfree(reply);
        reply = tmp;
        return reply;
    }

    return reply;
}

void handleReplyForExtraFullResyncInfo(char *reply, char *offset) {
    char *offset_initial_opid = NULL;

    offset_initial_opid = strchr(offset, ' ');
    if (offset_initial_opid) {
        offset_initial_opid++;
        server.repl_master_initial_opid = strtoll(offset_initial_opid,NULL,10);
        serverLog(LL_NOTICE,"Full resync from master, opid: %lld",
                 server.repl_master_initial_opid);
        char *offset_applied_info = NULL;
        offset_applied_info = strchr(offset_initial_opid, ' ');
        if (offset_applied_info) {
            offset_applied_info++;
            size_t remain_size = strlen(reply) - (offset_applied_info - reply);
            strncpy(server.repl_master_initial_applied_info,
                    offset_applied_info, remain_size);
            server.repl_master_initial_applied_info[remain_size] = '\0';
        }
        char *master_aof_psyncing_state = NULL;
        master_aof_psyncing_state = strchr(offset_applied_info, ' ');
        if (master_aof_psyncing_state) {
            master_aof_psyncing_state++;
            server.repl_master_aof_psyncing_state =
                strtoll(master_aof_psyncing_state, NULL, 10);
        }
    }
}

void resetSlaveNextOpidAfterFullResync(void) {
    server.next_opid = (server.repl_master_initial_opid > 0 ?
                        server.repl_master_initial_opid : server.next_opid);
}

void setMasterAofPsyncingStateAfterFullResync(void) {
    if (server.repl_master_aof_psyncing_state && server.master) {
        server.master->flags |= REDIS_AOF_PSYNCING;
    }
}

void checkAndSaveNewReplId(char *reply, int psync_type_str_len,
                           char *old_replid) {
    /* psync_type_str_len
     * if len is 9:  "+CONTINUE"
     * if len is 12: "+AOFCONTINUE"
     */
    char *start = reply + psync_type_str_len + 1;
    char *end = reply + psync_type_str_len;
    while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
    if (end-start == CONFIG_RUN_ID_SIZE) {
        char new[CONFIG_RUN_ID_SIZE+1];
        memcpy(new,start,CONFIG_RUN_ID_SIZE);
        new[CONFIG_RUN_ID_SIZE] = '\0';

        if (strcmp(new,old_replid)) {
            /* Master ID changed. */
            serverLog(LL_WARNING,"Master replication ID changed to %s",new);

            /* Set the old ID as our ID2, up to the current offset+1. */
            memcpy(server.replid2,old_replid,
                   sizeof(server.replid2));
            server.second_replid_offset = server.master_repl_offset+1;

            /* Update the cached master ID and our own primary ID to the
             * new one. */
            memcpy(server.replid,new,sizeof(server.replid));
            if (server.cached_master) {
                memcpy(server.cached_master->replid,new,sizeof(server.replid));
            }

            /* Disconnect all the sub-slaves: they need to be notified. */
            disconnectSlaves();

            saveChangedReplInfoIntoConfig();
        }
    }
}

int masterReplyAofPsyncingStateOnFullResync(char *buf, int size, int buf_len) {
    int aof_psyncing_state = (slaveCheckAofPsyncingState() == C_OK ? 1 : 0);
    size += snprintf(buf+size, buf_len-size, " %d\r\n", aof_psyncing_state);

    return size;
}

int slaveCheckAofPsyncingState(void) {
    if (server.master && server.master->flags & REDIS_AOF_PSYNCING) {
        return C_OK;
    }

    return C_ERR;
}

int masterCheckAofPsyncingState(client *slave) {
    if (slave->flags & REDIS_AOF_PSYNCING) {
        return C_OK;
    }

    return C_ERR;
}

int handleReplyForAofPsync(char *reply, int fd) {
    UNUSED(fd);
    if (!strncmp(reply, "+AOFCONTINUE", 12)) {
        server.master = createClient(server.repl_transfer_s);
        server.master->flags |= CLIENT_MASTER;
        server.master->flags |= REDIS_AOF_PSYNCING;
        server.master->authenticated = 1;
        server.master->lastinteraction = server.unixtime;
        server.repl_state = REPL_STATE_CONNECTED;
        checkAndSaveNewReplId(reply, 12, server.replid);
        memcpy(server.master->replid, server.replid,
               sizeof(server.replid));

        serverLog(LL_NOTICE,"aof psync from master, replid: %s",
                 server.replid);

        /* clean reply and discard cached master if have */
        sdsfree(reply);
        replicationDiscardCachedMaster();

        disconnectSlaves();
        if (!server.repl_backlog) createReplicationBacklog();

        return C_OK;
    }

    return C_ERR;
}

int checkMasterSlaveSameServerId(client *c) {
    /* master and slave must have the same server id */
    long long server_id = -1;
    if (getLongLongFromObject(c->argv[4], &server_id) != C_OK) {
        serverLog(LL_WARNING,
                 "Parse server-id from psync cmd sent by slave %s "
                 "failed, closing connection with slave",
                 replicationGetSlaveName(c));
        freeClientAsync(c);
        return C_ERR;
    } else if (server_id != server.server_id) {
        serverLog(LL_WARNING,
                 "server-id(%lld) of slave %s is not the same with "
                 "master's(%lld), closing connection with slave",
                 server_id, replicationGetSlaveName(c),
                 server.server_id);
        freeClientAsync(c);
        return C_ERR;
    }

    return C_OK;
}

int masterReplyReplVersion(client *c) {
    char buf[1024] = {'\0'};
    int buflen = 0;
    long long slave_repl_version = 0;

    buflen = snprintf(buf, sizeof(buf), "+REPL_VERSION %d ",
                      REDIS_REPL_VERSION);
    if (c->argc > 5 &&
        getLongLongFromObject(c->argv[5],
                              &slave_repl_version) != C_OK) {
        serverLog(LL_WARNING,
                 "invalid repl version %s recieved from slave %s",
                 (char *)c->argv[5]->ptr, replicationGetSlaveName(c));
        freeClientAsync(c);
        return C_ERR;
    }
    if (slave_repl_version >= 2) {
        /* send current master runid to slave,
         * for compatibility */
        buflen += snprintf(buf+buflen, sizeof(buf)-buflen, "%s ",
                           "0000000000000000000000000000000000000000");
    }
    if (write(c->fd, buf, buflen) != buflen) {
        freeClientAsync(c);
        return C_ERR;
    }

    return C_OK;
}

void shiftReplIdAndSaveWhenRestart(void) {
    shiftReplicationId();
    server.second_replid_offset = -1;
    saveChangedReplInfoIntoConfig();
}

int masterTryAofPsyncCheck(client *c) {
    long long slave_next_opid = LLONG_MAX;
    char *master_replid = c->argv[1]->ptr;

    if (getLongLongFromObject(c->argv[3], &slave_next_opid) != C_OK)
        return C_ERR;

    if ((!strcasecmp(master_replid, server.replid) &&
         slave_next_opid <= server.next_opid) ||
        (!strcasecmp(master_replid, server.replid2) &&
         slave_next_opid <= server.second_replid_opid)) {
        return C_OK;
    }

    serverLog(LL_NOTICE, "master can't start an aof psync, master_replid:%s, "
              "slave_next_opid:%lld, server.replid:%s, server.next_opid:%lld, "
              "server.replid2:%s, server.second_replid_opid:%lld",
              master_replid, slave_next_opid, server.replid, server.next_opid,
              server.replid2, server.second_replid_opid);

    return C_ERR;
}

long long getMinimumOpidOfMaster(void) {
    char *abuffer = readAllContentsFromTextFile(AOF_INDEX_FILE);
    if(NULL == abuffer) {
        serverLog(LL_WARNING,
                 "read all contents from %s failed", AOF_INDEX_FILE);
        return C_ERR;
    }

    char *aof_item = strtok(abuffer, "\n");
    while(aof_item != NULL) {
        long long tmp_opid = getAofFirstOpid(aof_item);
        if (tmp_opid == C_ERR) {
            zfree(abuffer);
            return C_ERR;
        } else if (tmp_opid != REDIS_EMPTY_FILE) {
            zfree(abuffer);
            return tmp_opid;
        }
        /* for empty file, we skip it and check for next aof */
        aof_item = strtok(NULL, "\n");
    }

    zfree(abuffer);
    return C_ERR;
}

int masterTryAofPsync(client *c) {
    if (!server.aof_psync_state) return C_ERR;

    if(server.aof_state != AOF_ON) {
        serverLog(LL_WARNING,
                 "master aof is off, aof psync terminated, go full resync");
        return C_ERR;
    }

    if (c->argc < 4) {
        serverLog(LL_WARNING, "old version slave use PSYNC <runid> <offset> "
                 "without next opid, master aof psync terminated, "
                 "just go full resync");
        return C_ERR;
    }

    long long slave_next_opid;
    char buf[128];
    int buflen;

    if (getLongLongFromObject(c->argv[3], &slave_next_opid) != C_OK) {
        return C_ERR;
    }
    serverLog(LL_NOTICE,
             "master try aof psync, recieved slave next opid: %lld",
             slave_next_opid);

    if (slave_next_opid <= REDIS_MIN_SLAVE_OPID_TO_AOF_PSYNC) {
        serverLog(LL_NOTICE,
                 "slave next opid is too small, just go full resync");
        return C_ERR;
    }

    long long master_min_opid = getMinimumOpidOfMaster();
    if (master_min_opid == C_ERR) {
        serverLog(LL_WARNING, "get minimum opid of master error, "
                 "master aof psync terminated");
        return C_ERR;
    }
    serverLog(LL_NOTICE,
             "master try aof psync, master minimum opid: %lld, "
             "slave next opid: %lld, master next opid: %lld",
             master_min_opid, slave_next_opid, server.next_opid);
    if (slave_next_opid < master_min_opid ||
        slave_next_opid > server.next_opid) {
        serverLog(LL_WARNING,
                 "invalid slave next opid, master aof psync terminated");
        return C_ERR;
    }

    c->flags |= CLIENT_SLAVE;
    if (slave_next_opid != server.next_opid) {
        /* sending aof, master won't replicate cmds to slave */
        c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
        c->flags |= REDIS_AOF_PSYNCING;
    } else {
        c->replstate = SLAVE_STATE_ONLINE;
    }
    c->repl_ack_time = server.unixtime;
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.slaves,c);
    buflen = snprintf(buf, (size_t)sizeof(buf), "+AOFCONTINUE %s\r\n",
                      server.replid);
    if (write(c->fd,buf,buflen) != buflen) {
        freeClientAsync(c);
        c->flags &= ~REDIS_AOF_PSYNCING;
        return C_ERR;
    }

    /* if slave next opid equals to master's,
     * we don't need to do sending aof to slave stuff */
    if (slave_next_opid != server.next_opid) {
        /* create background job to send aof to slave */
        bioCreateBackgroundJob(BIO_FIND_OFFSET_BY_OPID, c,
                               (void *)(long)slave_next_opid,
                               (void *)(long)c->id);
    }

    serverLog(LL_NOTICE,
             "master start aof partial resynchronization requested by %s",
             replicationGetSlaveName(c));

    /* when slave sync with master using aof, there may be no repl backlog
     * in this situation, we should create one, otherwise replicationFeedSlaves
     * will fail and kernel panic */
    if (!server.masterhost && listLength(server.slaves) == 1
        && server.repl_backlog == NULL) {
        createReplicationBacklog();
    }

    if (slave_next_opid == server.next_opid) {
        serverLog(LL_NOTICE,
                 "slave(%s) already have the same opid with master,"
                 " aof psync success", replicationGetSlaveName(c));
        if (!(server.master && server.master->flags & REDIS_AOF_PSYNCING)) {
            sendSyncReplOffsetCommandToSlave(c);
            c->flags &= ~REDIS_AOF_PSYNCING;
        }
    }

    refreshGoodSlavesCount();
    return C_OK;
}

void forceFullResyncCommand(client *c) {
    if (!(c->flags & CLIENT_MASTER)) {
        addReplyError(c, "forceFullResync command can only be "
                      "called in repl connection");
        return;
    }
    /* set next_opid and master reploff to an invalid value
     * to force full resync */
    server.next_opid = LLONG_MIN;
    if (server.master) {
        server.master->flags |= REDIS_INVALID_REPLOFF;
    }
    serverLog(LL_NOTICE, "force full resync requested by master aof psync");
    addReply(c, shared.ok);
}

size_t getSyncreploffCmdLen(long long offset) {
    sds buf = genSyncreploffsetCmdBuf(offset);
    size_t len = sdslen(buf);
    sdsfree(buf);
    return len;
}

size_t getAppliedBytesSize(client *c, uint64_t prev_flags,
                            long long prev_offset) {
    if (prev_flags & REDIS_AOF_PSYNCING &&
        !(c->flags & REDIS_AOF_PSYNCING) &&
        c->reploff_before_syncreploff >= 0) {
        size_t syncreploff_cmd_len =
            getSyncreploffCmdLen(server.master_repl_offset);
        size_t bytes_not_include_syncreploff =
            c->reploff_before_syncreploff - prev_offset;
        size_t bytes_include_syncreploff =
            bytes_not_include_syncreploff + syncreploff_cmd_len;
        size_t remain_size = sdslen(c->pending_querybuf) - bytes_include_syncreploff;
        if (remain_size > 0) {
            /* no overlap */
            memcpy(c->pending_querybuf+bytes_not_include_syncreploff,
                   c->pending_querybuf+bytes_include_syncreploff,
                   remain_size);

        }
        sdssetlen(c->pending_querybuf,
                  sdslen(c->pending_querybuf)-syncreploff_cmd_len);
        return (c->reploff - prev_offset - syncreploff_cmd_len);
    } else {
        return (c->reploff - prev_offset);
    }
}

void aofPsyncReplOffPostSet(client *c, uint64_t prev_flags,
                            size_t applied) {
    if (!(c->flags & CLIENT_MASTER)) return;

    /* invalid reploff to actively require a fullresync */
    if (c->flags & REDIS_INVALID_REPLOFF) {
        c->reploff = c->read_reploff = LLONG_MAX;
        c->flags &= (~REDIS_INVALID_REPLOFF);
        freeClientAsync(c);
        return;
    }

    /* freeze the reploff when we are aof psyncing with our master */
    if (c->flags & REDIS_AOF_PSYNCING) {
        c->read_reploff -= c->reploff;
        c->reploff = 0;
        return;
    }

    /* sync reploff with our master */
    if (prev_flags & REDIS_AOF_PSYNCING &&
        !(c->flags & REDIS_AOF_PSYNCING) &&
        c->reploff_before_syncreploff >= 0) {
        /* compute new reploff */
        long long unprocessed_bytes = c->read_reploff - c->reploff;
        server.master_repl_offset -= applied; /* resume */
        size_t syncreploff_cmd_len =
            getSyncreploffCmdLen(server.master_repl_offset);
        c->read_reploff = server.master_repl_offset +
            (c->read_reploff - c->reploff_before_syncreploff -
             syncreploff_cmd_len);
        server.master_repl_offset = c->reploff =
            c->read_reploff - unprocessed_bytes;
        c->reploff_before_syncreploff = -1;

        /* reset repl backlog */
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        server.repl_backlog_off = server.master_repl_offset+1;

        return;
    }
}

void syncReplOffsetCommand(client *c) {
    if (!(c->flags & CLIENT_MASTER)) {
        addReplyError(c, "syncReplOffset command can only be "
                      "called in repl connection");
        return;
    }

    serverLog(LL_NOTICE, "sync repl offset command received from master");

    if (server.master) {
        long long tmp;
        if (getLongLongFromObject(c->argv[1], &tmp) != C_OK) {
            c->flags |= REDIS_INVALID_REPLOFF;
            return;
        }
        serverLog(LL_NOTICE, "sync repl offset with master: %lld", tmp);

        server.master_repl_offset = tmp;
        c->reploff_before_syncreploff = c->reploff;

        /* clear master aof psyncing state when aof psync is finished */
        if (server.master) server.master->flags &= (~REDIS_AOF_PSYNCING);
    }
    addReply(c, shared.ok);
}

sds genSyncreploffsetCmdBuf(long long repl_offset) {
    char repl_offset_buf[64] = {'\0'};
    sds buf = NULL;
    snprintf(repl_offset_buf, 64, "%lld", repl_offset);
    buf = sdscatprintf(sdsempty(),
                       SYNCREPLOFFSET_CMD_BUF,
                       strlen(repl_offset_buf), repl_offset_buf);

    return buf;
}

void sendSyncReplOffsetCommandToSlave(client *slave) {
    sds buf = genSyncreploffsetCmdBuf(server.master_repl_offset);
    addReplySds(slave, buf);
}

void doSendAofToSlave(aeEventLoop *el, int fd, void *privdata, int mask) {
    client *slave = privdata;
    int aof_psync_reading_fd = server.aof_psync_slave_offset->fd;
    char *aof_psync_reading_filename = server.aof_psync_slave_offset->aof_filename;
    UNUSED(el);
    UNUSED(mask);

    /* Align IOBUF to multiple of 1024 */
    const int IOBUF_LEN = PROTO_IOBUF_LEN;
    char buf[IOBUF_LEN];
    ssize_t nwritten, buflen;

    lseek(aof_psync_reading_fd, slave->repldboff, SEEK_SET);
    buflen = read(aof_psync_reading_fd, buf, IOBUF_LEN);
    if (buflen < 0) {
        serverLog(LL_WARNING,"Read error sending aof %s to slave(%s): %s",
                 aof_psync_reading_filename,
                 replicationGetSlaveName(slave),
                 strerror(errno));
        goto dosenderr;
    } else if (buflen == 0) { /* End of file */
        if (strcmp(server.aof_filename, aof_psync_reading_filename)) {
            serverLog(LL_NOTICE, "%s read done, finding next aof",
                     aof_psync_reading_filename);

            /* set next aof to read */
            if (aofReadingSetNextAof(server.aof_psync_slave_offset,
                                     REDIS_AOFPSYNC_AOF_READ) != C_OK) {
                goto dosenderr;
            }

            /* init first reading aof offset */
            slave->repldboff = server.aof_psync_slave_offset->offset;
        } else {
            serverLog(LL_NOTICE, "final aof %s read done",
                     aof_psync_reading_filename);
            /* if we are reading current aof, we need to send aof_buf
             * to slave and aof psync done */
            aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);
            putSlaveOnline(slave);
            if (listSearchKey(server.slaves, slave) == NULL) return;

            /* add aof buf to slave output buffer */
            addReplySds(slave,sdsdup(server.aof_buf));

            /* force slave to sync offset with master
             * we must sync repl offset after we have sent aof buffer to slave
             * because when commands is writted to aof buffer,
             * master_repl_offset is incremented */
            if (!(server.master && server.master->flags & REDIS_AOF_PSYNCING)) {
                sendSyncReplOffsetCommandToSlave(slave);
                slave->flags &= ~REDIS_AOF_PSYNCING;
            }

            /* clean aof psync state */
            resetAofPsyncState();
        }
        return;
    }

    if ((nwritten = write(fd,buf,buflen)) == -1) {
        if (errno != EAGAIN && errno != EINTR) {
            serverLog(LL_WARNING,"Write error sending aof %s to slave(%s): %s",
                     aof_psync_reading_filename,
                     replicationGetSlaveName(slave),
                     strerror(errno));
            goto dosenderr;
        }
        return;
    }

    slave->repldboff += nwritten;
    server.stat_net_output_bytes += nwritten;
    return;

dosenderr:
    /* send command to force slave do full resync */
    syncWrite(slave->fd, AOF_PSYNC_FORCE_FULL_RESYNC_CMD,
              AOF_PSYNC_FORCE_FULL_RESYNC_CMD_LEN, AOF_PSYNC_SYNCWRITE_TIMEOUT);
    freeClient(slave);
}

int aofReadingSetNextAof(bioFindOffsetRes *res, int type) {
    listIter li;
    listNode *ln;
    list *aof_list = readAofIndexIntoList();
    int tmp_fd;
    FILE *tmp_fp;

    if (!aof_list) goto err;

    /* find next aof by aof index file */
    listRewind(aof_list, &li);
    while((ln = listNext(&li))) {
        if (!strcmp((char *)ln->value, res->aof_filename)) {
            break;
        }
    }
    if (!ln) {
        serverLog(LL_WARNING, "Cannot find currently reading aof %s in %s",
                 res->aof_filename, AOF_INDEX_FILE);
        goto err;
    }
    if (ln->next == NULL) {
        serverLog(LL_WARNING,
                 "%s is the last aof in %s, but not the server currently "
                 "writing aof(%s), which is weird",
                 res->aof_filename, AOF_INDEX_FILE, server.aof_filename);
        goto err;
    }

    char *tmp_filename = (char *)ln->next->value;
    if (type == REDIS_AOFPSYNC_AOF_READ) {
        tmp_fd = open(tmp_filename, O_RDONLY);
        if (tmp_fd < 0) {
            serverLog(LL_WARNING, "open aof %s for slave(%s) failed",
                     tmp_filename,
                     replicationGetSlaveName(res->c));
            goto err;
        }
        /* init next reading aof offset, fd/fp */
        res->offset = 0;
        strncpy(res->aof_filename, tmp_filename, REDIS_AOF_FILENAME_LEN);
        server.aof_psync_cur_reading_name = res->aof_filename;
        close(res->fd);
        res->fd = tmp_fd;
    } else if (type == REDIS_OPGET_AOF_READ) {
        tmp_fp = fopen(tmp_filename, "r");
        if (tmp_fp == NULL) {
            serverLog(LL_WARNING, "open aof %s for client(%s) failed",
                     tmp_filename,
                     getClientPeerId(res->c));
            goto err;
        }
        /* init next reading aof offset, fd/fp */
        res->offset = 0;
        strncpy(res->aof_filename, tmp_filename, REDIS_AOF_FILENAME_LEN);
        fclose(res->fp);
        res->fp = tmp_fp;
    }

    if (aof_list) listRelease(aof_list);

    return C_OK;

err:
    if (aof_list) listRelease(aof_list);
    return C_ERR;
}

/* bio thread produce a result, and main thead consume one */
void bioFindOffsetByOpid(client *c, long long opid, uint64_t client_id) {
    int error = 0;
    bioFindOffsetRes *offset_result = NULL;
    const char *last_aof_filename = NULL;
    long long found_offset = -1, last_valid_offset = 0;
    sds *argv = NULL;
    char *aof_index_buffer = NULL;
    char *aof_item = NULL;
    FILE *fp = NULL;
    int argc = 0;

    /* produce one item at a time, after that we just sleep */
    atomicGet(server.bio_find_offset_res, offset_result);
    while (offset_result) {
        sleep(1);
        atomicGet(server.bio_find_offset_res, offset_result);
    }

    offset_result = (bioFindOffsetRes *)zmalloc(sizeof(bioFindOffsetRes));
    offset_result->fd = -1;
    offset_result->fp = NULL;
    offset_result->error = 0;
    offset_result->c = NULL;
    offset_result->client_id = 0;
    offset_result->aof_filename[0] = '\0';

    aof_index_buffer = readAllContentsFromTextFile(AOF_INDEX_FILE);
    if (aof_index_buffer == NULL) {
        error = 1;
        goto finddone;
    }

    aof_item = strtok(aof_index_buffer, "\n");
    while (aof_item != NULL) {
        long long tmp_opid = getAofFirstOpid(aof_item);
        if (tmp_opid == C_ERR) {
            error = 1;
            goto finddone;
        } else if (tmp_opid != REDIS_EMPTY_FILE) {
            if (tmp_opid < opid) {
                last_aof_filename = aof_item;
            } else if (tmp_opid == opid) {
                found_offset = 0;
                strncpy(offset_result->aof_filename, aof_item,
                        REDIS_AOF_FILENAME_LEN);
                goto finddone;
            } else {
                break;
            }
        }
        /* for empty file, we skip it and check for next aof */
        aof_item = strtok(NULL, "\n");
    }

    if (last_aof_filename == NULL) {
        error = 1;
        goto finddone;
    } else {
        strncpy(offset_result->aof_filename, last_aof_filename,
                REDIS_AOF_FILENAME_LEN);
    }

    /* valid aof offset for slave_next_opid might exist
     * we analyze aof and check for it
     */
    fp = fopen(offset_result->aof_filename,"r");
    if (fp == NULL) {
        serverLog(LL_WARNING, "bioFindOffsetByOpid: Cannot open file: %s, %s",
                 offset_result->aof_filename, strerror(errno));
        error = 1;
        goto finddone;
    }

    while(1) {
        int j;
        /* if the command is not opinfo command,
         * we can omit the value of the args and just fseek */
        int can_fseek;
        unsigned long len;
        char buf[128];
        sds argsds;
        argc = j = can_fseek = 0;

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            if (feof(fp))
                break;
            else {
                error = 1;
                serverLog(LL_WARNING, "bioFindOffsetByOpid: aof %s read error",
                         offset_result->aof_filename);
                goto finddone;
            }
        }
        if (buf[0] != '*') {
            error = 1;
            serverLog(LL_WARNING, "bioFindOffsetByOpid: aof %s file format error",
                     offset_result->aof_filename);
            goto finddone;
        }
        if (buf[1] == '\0') {
            error = 1;
            serverLog(LL_WARNING, "bioFindOffsetByOpid: aof %s read error",
                     offset_result->aof_filename);
            goto finddone;
        }
        argc = atoi(buf+1);
        if (argc < 1) {
            error = 1;
            serverLog(LL_WARNING, "bioFindOffsetByOpid: aof %s file format error",
                     offset_result->aof_filename);
            goto finddone;
        }

        argv = zmalloc(sizeof(sds)*argc);
        bzero(argv, sizeof(sds)*argc);

        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                error = 1;
                serverLog(LL_WARNING, "bioFindOffsetByOpid: aof %s read error",
                         offset_result->aof_filename);
                goto finddone;
            }
            if (buf[0] != '$') {
                error = 1;
                serverLog(LL_WARNING, "bioFindOffsetByOpid: aof %s file format error",
                         offset_result->aof_filename);
                goto finddone;
            }
            len = strtol(buf+1,NULL,10);
            if (can_fseek) {
                argsds = NULL;
            } else {
                argsds = sdsnewlen(NULL,len);
            }
            if (len && ((can_fseek && fseek(fp, len, SEEK_CUR) == -1) ||
                        (!can_fseek && fread(argsds,len,1,fp) != 1))) {
                sdsfree(argsds);
                error = 1;
                serverLog(LL_WARNING, "bioFindOffsetByOpid: aof %s read error",
                         offset_result->aof_filename);
                goto finddone;
            }
            argv[j] = argsds;
            if (j == 0 && strcasecmp("opinfo", argv[j])) can_fseek = 1;
            if ((can_fseek && fseek(fp, 2, SEEK_CUR) == -1) ||
                (!can_fseek && fread(buf,2,1,fp) != 1)) {
                error = 1;
                serverLog(LL_WARNING, "bioFindOffsetByOpid: aof %s read error",
                         offset_result->aof_filename);
                goto finddone;
            }
        }

        if (!strcasecmp("opinfo", argv[0])) {
            redisOplogHeader *loaded_header = (redisOplogHeader *)argv[1];
            if (loaded_header->opid == opid) {
                found_offset = last_valid_offset;
                serverLog(LL_NOTICE, "offset found for opid(%lld): %s:%lld",
                         opid, offset_result->aof_filename, last_valid_offset);
                goto finddone;
            }
        }
        for(int i = 0; i < argc; i++)
            sdsfree(argv[i]);
        zfree(argv);
        argv = NULL;
        last_valid_offset = ftello(fp);
    }

    if (found_offset == -1) {
        error = 1;
        serverLog(LL_WARNING, "Cannot find opid %lld in aof %s",
                 opid, offset_result->aof_filename);
    }

finddone:
    if (fp) {
        fclose(fp);
    }
    for(int i = 0; i < argc; i++) {
        sdsfree(argv[i]);
    }
    zfree(argv);
    zfree(aof_index_buffer);
    offset_result->c = c;
    offset_result->client_id = client_id;
    offset_result->error = error;
    offset_result->offset = found_offset;
    /* result produced, consumed in serverCron() using handleBioFindOffsetRes()
     * single cpu instruction, lock free */
    server.bio_find_offset_res = offset_result;
}

void handleBioFindOffsetRes() {
    if (listLength(server.bio_find_offset_results) == 0) {
        return;
    }

    listIter li;
    listNode *ln;
    listRewind(server.bio_find_offset_results, &li);
    while((ln = listNext(&li))) {
        bioFindOffsetRes *tmp_res = (bioFindOffsetRes *)ln->value;
        client *tmp_client = tmp_res->c;
        if (tmp_client == NULL ||
            listSearchKey(server.clients, tmp_client) == NULL ||
            tmp_client->id != tmp_res->client_id) {
            listDelNode(server.bio_find_offset_results, ln);
            continue;
        }

        if (tmp_client->flags & CLIENT_SLAVE){
            /* it's a redis slave client waiting for aof psync */
            if (server.do_aof_psync_send) {
                server.aof_psync_slave_offset = tmp_res;
                masterSendAofToSlave();
                listDelNode(server.bio_find_offset_results, ln);
            }
        } else if (tmp_client->flags & REDIS_OPGET_CLIENT) {
            /* admin client using opget cmd to retrieve updates from Redis */
            tmp_client->opget_client_state->wait_bio_res = 0;
            if (tmp_res->error) { /* find offset by opid error */
                addReplyErrorFormat(
                    tmp_client,
                    "opget find aof offset by opid for client(%s) failed",
                    getClientPeerId(tmp_client));
                freeClientAsync(tmp_client);
            } else {
                /* save bioFindOffsetRes pointor in client structure */
                tmp_client->opget_client_state->opget_aof_state = tmp_res;

                /* prepare reading fp */
                tmp_res->fp = fopen(tmp_res->aof_filename, "r");
                if (tmp_res->fp == NULL) {
                    serverLog(LL_WARNING,
                             "open aof %s for opget client(%s) failed, %s",
                             tmp_res->aof_filename, getClientPeerId(tmp_client),
                             strerror(errno));
                    addReplyErrorFormat(
                        tmp_client,
                        "opget open aof %s for opget client(%s) failed",
                        tmp_res->aof_filename, getClientPeerId(tmp_client));
                    freeClientAsync(tmp_client);
                } else {
                    /* seek to the found offset */
                    fseek(tmp_res->fp, tmp_res->offset, SEEK_SET);
                    server.current_client = tmp_client;
                    sendOplogToClient(tmp_client); /* send n oplogs to client */
                    /* Process remaining data in the input buffer. */
                    if (tmp_client->querybuf && sdslen(tmp_client->querybuf) > 0) {
                        processInputBuffer(tmp_client);
                    }
                    server.current_client = NULL;
                }
            }

            listDelNode(server.bio_find_offset_results, ln);
        } else {
            /* unknown client not supposed to use this bio thread result */
            serverLog(LL_WARNING,
                     "client %s not supposed to use bio find offset thread",
                     getClientPeerId(tmp_client));
            zfree(tmp_res);
            listDelNode(server.bio_find_offset_results, ln);
        }
    }
}

void handleBioFindOffsetResCron(void) {
    /* master catch result generated by bio thread */
    bioFindOffsetRes *offset_result = NULL;
    atomicGet(server.bio_find_offset_res, offset_result);
    if(offset_result) {
        /* consume bio find offset result and save it to a list */
        listAddNodeTail(server.bio_find_offset_results, offset_result);
        offset_result = NULL;
        atomicSet(server.bio_find_offset_res, offset_result);
    }
    handleBioFindOffsetRes();
}

client *listSearchClient(list *clients, uint64_t client_id) {
    listIter iter;
    listNode *node;

    listRewind(clients, &iter);
    while((node = listNext(&iter)) != NULL) {
        client *client = listNodeValue(node);
        if (client->id == client_id)
            return client;
    }
    return NULL;
}

void masterSendAofToSlave() {
    server.do_aof_psync_send = 0;
    client *slave = server.aof_psync_slave_offset->c;

    if (slave == NULL ||
        listSearchClient(server.slaves,
                         server.aof_psync_slave_offset->client_id) == NULL) {
        serverLog(LL_WARNING,
                 "slave is freed, no need to send aof, aof psync terminated");
        goto senderr;
    } else if (slave->replstate == SLAVE_STATE_ONLINE) {
        serverLog(LL_NOTICE,
                 "slave(%s) is already online, no need to send aof, "
                 "aof psync terminated",
                 replicationGetSlaveName(slave));
        if (!(server.master && server.master->flags & REDIS_AOF_PSYNCING)) {
            sendSyncReplOffsetCommandToSlave(slave);
            slave->flags &= ~REDIS_AOF_PSYNCING;
        }
        goto senderr;
    } else if (server.aof_psync_slave_offset->error == 1) {
        serverLog(LL_WARNING,
                 "find offset by opid for slave(%s) failed, "
                 "force slave to do FULL RESYNC, aof psync terminated",
                 replicationGetSlaveName(slave));
        goto forcefullresync;
    }

    server.aof_psync_cur_reading_name =
        server.aof_psync_slave_offset->aof_filename;
    server.aof_psync_slave_offset->fd =
        open(server.aof_psync_slave_offset->aof_filename, O_RDONLY);
    if (server.aof_psync_slave_offset->fd < 0) {
        serverLog(LL_WARNING,
                 "open aof %s for slave(%s) failed, "
                 "force slave to do FULL RESYNC, aof psync terminated",
                 server.aof_psync_slave_offset->aof_filename,
                 replicationGetSlaveName(slave));
        goto forcefullresync;
    }

    /* init first reading aof offset */
    slave->repldboff = server.aof_psync_slave_offset->offset;

    aeDeleteFileEvent(server.el, slave->fd, AE_WRITABLE);
    if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE,
                          doSendAofToSlave, slave) == AE_ERR) {
        serverLog(LL_WARNING,
                 "Create file event to send aof for slave(%s) failed, "
                 "force slave to do FULL RESYNC, aof psync terminated",
                 replicationGetSlaveName(slave));
        goto forcefullresync;
    }
    return;

forcefullresync:
    /* send command to force slave do full resync */
    syncWrite(slave->fd, AOF_PSYNC_FORCE_FULL_RESYNC_CMD,
              AOF_PSYNC_FORCE_FULL_RESYNC_CMD_LEN, AOF_PSYNC_SYNCWRITE_TIMEOUT);
    freeClient(slave);
    return;

senderr:
    resetAofPsyncState();
}

int ignoreNewLineOnPsyncing(client *slave) {
    if (server.aof_psync_slave_offset &&
        server.aof_psync_slave_offset->c == slave) {
        return C_OK;
    }

    return C_ERR;
}

void updateSlaveSyncedOpid(client *c, int arg_idx) {
    long long opid;
    if ((getLongLongFromObject(c->argv[arg_idx+1], &opid) != C_OK))
        return;
    if (opid > c->repl_ack_next_opid)
        c->repl_ack_next_opid = opid;
}

/************************* aof binlog stream(On BLS) *************************/
void updateSrcServerIdOpid(long long src_server_id, long long applied_opid) {
    if (src_server_id < 0 || applied_opid < 1) return;

    robj key;
    key.encoding = OBJ_ENCODING_INT;
    key.type = OBJ_STRING;
    key.ptr = (void *)(src_server_id);
    dictEntry *dentry = NULL;

    if ((dentry = dictFind(server.src_serverid_applied_opid_dict, &key))) {
        robj *tmp = dictGetVal(dentry);
        tmp->ptr = (void *)(applied_opid);
    } else {
        robj *o = createObject(OBJ_STRING, NULL);
        o->encoding = OBJ_ENCODING_INT;
        o->ptr = (void*)((long)applied_opid);
        dictAdd(server.src_serverid_applied_opid_dict,
                createStringObjectFromLongLong(src_server_id), o);
    }
}

void updateAppliedInfoOnStartup(char *buf) {
    if (!buf || *buf == '\n') return;
    char *src_serverid = strtok(buf, " ");
    char *applied_opid = strtok(NULL, " ");
    while (src_serverid && applied_opid) {
        long long tmp_serverid = strtoll(src_serverid, NULL, 10);
        long long tmp_opid = strtoll(applied_opid, NULL, 10);
        updateSrcServerIdOpid(tmp_serverid, tmp_opid);
        src_serverid = strtok(NULL, " ");
        applied_opid = strtok(NULL, " ");
    }
}

long long dictGetAppliedOpidBySrcServerId(long long src_server_id) {
    robj key;
    key.encoding = OBJ_ENCODING_INT;
    key.type = OBJ_STRING;
    key.ptr = (void *)(src_server_id);
    dictEntry *dentry = NULL;

    if ((dentry = dictFind(server.src_serverid_applied_opid_dict, &key))) {
        robj *tmp = dictGetVal(dentry);
        return (long)(tmp->ptr);
    }

    return -1;
}

int checkOpapplyCmdIgnored(client *c) {
    if (c->cmd->proc != opinfoCommand && c->flags & REDIS_OPAPPLY_IGNORE_CMDS) {
        flagTransaction(c);
        addReply(c, shared.ignored);
        return C_OK;
    }

    return C_ERR;
}

void cleanOpGetClientState(opGetClientState *state, client *c) {
    if (state) {
        zfree(state->matchdbs);
        zfree(state->matchids);
        if(state->matchkeys) listRelease(state->matchkeys);

        if (state->opget_aof_state) {
            serverAssert(state->opget_aof_state->c == c);
            if (state->opget_aof_state->fp) {
                fclose(state->opget_aof_state->fp);
            }
            zfree(state->opget_aof_state);
        }

        /* delete node in opget client list */
        listNode *ln = listSearchKey(server.opget_client_list, c);
        serverAssert(ln != NULL);
        listDelNode(server.opget_client_list, ln);

        zfree(state);
        c->opget_client_state = NULL;
    }
}

int getLongLongFromCString(char *o, long long *target) {
    long long value;
    char *eptr;

    if (o == NULL) {
        return C_ERR;
    } else {
        errno = 0;
        value = strtoll(o, &eptr, 10);
        if (isspace(o[0]) || eptr[0] != '\0' || errno == ERANGE)
            return C_ERR;
    }
    if (target) *target = value;
    return C_OK;
}

void slaveUpdateAppliedInfoOnFullResync(char *applied_info) {
    /* master reply format like:
     * +FULLRESYNC runid offset opid
     * applied_info{serverid1:opid1,serverid2:opid2,serverid3:opid3} */
    if (!applied_info || *applied_info == '\0' ||
        strncmp(applied_info, "applied_info{", 13)) return;

    char *kv_str = applied_info + 13;
    char *end = NULL;
    if (*kv_str == '\0' || !(end = strchr(kv_str, '}'))) return;
    *end = '\0';

    char *kv = strtok(kv_str, ",");
    while(kv != NULL) {
        char *kv_delim = strchr(kv, ':');
        if (!kv_delim) return;
        *kv_delim = '\0';
        long long server_id, opid;
        if (getLongLongFromCString(kv, &server_id) != C_OK ||
            getLongLongFromCString(kv_delim + 1, &opid) != C_OK) return;
        updateSrcServerIdOpid(server_id, opid);
        kv = strtok(NULL, ",");
    }
}

int masterReplyAppliedInfoOnFullResync(char *buf, int size, int buf_len) {
    /* add reply <src_serverid, applied_opid> */
    size += snprintf(buf+size, buf_len-size, " applied_info{");
    dictIterator *di = dictGetIterator(server.src_serverid_applied_opid_dict);
    dictEntry *de = NULL;
    while ((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        robj *val = dictGetVal(de);
        size += snprintf(buf+size, buf_len-size, "%ld:%ld,", (long)key->ptr,
                         (long)val->ptr);
    }
    dictReleaseIterator(di);
    /* trim the last ',' */
    if (dictSize(server.src_serverid_applied_opid_dict)) size--;
    size += snprintf(buf+size, buf_len-size, "}");
    return size;
}

void initExtraClientState(client *c) {
    c->opget_client_state = NULL;
    c->repl_ack_next_opid = -1;
    c->opapply_src_opid = -1;
    c->opapply_src_serverid = -1;
    c->reploff_before_syncreploff = -1;
}

long long getSlaveCurOpid(client *slave) {
    return slave->repl_ack_next_opid;
}

#define REDIS_READ_OPLOG_FORMAT_ERROR             -2
#define REDIS_READ_OPLOG_READ_ERROR               -1
#define REDIS_READ_OPLOG_EOF                       0
#define REDIS_READ_OPLOG_UNMATCH                   1
#define REDIS_READ_OPLOG_SUCC                      2
#define REDIS_READ_OPLOG_IGNORE                    3
static int read_oplog_errno = REDIS_READ_OPLOG_SUCC;

/* Read one redis command from aof
 * On success, return the end offset of the command
 * else, return -1 */
long readOneCommand(FILE *fp,
                    int *dbid,
                    long long *serverid,
                    sds *key,
                    int *oplog_cmd_count,
                    redisOplogCmdType *cmd_type,
                    long long *opid) {
    int j, argc;
    int can_fseek; /* if the command is not opinfo command,
                    * we can omit the value of command args
                    * except the key argument and just fseek */
    unsigned long len;
    char buf[128];
    sds argsds;
    sds argv[2] = {NULL};
    argc = j = can_fseek = 0;
    *cmd_type = REDIS_OPLOG_OTHER_CMD;

    if (fgets(buf,sizeof(buf),fp) == NULL) {
        if (feof(fp)) {
            read_oplog_errno = REDIS_READ_OPLOG_EOF;
            return ftell(fp);
        }
        else {
            goto readerr;
        }
    }
    if (buf[0] != '*') goto fmterr;
    if (buf[1] == '\0') goto readerr;
    argc = atoi(buf+1);
    if (argc < 1) goto fmterr;

    for (j = 0; j < argc; j++) {
        /* only command name and argv[1] is needed */
        if (!can_fseek && j > 1) can_fseek = 1;
        if (fgets(buf,sizeof(buf),fp) == NULL) {
            goto readerr;
        }
        if (buf[0] != '$') {
            goto fmterr;
        }
        len = strtol(buf+1,NULL,10);
        if (can_fseek) {
            argsds = NULL;
        } else {
            argsds = sdsnewlen(NULL,len);
        }
        if (len && ((can_fseek && fseek(fp, len, SEEK_CUR) == -1)
                    || (!can_fseek && fread(argsds,len,1,fp) != 1))) {
            sdsfree(argsds);
            goto readerr;
        }
        if(j <= 1) argv[j] = argsds;
        if (fseek(fp, 2, SEEK_CUR) == -1) { /* fseek \r\n */
            goto readerr;
        }
    }

    /* get dbid */
    if (!strcasecmp("opinfo", argv[0])) {
        *cmd_type = REDIS_OPLOG_OPINFO_CMD;
        redisOplogHeader *loaded_header = (redisOplogHeader *)argv[1];
        *dbid = loaded_header->dbid;
        *serverid = loaded_header->server_id;
        *oplog_cmd_count = loaded_header->cmd_num;
        *opid = loaded_header->opid;
        sdsfree(argv[1]);
    } else if (!strcasecmp("multi", argv[0])) {
        /* for mult/exec, shouldn't filter it by dbid, always
         * send it to client */
        *dbid = -1;
        *cmd_type = REDIS_OPLOG_MULTI_CMD;
    } else if (!strcasecmp("exec",  argv[0])) {
        *dbid = -1;
        *cmd_type = REDIS_OPLOG_EXEC_CMD;
    } else {
        if (*key == NULL) {
            *key = argv[1]; /* get key */
        } else {
            sdsfree(argv[1]);
        }
    }

    sdsfree(argv[0]);
    return ftell(fp);

readerr:
    read_oplog_errno = REDIS_READ_OPLOG_READ_ERROR;
    return -1;

fmterr:
    read_oplog_errno = REDIS_READ_OPLOG_FORMAT_ERROR;
    return -1;
}

uint8_t isDbMatch(opGetClientState *state, int dbid) {
    if (!state->use_matchdb || !intsetLen(state->matchdbs) || dbid == -1)
        return 1;

    return intsetFind(state->matchdbs, dbid);
}

uint8_t isKeyMatch(opGetClientState *state, sds key) {
    if (!state->use_matchkey || !listLength(state->matchkeys) || !key)
        return 1;

    listIter li;
    listNode *ln;
    listRewind(state->matchkeys, &li);

    while ((ln = listNext(&li))) {
        robj *tmp = (robj *)ln->value;
        if (stringmatchlen(tmp->ptr,
                           sdslen(tmp->ptr), key, sdslen(key), 0)) {
            return 1;
        }
    }

    return 0;
}

uint8_t isIdMatch(opGetClientState *state, long long serverid) {
    if (!state->use_matchid || !intsetLen(state->matchids) || serverid == -1)
        return 1;

    return intsetFind(state->matchids, serverid);
}

sds readOneOplog(opGetClientState *state, long long *opid) {
    FILE *fp = state->opget_aof_state->fp;
    long aof_initial_offset = ftell(fp);
    long long aof_initial_opid = *opid;
    long aof_last_offset = aof_initial_offset;
    sds oplog = NULL;
    long long oplog_len = 0;
    int oplog_cmd_count = 2; /* number of commands in one oplog */
    int i;
    sds key = NULL;
    int dbid = -1;
    long long serverid = -1;
    long long start, duration;
    int oplog_with_transaction = 0;

    /* for setex/psetex command, there will be three commands in one oplog:
     * opinfo
     * w_command
     * pexpireat
     *
     * otherwise, there will ben two commands in one oplog:
     * opinfo
     * w_command */
    for (i = 0; i < oplog_cmd_count; i++) {
        redisOplogCmdType cmd_type = REDIS_OPLOG_OTHER_CMD;
        start = ustime();
        long aof_cur_offset = readOneCommand(fp, &dbid, &serverid, &key,
                                             &oplog_cmd_count, &cmd_type,
                                             opid);
        if (oplog_cmd_count == 0) {
            /* read until exec command is encountered */
            oplog_cmd_count = INT_MAX;
            oplog_with_transaction = 1;
        }

        duration = ustime() - start;
        latencyAddSampleIfNeeded(REDIS_OPGET_READ_ONE_COMMAND_EVENT,
                                 duration/1000);

        if (aof_cur_offset < 0) {
            goto needrollback; /* with errno set by readOneCommand() */
        } else if (aof_cur_offset == aof_last_offset) {
            /* read end of current aof, read_oplog_errno is set to
             * REDIS_READ_OPLOG_EOF by readOneCommand() */
            if (aof_cur_offset == aof_initial_offset) {
                /* nothing has read, find next aof */
                if (!strcmp(state->opget_aof_state->aof_filename,
                            server.aof_filename)) {
                    /* all the aof logs have been read */
                    goto retnull;
                } else {
                    /* read next aof */
                    if (aofReadingSetNextAof(state->opget_aof_state,
                                             REDIS_OPGET_AOF_READ) != C_OK) {
                        read_oplog_errno = REDIS_READ_OPLOG_READ_ERROR;
                        goto retnull;
                    }
                    sdsfree(oplog);
                    /* read an oplog in newly set aof */
                    return readOneOplog(state, opid);
                }
            } else {
                /* for current writing aof, if the latest oplog we have read
                 * is incomplete, we need to rollback and let the
                 * opget client try again */
                if (!strcmp(state->opget_aof_state->aof_filename, server.aof_filename) &&
                    (oplog_cmd_count - i) > 0) {
                    goto needrollback;
                }
                /* we have read the last oplog of an aof,
                 * send the oplog to client */
                if (i == 1) {
                    /* ignore the last single opinfo command */
                    read_oplog_errno = REDIS_READ_OPLOG_IGNORE;
                    goto retnull;
                } else {
                    break;
                }
            }
        }

        /* check format, first command in oplog must be opinfo */
        if (i == 0 && cmd_type != REDIS_OPLOG_OPINFO_CMD) {
            /* if command is opinfo, dbid is updated */
            read_oplog_errno = REDIS_READ_OPLOG_FORMAT_ERROR;
            goto needrollback;
        }

        /* for multi/exec */
        if (cmd_type == REDIS_OPLOG_EXEC_CMD) {
            /* add exec command len */
            aof_last_offset = aof_cur_offset;
            break;
        }

        /* in some cases, eg. a crash recovery or opid roll back
         * when replication is disconnected
         * there may be only one opinfo command in an oplog, as follows
         * opinfo\r\n
         * w_command\r\n
         * opinfo\r\n ----> the badass
         * opinfo\r\n
         * w_command
         *
         * we ignore this oplog with only one opinfo command inside and
         * won't send it to client */
        if (i >= 1 && cmd_type == REDIS_OPLOG_OPINFO_CMD) {
            /* roll back to start of current opinfo command */
            fseek(fp, aof_last_offset, SEEK_SET);
            /* last read opid is reversed too */
            *opid -= 1;
            if (i == 1) {
                /* ignore single opinfo command */
                read_oplog_errno = REDIS_READ_OPLOG_IGNORE;
                goto retnull;
            } else {
                break;
            }
        }

        /* read next command */
        aof_last_offset = aof_cur_offset;
    }

    /* filter */
    if (!isIdMatch(state, serverid) ||
        !isDbMatch(state, dbid) ||
        !isKeyMatch(state, key)) { /* doesn't match */
        read_oplog_errno = REDIS_READ_OPLOG_UNMATCH;
        goto retnull;
    }

    /* generate oplog */
    serverAssert(aof_last_offset > aof_initial_offset);
    size_t oplog_cmds_total_len = aof_last_offset - aof_initial_offset;

    oplog_len = oplog_cmds_total_len;
    if (oplog_len > UINT_MAX) {
        read_oplog_errno = REDIS_READ_OPLOG_READ_ERROR;
        serverLog(LL_WARNING, "oplog length is bigger than %du", UINT_MAX);
        goto needrollback;
    }

    oplog = sdsnewlen(NULL, oplog_len);

    /* add cmds */
    /* go back to the start of this oplog */
    fseek(fp, aof_initial_offset, SEEK_SET);
    /* read an oplog with only one fread() call */
    if (fread(oplog, oplog_cmds_total_len, 1, fp) != 1) {
        read_oplog_errno = REDIS_READ_OPLOG_READ_ERROR;
        goto needrollback;
    }

    /* check if the actual number of commands in oplog and
     * cmd_num saved in oplog header are equal.
     * if not, add ping command in it */
    if (!oplog_with_transaction) {
        for (int j = 0; j < (oplog_cmd_count - i); j++) {
            oplog = sdscat(oplog, "*1\r\n$4\r\nPING\r\n");
        }
    }

    /* success */
    read_oplog_errno = REDIS_READ_OPLOG_SUCC;
    sdsfree(key);
    return oplog;

needrollback:
    /* roll back aof reading offset to last correct one */
    fseek(fp, aof_initial_offset, SEEK_SET);
    *opid = aof_initial_opid;

retnull:
    sdsfree(oplog);
    sdsfree(key);
    return NULL;
}

long long getMinOpidOfSlaves(client **slave) {
    listIter li;
    listNode *ln;
    listRewind(server.slaves, &li);
    long long min = LLONG_MAX;

    while((ln = listNext(&li))) {
        client *tmp = ln->value;
        if (tmp->repl_ack_next_opid > 1 && tmp->repl_ack_next_opid <= min) {
            min = tmp->repl_ack_next_opid;
            *slave = tmp;
        }
    }

    return min;
}

void sendOplogToClient(client *c) {
    int count, matched_count = 0;
    opGetClientState *state = c->opget_client_state;
    int total_count = state->oplog_count;
    int error = 0;
    sds oplog;
    list *oplog_list = listCreate();
    listNode *node;
    long oplog_ret_size = 0;
    long long opid = state->next_start_opid - 1;

    /* if client is getting oplog from master,
     * master will only return oplog whose opid is less than
     * the minimum opid of its slaves */
    if(!server.masterhost && server.opget_master_min_slaves) {
        if(listLength(server.slaves) <
           (unsigned int)server.opget_master_min_slaves) {
            addReplyErrorFormat(c, "opget master has not enough online slave");
            goto cleanup;
        } else {
            client *tmp = NULL;
            long long slave_min_opid = getMinOpidOfSlaves(&tmp);
            /*
             * slave hasn't report opid yet
             */
            if (slave_min_opid == LLONG_MAX) {
                addReplyErrorFormat(c,
                                    "opget master has not enough online slave");
                goto cleanup;
            }
            int min_count = slave_min_opid - state->next_start_opid;
            if (min_count <= 0) {
                addReplyErrorFormat(c, "opget start opid is bigger than slaves' "
                                    "minimum opid, slave(%s), min opid(%lld).",
                                    replicationGetSlaveName(tmp),
                                    slave_min_opid);
                goto cleanup;
            }
            total_count = (min_count < state->oplog_count ?
                           min_count : state->oplog_count);
        }
    }

    /* reading oplog up to total_count number */
    for (count = 0; count < total_count;) {
        if ((oplog = readOneOplog(state, &opid)) == NULL) {
            if (read_oplog_errno == REDIS_READ_OPLOG_UNMATCH) {
                count++;
                continue; /* try next */
            } else if(read_oplog_errno == REDIS_READ_OPLOG_IGNORE) {
                continue;
            } else {
                if (read_oplog_errno != REDIS_READ_OPLOG_EOF) { /* read error */
                    error = 1;
                }
                break;
            }
        } else {
            listAddNodeTail(oplog_list, oplog); /* save oplog into list */
            count++;
            matched_count++;
            oplog_ret_size += sdslen(oplog);
            if (oplog_ret_size > REDIS_OPGET_MAX_RETURN_SIZE) break;
        }
    }

    /* error handling */
    if (error && !matched_count) {
        char *err_msg;
        if (read_oplog_errno == REDIS_READ_OPLOG_READ_ERROR) {
            err_msg = "opget oplog read error";
        } else if (read_oplog_errno == REDIS_READ_OPLOG_FORMAT_ERROR) {
            err_msg = "opget oplog format error";
        } else {
            err_msg = "opget unknown error";
        }

        /* try our best to send
         * if error happened when reading an oplog
         * msg is set to specified error msg,
         * the oplogs which are already successfully read
         * are returned to client */
        addReplyErrorFormat(c, "%s, aof: %s, opid: %lld", err_msg,
                            state->opget_aof_state->aof_filename,
                            state->next_start_opid + count);
        goto cleanup;
    }

    /* no error */
    addReplyMultiBulkLen(c, 2);
    addReplyBulkLongLong(c, opid+1);
    addReplyMultiBulkLen(c, matched_count);
    /* Reply oplog to the client. */
    while ((node = listFirst(oplog_list)) != NULL) { /* reply content */
        sds val = listNodeValue(node);
        addReplyBulkSds(c, val);
        listDelNode(oplog_list, node);
    }

    /* update expected next opid of opget client */
    state->next_start_opid = opid + 1;

    /* for situation in which opid is reversed */
    if (state->next_start_opid > server.next_opid)
        state->next_start_opid = server.next_opid;

cleanup:
    /* cleanup */
    listSetFreeMethod(oplog_list, (void (*)(void *))sdsfree);
    listRelease(oplog_list);
}

void initOpGetClientState(opGetClientState **client_state) {
    *client_state = (opGetClientState *)zmalloc(sizeof(struct opGetClientState));
    opGetClientState *state = *client_state;
    state->next_start_opid = 1;
    state->opget_aof_state = NULL;
    state->use_matchdb = 1;
    state->matchdbs = intsetNew();
    state->use_matchid = 1;
    state->matchids = intsetNew();
    state->use_matchkey = 1;
    state->matchkeys = listCreate();
    listSetFreeMethod(state->matchkeys, decrRefCountVoid);
    state->oplog_count = REDIS_DEFAULT_RETURNED_OPLOG_COUNT;
    state->wait_bio_res = 0;
}

void opGetCommand(client *c) {
    if (server.aof_state != AOF_ON) {
        addReplyError(c, "redis aof is off");
    }

    long long start_opid;
    int j, i = 2;
    int reset_matchdb_options = 1, reset_matchkey_options = 1,
        reset_matchid_options = 1;
    long long count, dbid, serverid;
    sds pat = NULL;
    int patlen = 0;
    opGetClientState *state;

    /* init opget client stat */
    if (c->opget_client_state == NULL) {
        c->flags |= REDIS_OPGET_CLIENT;
        listAddNodeTail(server.opget_client_list, c);
        initOpGetClientState(&c->opget_client_state);
    }
    state = c->opget_client_state;

    /* get start opid from client */
    if (getLongLongFromObject(c->argv[1], &start_opid) != C_OK ||
        start_opid < 1) {
        addReplyError(c, "opget invalid start opid");
        return;
    }

    if (start_opid > server.next_opid) {
        addReplyErrorFormat(c,
                            "opget client start opid is bigger than "
                            "server next opid(%lld)",
                            server.next_opid);
        return;
    }

    /* check if the start opid sent by client is valid or not
     * state->opget_aof_state != NULL indicates that
     * it's not the first time that client issue an opget command */
    if (state->opget_aof_state != NULL &&
        state->next_start_opid != start_opid) {
        addReplyErrorFormat(c,
                            "opget discontinuous opid, you may miss some oplog,"
                            " expected opid is: %lld",
                            state->next_start_opid);
        return;
    }

    /* opget sent first time, update first start opid */
    if (!state->opget_aof_state) state->next_start_opid = start_opid;

    /* first we need to parse optional parameters */
    while (i < c->argc) {
        j = c->argc - i;
        if (!strcasecmp(c->argv[i]->ptr, "count") && j >= 2) {
            if (getLongLongFromObject(c->argv[i+1], &count) != C_OK) {
                addReplyErrorFormat(c,
                                    "opget invalid COUNT value: %lld", count);
                return;
            }
            if (count > server.opget_max_count || count < 1) {
                addReplyErrorFormat(c,
                                    "opget invalid COUNT value: %lld", count);
                return;
            }
            if (!server.masterhost && count > server.opget_max_count/10) {
                count = server.opget_max_count/10;
            }
            state->oplog_count = (int)count;
            i += 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "matchdb") && j >= 2) {
            /* client has modified the matchdb filter option
             * after last matchdb options sent,
             * we need to update related saved options in client structure */
            if (state->opget_aof_state != NULL && reset_matchdb_options) {
                zfree(state->matchdbs);
                state->matchdbs = intsetNew();
                state->use_matchdb = 1;
                reset_matchdb_options = 0;
            }
            if (state->use_matchdb == 0) {
                i += 2;
                continue;
            }
            if (getLongLongFromObject(c->argv[i+1], &dbid) != C_OK) {
                addReplyErrorFormat(c,
                                    "opget invalid MATCHDB value: %lld", dbid);
                return;
            }
            if (dbid == -1) {
                state->use_matchdb = 0;
                i += 2;
                continue;
            } else if (dbid < 0 || dbid >= server.dbnum) {
                addReplyErrorFormat(c,
                                    "opget invalid MATCHDB value: %lld", dbid);
                return;
            }
            state->matchdbs = intsetAdd(state->matchdbs, dbid, NULL);
            i += 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "matchkey") && j >= 2) {
            /* client has modified the matchkey filter option
             * after connection connected,
             * we need to update related saved options in client structure */
            if (state->opget_aof_state != NULL && reset_matchkey_options) {
                listRelease(state->matchkeys);
                state->matchkeys = listCreate();
                listSetFreeMethod(state->matchkeys, decrRefCountVoid);
                state->use_matchkey = 1;
                reset_matchkey_options = 0;
            }
            if (state->use_matchkey == 0) {
                i += 2;
                continue;
            }
            pat = c->argv[i+1]->ptr;
            patlen = sdslen(pat);
            /* The pattern always matches if it is exactly "*", so it is
             * equivalent to disabling it. */
            if (pat[0] == '*' && patlen == 1) {
                state->use_matchkey = 0;
                i += 2;
                continue;
            }
            listAddNodeTail(state->matchkeys, c->argv[i+1]);
            incrRefCount(c->argv[i+1]);
            i += 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "matchid") && j >= 2) {
            if (state->opget_aof_state != NULL && reset_matchid_options) {
                zfree(state->matchids);
                state->matchids = intsetNew();
                state->use_matchid = 1;
                reset_matchid_options = 0;
            }
            if (state->use_matchid == 0) {
                i += 2;
                continue;
            }
            if (getLongLongFromObject(c->argv[i+1], &serverid) != C_OK) {
                addReplyErrorFormat(c,
                                    "opget invalid MATCHID value: %lld",
                                    serverid);
                return;
            }
            if (serverid == -1) {
                state->use_matchid = 0;
                i += 2;
                continue;
            } else if (serverid < 0) {
                addReplyErrorFormat(c,
                                    "opget invalid MATCHID value: %lld",
                                    serverid);
                return;
            }
            state->matchids = intsetAdd(state->matchids, serverid, NULL);
            i += 2;
        } else {
            addReply(c, shared.syntaxerr);
            return;
        }
    }
    serverLog(LL_DEBUG,
             "start_opid: %lld, count: %d, matchdbs set len: %d, "
             "matchids set len: %d, matchkeys list len: %lu",
             start_opid, c->opget_client_state->oplog_count,
             intsetLen(c->opget_client_state->matchdbs),
             intsetLen(c->opget_client_state->matchids),
             listLength(c->opget_client_state->matchkeys));

    if (start_opid == server.next_opid) {
        /* server hasn't received any write commands yet, no oplog replied */
        addReplyMultiBulkLen(c, 2);
        addReplyBulkLongLong(c, start_opid);
        addReplyMultiBulkLen(c, 0);
        return;
    }

    if (state->opget_aof_state == NULL) {
        /* if it's the first time that client send a opget command
         * we need to create a background job to find the
         * aof offset by the specified start opid */
        bioCreateBackgroundJob(BIO_FIND_OFFSET_BY_OPID, c,
                               (void *)(long)start_opid, (void *)(long)c->id);
        state->wait_bio_res = 1;
    } else {
        /* we needn't to use bio to find aof offset every time,
         * only the first time when the connection is established*/
        sendOplogToClient(c);
    }
}

void syncstateCommand(client *c) {
    if (listLength(server.slaves) == 0) {
        addReplyError(c, "No slave connected");
        return;
    }

    char *slave_ip = c->argv[1]->ptr;
    char *slave_port = c->argv[2]->ptr;
    char *sync_type = c->argv[3]->ptr;

    client *slave;
    listNode *ln;
    listIter li;
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        slave = listNodeValue(ln);
        char ip[NET_IP_STR_LEN];
        int port;
        if (anetPeerToString(slave->fd,ip,sizeof(ip),&port) == -1) continue;
        if (!strcmp(ip, slave_ip) && slave->slave_listening_port == atoi(slave_port)) {
            break;
        }
    }

    if (!ln) {
        addReplyErrorFormat(c, "No slave %s:%s connected", slave_ip, slave_port);
        return;
    }

    if (!strcasecmp(sync_type, "FULL")) {
        if (slave->replstate == SLAVE_STATE_ONLINE) {
            addReplyStatus(c, "SYNCED");
        } else {
            addReplyStatus(c, "SYNCING");
        }
    }
    else if (!strcasecmp(sync_type, "INCR")) {
        addReplyError(c, "Instance not readonly");
    } else {
        addReplyErrorFormat(c, "Sync type %s invalid", sync_type);
    }
}

/* opRestore key */
void opRestoreCommand(client *c) {
    robj *o;
    rio payload;
    robj **newargv = NULL;
    const int newargc = 5;
    long long expire, ttl = 0;

    /* Check server is master now */
    if (server.masterhost) {
        addReplyError(c, "Command rejected by slave instance");
        return;
    }

    /* Check if the key is here. */
    if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk)) == NULL)
        return;

    /* Check if the key has ttl. */
    expire = getExpire(c->db, c->argv[1]);
    if (expire != -1) {
        ttl = expire - mstime();
        if (ttl <= 0) {
            expireIfNeeded(c->db, c->argv[1]);
            addReply(c, shared.nullbulk);
            return;
        }
    }

    /* Create the DUMP encoded representation. */
    createDumpPayload(&payload,o);

    newargv = zmalloc(sizeof(robj*) * (newargc));
    newargv[0] = createStringObject("RESTORE", 7);
    newargv[1] = c->argv[1];
    incrRefCount(c->argv[1]);
    newargv[2] = createStringObjectFromLongLong(ttl);
    newargv[3]= createObject(OBJ_STRING,payload.io.buffer.ptr);
    newargv[4] = createStringObject("REPLACE", 7);

    /* Translate OPRESTORE as RESTORE for replication/AOF. */
    replaceClientCommandVector(c, newargc, newargv);
    /* asumme that we call restore command. */
    //forceCommandPropagation(c, REDIS_PROPAGATE_REPL | REDIS_PROPAGATE_AOF);
    server.dirty++;
    addReply(c, shared.ok);

    return;
}

void getOpidByAofCommand(client *c) {
    long long opid = getAofFirstOpid(c->argv[1]->ptr);
    char *msg = "+Empty file\r\n";
    if (opid == REDIS_EMPTY_FILE) {
        addReplyString(c, msg, strlen(msg));
    } else if (opid == C_ERR) {
        addReplyError(c, "Get opid by aof filename error");
    } else if (opid == REDIS_NO_FIRST_OPID) {
        addReplyString(c, "+Old format aof\r\n", strlen("+Old format aof\r\n"));
    } else {
        addReplyLongLong(c, opid);
    }
}

int checkOpgetClientWaitingBioState(client *c) {
    /* Immediately abort if the client is opget client and
     * waiting for bio result */
    if (c->flags & REDIS_OPGET_CLIENT &&
        c->opget_client_state->wait_bio_res) return C_OK;

    return C_ERR;
}

/* apply oplog from BLS on redis */
void opApplyCommand(client *c) {
    c->flags |= REDIS_OPAPPLY_CLIENT;
    addReply(c, shared.ok);
}
