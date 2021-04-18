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

#ifndef __AOF_BUF_QUEUE_H
#define __AOF_BUF_QUEUE_H
#include <unistd.h>
#include <pthread.h>
#include <sys/param.h>
#include "server.h"
#include "config.h"
#include "adlist.h"
#include "bio.h"
#include "sds.h"
#include "msqueue.h"
#include "util.h"

#ifdef __APPLE__
#define pthread_yield sched_yield
#endif

#define REDIS_AOF_BUF_QUEUE_DEFAULT_LIMIT (1 << 30)
#define REDIS_AOF_BUF_QUEUE_MIN_LIMIT (256*1024*1024)
#define AOF_BIO_LATENCY_LOG_RATE_US (30*1000*1000) //30 seconds
#define REDIS_DEFAULT_RDB_SAVE_INCREMENTAL_FSYNC 1
#define REDIS_RDB_AUTOSYNC_BYTES (1024*1024*32) /* fdatasync every 32MB */
#define AOF_FSYNC_BIO_WRITE 3

struct aofQueue;

typedef struct aofFileItem {
    int fd;
    int need_sync;
    size_t filesize;
    size_t sync_offset;
    long long last_sync;
} aofFileItem;

#define AOF_BUF_ACT_WRITE 0
#define AOF_BUF_ACT_CLOSE 1
#define AOF_BUF_ACT_SYNC  2

typedef struct aofReqItem {
    queueNode  node;
    int req;
    aofFileItem *file;
    sds buf;
} aofReqItem;

typedef struct aofQueue {
    queue       *req_queue;
    list        *file_list;
    size_t      total_buf_len;
    int         handling;
    long long   flow_ctrl_duration;
    long long   flow_ctrl_times;
    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
} aofQueue;

aofQueue *aofQueueCreate();
void aofQueuePushWriteFileReq(aofQueue *queue, int fd, sds buf);
int aofQueueNeedSync(aofQueue *queue, int fd);
void aofQueuePushSyncFileReq(aofQueue *queue, int fd);
int aofQueuePushCloseFileReq(aofQueue *queue, int fd);
aofReqItem *aofQueueBufPop(aofQueue *queue);
void aofQueueHandleReq(aofQueue *queue);
void aofQueueHandleAppendOnlyFlush(int force);
int enableOrDisableAofBioWrite(struct client *c, int sync_mode);
void aofBioWriteInit(void);
sds catAofBioInfo(sds info);

inline size_t aofQueueTotalLen(aofQueue *queue) {
    return atomic_add(queue->total_buf_len, 0);
}

#endif
