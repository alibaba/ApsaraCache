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

#include "server.h"
#include "aof_buf_queue.h"
#include <fcntl.h>

/* Create queue to handle aof request */
aofQueue *aofQueueCreate() {
    aofQueue *queue = zcalloc(sizeof(aofQueue));

    if (queue != NULL) {
        queue->req_queue = queueCreate();
        queue->file_list = listCreate();
        pthread_mutex_init(&(queue->mutex), NULL);
        pthread_cond_init(&(queue->cond), NULL);
    }

    return queue;
}

/* Find aofFileItem from file_list base on fd value */
aofFileItem *listFindFileItem(list *file_list, int fd) {
    listIter iter = { .next = file_list->head, .direction = AL_START_HEAD};
    listNode *node;

    while((node = listNext(&iter)) != NULL) {
        aofFileItem *item = (aofFileItem *)(node->value);
        if (item != NULL && item->fd == fd) {
            return item;
        }
    }

    return NULL;
}

/* Create aofFileItem base on fd value, and insert into file_list */
aofFileItem *listAddFileItem(list *file_list, int fd) {
    aofFileItem *item = zcalloc(sizeof(aofFileItem));

    if (item) {
        item->fd = fd;
        listAddNodeTail(file_list, item);
    }

    return item;
}

/* Return aofFileItem from file_list base on fd value , if it doesn't exists, add new one. */
aofFileItem *listGetFileItem(list *file_list, int fd) {
    aofFileItem *file = listFindFileItem(file_list, fd);

    if (file == NULL) {
        file = listAddFileItem(file_list, fd);
    }

    return file;
}

/* Wait for aof buffer written to file,or return if the relative time specified by wait passes */
void aofQueueCondWait(aofQueue *queue, long long wait ) {
    struct timespec   ts;

    wait += ustime();
    ts.tv_sec =  (wait / 1000000);
    ts.tv_nsec = ((wait % 1000000) * 1000);
    pthread_cond_timedwait(&(queue->cond), &(queue->mutex), &ts);
}

/* Choose wait time base on queue total_buf_len value if total_buf_len >= buf_limit*2/3 and total_buf_len < buf_limit */
void aofQueueFlowCtrlTimeWait(aofQueue *queue, size_t buf_limit) {
    static const useconds_t us_sleep[] = {10000, 20000, 40000, 80000, 100000};
    static const size_t arry_size = ARR_LEN(us_sleep);
    static long long log_condwait = 0;
    long long start_condwait = ustime();
    long long latency = 0;
    size_t len = aofQueueTotalLen(queue);

    if (len >= buf_limit) {
        return ;
    }

    for (int i=(arry_size-1); i>=0; i--) {
        size_t limit = buf_limit * (i+2)/(i+3);
        if (len > limit) {
            pthread_mutex_lock(&(queue->mutex));
            aofQueueCondWait(queue, us_sleep[i]);
            pthread_mutex_unlock(&(queue->mutex));
            latency = (ustime() - start_condwait);

            if (latency > 0) {
                queue->flow_ctrl_duration += latency;
                queue->flow_ctrl_times++;

                if ((start_condwait - log_condwait) > AOF_BIO_LATENCY_LOG_RATE_US) {
                    serverLog(LL_WARNING, " aof buffer size is %ld, limit=%ld, and flow ctrl time wait %lldus", len, limit, latency);
                    log_condwait = start_condwait;
                }
            }

            return ;
        }
    }
}

/* Wait until queue total_buf_len < buf_limit if total_buf_len >= buf_limit */
#define AOF_QUEUE_BUF_LIMIT_WAIT_INTERVAL_US (100*1000) //100 ms
void aofQueueFlowCtrlBufLimitWait(aofQueue *queue, size_t buf_limit) {
    static long long log_condwait = 0;
    long long start_condwait = ustime();
    long long latency = 0;
    size_t len = aofQueueTotalLen(queue);

    if ((len < buf_limit) || (len == 0)) {
        return ;
    }

    pthread_mutex_lock(&(queue->mutex));
    while((queue->total_buf_len >= buf_limit) && (queue->total_buf_len > 0)) {
        aofQueueCondWait(queue, AOF_QUEUE_BUF_LIMIT_WAIT_INTERVAL_US);
    }
    pthread_mutex_unlock(&(queue->mutex));

    latency = ustime() - start_condwait;
    if (latency > 0) {
        queue->flow_ctrl_duration += latency;
        queue->flow_ctrl_times++;

        if((start_condwait - log_condwait) > AOF_BIO_LATENCY_LOG_RATE_US) {
            serverLog(LL_WARNING, "aof buffer size is %ld, limit=%ld, and flow ctrl buf limit wait %lldus for aof buf write", len, buf_limit, latency);
            log_condwait = start_condwait;
        }
    }
}

/* Flow control function
 * 1. Return directly if queue total_buf_len < server.aof_buf_limit/2
 * 2. Call pthread_yield to relinquish the CPU if queue aof_buf_len >= server.aof_buf_limit/2 and aof_buf_len < server.aof_buf_limit*2/3
 * 3. Choose wait time base on queue total_buf_len value if total_buf_len >= server.aof_buf_limit*2/3 and total_buf_len < server.aof_buf_limit
 * 4. Wait until queue total_buf_len < server.aof_buf_limit if total_buf_len >= server.aof_buf_limit */
void aofQueueFlowCtrl(aofQueue *queue) {
    size_t len = aofQueueTotalLen(queue);
    size_t buf_limit = server.aof_buf_limit;

    if (len < buf_limit / 2) {
        return;
    }

    if ((len >= buf_limit / 2)
            && (len < buf_limit * 2 / 3)) {
        pthread_yield();
        return ;
    }

    if ((len >= buf_limit * 2/3)
            && (len < buf_limit)) {
        aofQueueFlowCtrlTimeWait(queue, buf_limit);
        return ;
    }

    aofQueueFlowCtrlBufLimitWait(queue, buf_limit);
}

/* Create request item to write buf */
aofReqItem *aofQueueCreateWriteReq(aofQueue *queue, int fd, sds buf) {
    aofReqItem *item = NULL;

    if (buf != NULL && sdslen(buf) > 0) {
        aofFileItem *file = listGetFileItem(queue->file_list, fd);

        if ((file != NULL) &&
                ((item = zcalloc(sizeof(aofReqItem))) != NULL)) {
            item->buf = buf;
            item->file = file;
            item->req = AOF_BUF_ACT_WRITE;
        }
    }

    return item;
}

/* Create request item to sync file */
aofReqItem *aofQueueCreateSyncReq(aofQueue *queue, int fd) {
    aofFileItem *file = listFindFileItem(queue->file_list, fd);
    aofReqItem *item = NULL;

    if ((file != NULL) &&
            ((item = zcalloc(sizeof(aofReqItem))) != NULL)) {
        item->file = file;
        item->req = AOF_BUF_ACT_SYNC;
    }

    return item;
}

/* Create bio_job to handle aof queue if background thread is idle */
void aofQueueSendBioHandleAofJob(aofQueue *queue) {
    if (atomic_add(queue->handling, 0) == 0) {
        bioCreateBackgroundJob(BIO_AOF_FSYNC, queue, (void *)BIO_HANDLE_AOF_QUEUE, NULL);
    }
}

/* Control flow and monitor control time */
void aofQueueMonitorFlowCtrl(aofQueue *queue) {
    mstime_t latency;

    latencyStartMonitor(latency);
    aofQueueFlowCtrl(queue);
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("aof-queue-bio-write-flow-ctrl",latency);

}

/* Push request item to write buf */
void aofQueuePushWriteFileReq(aofQueue *queue, int fd, sds buf) {
    aofReqItem *writeReq = aofQueueCreateWriteReq(queue, fd, buf);

    if (writeReq) {
        queuePush(queue->req_queue, (queueNode *)writeReq);
        atomic_add(queue->total_buf_len, sdslen(buf));
        aofQueueSendBioHandleAofJob(queue);
        aofQueueMonitorFlowCtrl(queue);
    }
}

/* Check whether need to do sync operation */
int aofQueueNeedSync(aofQueue *queue, int fd) {
    if (aofQueueTotalLen(queue) > 0) {
        return 1;
    }

    aofFileItem *file = listFindFileItem(queue->file_list, fd);

    if (file != NULL) {
        return atomic_add(file->need_sync, 0);
    }

    return 0;
}

/* Push request item to sync file */
void aofQueuePushSyncFileReq(aofQueue *queue, int fd) {
    aofReqItem *syncReq = aofQueueCreateSyncReq(queue, fd);

    if (syncReq) {
        queuePush(queue->req_queue, (queueNode *)syncReq);
        aofQueueSendBioHandleAofJob(queue);
        aofQueueMonitorFlowCtrl(queue);
    }
}

/* Pop one request item from the queue to handle */
aofReqItem *aofQueuePopReq(aofQueue *queue) {
    return (aofReqItem *)queuePop(queue->req_queue);
}

/* Force to insert one request item at the head of the queue */
void aofQueueSetHeadReq(aofQueue *queue, aofReqItem *item) {
    queueSetHead(queue->req_queue, (queueNode *)item);
}

/* Sync file content to disk */
#define AOF_QUEUE_BIO_SYNC_RATE_US (1*1000*1000) //1 second
#define AOF_QUEUE_BIO_SYNC_LATENCY_US (500*1000) // 500 ms
int syncAofFile(aofFileItem *file) {
    static long long last_record_time = 0;

    if (file->need_sync == 0) {
        return 0;
    }

    long long us_begin = ustime();

    if ((us_begin - file->last_sync) > AOF_QUEUE_BIO_SYNC_RATE_US) {
        if (aof_fsync(file->fd) != 0) {
            char path[PATH_MAX];
            getFilePathByFd(file->fd, path, sizeof(path));
            serverLog(LL_WARNING, "Failed to sync file %s,err is %s", path, strerror(errno));
            return -1;
        }

        long long us_end = ustime();
        long long us_latency = us_end - us_begin;
        int tmp = 1;

        atomic_bool_compare_and_swap(file->need_sync, tmp, 0);
        file->last_sync = us_end;

        if ((us_latency > AOF_QUEUE_BIO_SYNC_LATENCY_US) &&
               (us_end - last_record_time > AOF_BIO_LATENCY_LOG_RATE_US)) {
            serverLog(LL_WARNING, "bio sync file size=%ld and latency is %lldus",
                    (file->filesize - file->sync_offset), us_latency);
            last_record_time = us_end;
        }

        file->sync_offset = file->filesize;
    }

    return 0;
}

void handleAofWriteIssue(aofFileItem *file, ssize_t buf_len, ssize_t nwritten, long long ustime, int syserr) {
    static long long last_error_time = 0;

    /* If fail to write then truncate to original file size and set request item at the head of the queue */
    int can_log = 0;
    if ((server.aof_last_write_errno = syserr) != 0) {
        server.aof_last_write_status = C_ERR;
    }

    if (ustime > (last_error_time + AOF_BIO_LATENCY_LOG_RATE_US)) {
        can_log = 1;
        last_error_time = ustime;
    }

    if (can_log) {
        char path[PATH_MAX];
        getFilePathByFd(file->fd, path, sizeof(path));
        serverLog(LL_WARNING,"Short bio write while writing to "
                               "%s: (nwritten=%ld, "
                               "expected=%ld), error:%s",
                               path, nwritten, buf_len, strerror(syserr));
    }

    /* Truncate to original file size */
    if (ftruncate(file->fd, file->filesize) && can_log) {
        char path[PATH_MAX];
        getFilePathByFd(file->fd, path, sizeof(path));
        serverLog(LL_WARNING, "Could not remove short bio write "
             "from %s.  Redis may refuse "
             "to load the AOF the next time it starts.  "
             "ftruncate: %s", path, strerror(syserr));
    }
}

/* Write item->buf to aof file and unblock the thread that waiting for aof write */
#define AOF_QUEUE_BIO_WRITE_LATENCY_US (300*1000) // 300 ms
ssize_t aofQueueHandleWriteReq(aofQueue *queue, aofReqItem *item) {
    static long long last_record_time = 0;
    ssize_t buf_len = sdslen(item->buf);
    long long us_begin = ustime();
    ssize_t nwritten = safe_write(item->file->fd, item->buf, buf_len);

    if (nwritten != buf_len) {
        handleAofWriteIssue(item->file, buf_len, nwritten, us_begin, errno);
        /* Force to insert the request item at the head of the queue */
        aofQueueSetHeadReq(queue, item);
        return -1;
    }

    int tmp = 0;
    atomic_bool_compare_and_swap(item->file->need_sync, tmp, 1);

    long long us_end = ustime();
    long long us_latency = us_end - us_begin;

    if (server.aof_last_write_status == C_ERR) {
        server.aof_last_write_status = C_OK;
        serverLog(LL_WARNING, "AOF bio write error looks solved, Redis can write again.");
    }

    if ((us_latency > AOF_QUEUE_BIO_SYNC_LATENCY_US) &&
           (us_end - last_record_time > AOF_QUEUE_BIO_WRITE_LATENCY_US)) {
        serverLog(LL_WARNING, "AOF bio write data=%ld and latency is %lldus", buf_len, us_latency);
        last_record_time = us_end;
    }

    sdsfree(item->buf);
    item->file->filesize += buf_len;
    atomic_sub(queue->total_buf_len, buf_len);

    pthread_mutex_lock(&(queue->mutex));
    pthread_cond_signal(&(queue->cond));
    pthread_mutex_unlock(&(queue->mutex));
    return nwritten;
}

/* Create bio job to close file */
int createBioCloseFdJob(aofFileItem *file) {
    bioCreateBackgroundJob(BIO_CLOSE_FILE, (void *)((long )(file->fd)), (void *)(long)BIO_CLOSE_FD, NULL);
    zfree(file);
    return 0;
}

/* Handle aof file write, sync and close request */
void aofQueueHandleReq(aofQueue *queue) {
    int tmp;
    aofReqItem *item = NULL;

    while((item = aofQueuePopReq(queue)) != NULL) {
        int need_free = 1;
        tmp = 0;
        atomic_bool_compare_and_swap(queue->handling, tmp, 1);

        switch(item->req) {
            case AOF_BUF_ACT_WRITE:
                if (aofQueueHandleWriteReq(queue, item) < 0) {
                    /* Write failed and we will try to rewrite this item */
                    need_free = 0;
                }
                break;
            case AOF_BUF_ACT_CLOSE:
                createBioCloseFdJob(item->file);
                break;
            case AOF_BUF_ACT_SYNC:
                syncAofFile(item->file);
                break;
            default:
                serverLog(LL_WARNING, "Unknown aof queue req=%d", item->req);
                break;
        }

        if (need_free) {
            zfree(item);
        }

        tmp = 1;
        atomic_bool_compare_and_swap(queue->handling, tmp, 0);
    }
}

/* Push request item to close file */
int aofQueuePushCloseFileReq(aofQueue *queue, int fd) {
    listIter iter = { .next = queue->file_list->head, .direction = AL_START_HEAD};
    listNode *node;
    aofFileItem *file  = NULL;

    /* The fd value must be unique in file_list, so the node of fd must be removed first
     * and then send close reqest to avoid open same fd after close */
    while((node = listNext(&iter)) != NULL) {
        file = (aofFileItem *)(node->value);
        if (file != NULL && file->fd == fd) {
            listDelNode(queue->file_list, node);
            break;
        }
    }

    if (file != NULL) {
        aofReqItem *item = zcalloc(sizeof(aofReqItem));
        item->file = file;
        item->req = AOF_BUF_ACT_CLOSE;
        queuePush(queue->req_queue, (queueNode *)item);
        bioCreateBackgroundJob(BIO_AOF_FSYNC, queue, (void *)BIO_HANDLE_AOF_QUEUE, NULL);
    } else {
        bioCreateBackgroundJob(BIO_CLOSE_FILE, (void *)((long)(fd)), (void *)(long)BIO_CLOSE_FD, NULL);
    }

    return 0;
}

void aofQueueHandleAppendOnlyFlush(int force) {
    int sync = force ? 1 : (server.unixtime > server.aof_last_fsync);
    size_t val = server.aof_buf_limit;
    size_t len = sdslen(server.aof_buf);

    if (force) {
        server.aof_buf_limit = 0;
    }

    if (len > 0) {
        aofQueuePushWriteFileReq(server.aof_queue, server.aof_fd, server.aof_buf);
        server.aof_current_size += len;
        server.aof_total_size += len;
        server.aof_inc_from_last_cron_bgsave += len;
        server.aof_buf = sdsempty();
    }

    if (sync && aofQueueNeedSync(server.aof_queue, server.aof_fd)) {
        aofQueuePushSyncFileReq(server.aof_queue, server.aof_fd);
        server.aof_last_fsync = server.unixtime;
    }

    server.aof_buf_limit = val;
}

int enableOrDisableAofBioWrite(struct client *c, int sync_mode) {
    if ((sync_mode != AOF_FSYNC_BIO_WRITE) &&
        (server.aof_fsync == AOF_FSYNC_BIO_WRITE)) {
        flushAppendOnlyFile(1);
    } else if ((sync_mode == AOF_FSYNC_BIO_WRITE) &&
               (server.aof_queue == NULL)) {
        server.aof_queue = aofQueueCreate();
        if (server.aof_queue == NULL) {
            addReplyError(c, "Unable to create aof queue. Check server logs.");
            return C_ERR;
        }
    }

    server.aof_fsync = sync_mode;
    return C_OK;
}

void aofBioWriteInit(void) {
    if (server.aof_fsync == AOF_FSYNC_BIO_WRITE) {
        server.aof_queue = aofQueueCreate() ;
        if (server.aof_queue == NULL) {
            serverLog(LL_WARNING,
                     "Can't not create aof queue for append-only file: %s",
                     strerror(errno));
            exit(1);
        }
    }
}

sds catAofBioInfo(sds info) {
    return sdscatprintf(info,
                        "# AofBioStats\r\n"
                        "bio_aof_file_num:%ld\r\n"
                        "bio_aof_queue_buf_len:%ld\r\n"
                        "bio_flow_ctrl_duration:%lld\r\n"
                        "bio_flow_ctrl_times:%lld\r\n"
                        "bio_handling:%d\r\n",
                        listLength(server.aof_queue->file_list),
                        aofQueueTotalLen(server.aof_queue),
                        server.aof_queue->flow_ctrl_duration,
                        server.aof_queue->flow_ctrl_times,
                        atomic_add(server.aof_queue->handling, 0));
}
