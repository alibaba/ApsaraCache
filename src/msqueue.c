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

#include "msqueue.h"
#include "zmalloc.h"

queue* queueCreate() {
    queue *q = zcalloc(sizeof(queue));
    if (q != NULL) {
        return queueInit(q);
    }

    return NULL;
}

queue* queueInit(queue *q) {
    pthread_spin_init(&(q->head_lock), 0);
    pthread_spin_init(&(q->tail_lock), 0);
    q->divider.next = NULL;
    q->head = &(q->divider);
    q->tail = &(q->divider);

    return q;
}

void queuePush(queue *q, queueNode *node) {
    node->next = NULL;
    pthread_spin_lock(&(q->tail_lock));
    q->tail->next = node;
    q->tail = node;
    pthread_spin_unlock(&(q->tail_lock));

}

void queueSetHead(queue *q, queueNode *node) {
    pthread_spin_lock(&(q->head_lock));
    node->next = q->head;
    q->head = node;

    pthread_spin_unlock(&(q->head_lock));
}

queueNode *queuePop(queue *q) {
    queueNode *head, *next;
    while (1) {
        pthread_spin_lock(&(q->head_lock));
        head = q->head;
        next = head->next;
        if (next == NULL) {
            pthread_spin_unlock(&(q->head_lock));
            return NULL;
        }

        q->head = next;
        pthread_spin_unlock(&(q->head_lock));

        if (head == &(q->divider)) {
            queuePush(q, head);
            continue;
        }

        head->next = NULL;

        return head;
    }
}

