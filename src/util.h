/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __REDIS_UTIL_H
#define __REDIS_UTIL_H

#include <stdint.h>
#include "sds.h"

#define ARR_LEN(arr) ((sizeof(arr))/(sizeof(arr[0])))

#if defined(__ATOMIC_RELAXED)
#define atomic_add(q, n) __atomic_add_fetch(&(q), (n), __ATOMIC_RELAXED)
#define atomic_sub(q, n) __atomic_sub_fetch(&(q), (n), __ATOMIC_RELAXED)
#define atomic_bool_compare_and_swap(q, o, n) __atomic_compare_exchange_n(&(q), &(o), (n), 0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)
#else
#define atomic_add(q, n) __sync_add_and_fetch(&(q), (n))
#define atomic_sub(q, n) __sync_sub_and_fetch(&(q), (n))
#define atomic_bool_compare_and_swap(q, o, n) __sync_bool_compare_and_swap(&(q), (o), (n))
#endif

int stringmatchlen(const char *p, int plen, const char *s, int slen, int nocase);
int stringmatch(const char *p, const char *s, int nocase);
long long memtoll(const char *p, int *err);
uint32_t digits10(uint64_t v);
uint32_t sdigits10(int64_t v);
int ll2string(char *s, size_t len, long long value);
int string2ll(const char *s, size_t slen, long long *value);
int string2l(const char *s, size_t slen, long *value);
int string2ld(const char *s, size_t slen, long double *dp);
int d2string(char *buf, size_t len, double value);
int ld2string(char *buf, size_t len, long double value, int humanfriendly);
sds getAbsolutePath(char *filename);
int pathIsBaseName(char *path);
unsigned ull2string(char *s, size_t slen, uint64_t value);

ssize_t safe_write(int fd, const char *buf, size_t len);
int getFilePathByFd(int fd, char *buf, size_t len);

/* Wrappers around strtoull/strtoll that are safer and easier to
 * use.  For tests and assumptions, see internal_tests.c.
 *
 * str   a NULL-terminated base decimal 10 unsigned integer
 * out   out parameter, if conversion succeeded
 *
 * returns true if conversion succeeded.
 */
int safe_strtoull(const char *str, uint64_t *out);
int safe_strtoll(const char *str, int64_t *out);
int safe_strtoul(const char *str, uint32_t *out);
int safe_strtol(const char *str, int32_t *out);


#ifdef REDIS_TEST
int utilTest(int argc, char **argv);
#endif

#endif
