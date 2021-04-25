/* Redis aof tootls.
 * dejun.xdj@alibaba-inc.com
 * 2017-10-02 18:43
 */

#include "fmacros.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

#include "config.h"
#include "sds.h"
#include "zmalloc.h"
#include "redis_oplog.h"

#define C_OK 0
#define ERROR(...) {                                            \
        char __buf[1024];                                       \
        sprintf(__buf, __VA_ARGS__);                            \
        sprintf(error, "0x%16llx: %s", (long long)epos, __buf); \
    }

static struct config {
    int check_aof;
    int fix_aof;
    int format_aof;
    int scan_aof;
    char *aof_filename;
    char *scan_key;
    char *scan_sub;
} config;

struct argss {
    int argc;
    sds *argv;
};

static char error[1024];
static off_t epos;

int consumeNewline(char *buf) {
    if (strncmp(buf,"\r\n",2) != 0) {
        ERROR("Expected \\r\\n, got: %02x%02x",
              buf[0],buf[1]);
        return 0;
    }
    return 1;
}

int readLong(FILE *fp, char prefix, long *target) {
    char buf[128], *eptr;
    epos = ftello(fp);
    if (fgets(buf,sizeof(buf),fp) == NULL) {
        return 0;
    }
    if (buf[0] != prefix) {
        ERROR("Expected prefix '%c', got: '%c'",prefix,buf[0]);
        return 0;
    }
    *target = strtol(buf+1,&eptr,10);
    return consumeNewline(eptr);
}

int readBytes(FILE *fp, char *target, long length) {
    long real;
    epos = ftello(fp);
    real = fread(target,1,length,fp);
    if (real != length) {
        ERROR("Expected to read %ld bytes, got %ld bytes",length,real);
        return 0;
    }
    return 1;
}

int readString(FILE *fp, char** target) {
    long len;
    *target = NULL;
    if (!readLong(fp,'$',&len)) {
        return 0;
    }

    /* Increase length to also consume \r\n */
    len += 2;
    *target = (char*)zmalloc(len);
    if (!readBytes(fp,*target,len)) {
        return 0;
    }
    if (!consumeNewline(*target+len-2)) {
        return 0;
    }
    (*target)[len-2] = '\0';
    return 1;
}

int readArgc(FILE *fp, long *target) {
    return readLong(fp,'*',target);
}

off_t process(FILE *fp) {
    long argc;
    off_t pos = 0;
    int i, multi = 0;
    char *str;

    while(1) {
        if (!multi) pos = ftello(fp);
        if (!readArgc(fp, &argc)) break;

        for (i = 0; i < argc; i++) {
            if (!readString(fp,&str)) break;
            if (i == 0) {
                if (strcasecmp(str, "multi") == 0) {
                    if (multi++) {
                        ERROR("Unexpected MULTI");
                        break;
                    }
                } else if (strcasecmp(str, "exec") == 0) {
                    if (--multi) {
                        ERROR("Unexpected EXEC");
                        break;
                    }
                }
            }
            zfree(str);
        }

        /* Stop if the loop did not finish */
        if (i < argc) {
            if (str) zfree(str);
            break;
        }
    }

    if (feof(fp) && multi && strlen(error) == 0) {
        ERROR("Reached EOF before reading EXEC for MULTI");
    }
    if (strlen(error) > 0) {
        printf("%s\n", error);
    }
    return pos;
}

void usage() {
    printf(
        "Usage: redis-aof-tools {check|fix|format|scan} ...\n\n"
        "[check aof]:\n"
        "redis-aof-tools check -f aof_filename\n\n"
        "[fix aof]:\n"
        "redis-aof-tools fix -f aof_filename\n\n"
        "[format aof]:\n"
        "redis-aof-tools format -f aof_filename\n\n"
        "[scan aof]:\n"
        "redis-aof-tools scan -f aof_filename --key key1\n"
        "redis-aof-tools scan -f aof_filename --key key1 --sub item1\n"
        "--key specify the key name to scan\n"
        "--sub specify the sub item of the key to scan, "
        "ie. member of the list, hash, set\n\n"
        );
}

int checkFirstArg(const char *first_arg) {
    if (!strcmp(first_arg, "check")) {
        config.check_aof = 1;
    } else if (!strcmp(first_arg, "fix")) {
        config.fix_aof = 1;
    } else if (!strcmp(first_arg, "format")) {
        config.format_aof = 1;
    } else if (!strcmp(first_arg, "scan")) {
        config.scan_aof = 1;
    } else if (!strcmp(first_arg, "--help") ||
               !strcmp(first_arg, "-h")) {
        usage();
        exit(0);
    } else {
        return 0;
    }

    return 1;
}

/* Returns number of consumed options. */
int parseOptions(int argc, const char **argv) {
    int i = 1;
    int lastarg;
    int exit_status = 1;

    if (argc < 2 || !checkFirstArg(argv[1])) {
        goto invalid;
    }

    for (i = 2; i < argc; i++) {
        lastarg = (i == (argc-1));

        if (!strcmp(argv[i],"-f")) {
            if (lastarg) goto invalid;
            config.aof_filename = zstrdup(argv[++i]);
        } else if (!strcmp(argv[i],"--key")) {
            if (lastarg) goto invalid;
            config.scan_key = zstrdup(argv[++i]);
        } else if (!strcmp(argv[i],"--sub")) {
            if (lastarg) goto invalid;
            config.scan_sub = zstrdup(argv[++i]);
        } else if (!strcmp(argv[i],"--help") ||
                   !strcmp(argv[i],"-h")) {
            exit_status = 0;
            goto usage;
        } else {
            goto invalid;
        }
    }

    return i;

invalid:
    if (i == 1) {
        printf("Invalid first option: %s\n\n", (argc > 1 ? argv[1] : ""));
    } else {
        printf("Invalid option \"%s\" or option argument missing\n\n",argv[i]);
    }

usage:
    usage();
    exit(exit_status);
}

FILE *validateFile(struct redis_stat *sb) {
    if (!config.aof_filename) {
        printf("missing aof filename, use -f option\n\n");
        usage();
        return NULL;
    }

    FILE *fp = NULL;
    if (config.fix_aof) {
        fp = fopen(config.aof_filename,"r+");
    } else {
        fp = fopen(config.aof_filename,"r");
    }
    if (fp == NULL) {
        printf("Cannot open file: %s\n", config.aof_filename);
        return NULL;
    }

    if (redis_fstat(fileno(fp),sb) == -1) {
        printf("Cannot stat file: %s\n", config.aof_filename);
        return NULL;
    }

    off_t size = sb->st_size;
    if (size == 0) {
        printf("Empty file: %s\n", config.aof_filename);
        return NULL;
    }

    return fp;
}

void checkOrFixAof() {
    off_t size;
    struct redis_stat sb;
    FILE *fp = validateFile(&sb);
    if (!fp) exit(1);
    size = sb.st_size;

    off_t pos = process(fp);
    off_t diff = size-pos;
    printf("AOF analyzed: size=%lld, ok_up_to=%lld, diff=%lld\n",
        (long long) size, (long long) pos, (long long) diff);
    if (diff > 0) {
        if (config.fix_aof) {
            char buf[2];
            printf("This will shrink the AOF from %lld bytes, "
                   "with %lld bytes, to %lld bytes\n",
                   (long long)size,(long long)diff,(long long)pos);
            printf("Continue? [y/N]: ");
            if (fgets(buf,sizeof(buf),stdin) == NULL ||
                strncasecmp(buf,"y",1) != 0) {
                    printf("Aborting...\n");
                    exit(1);
            }
            if (ftruncate(fileno(fp), pos) == -1) {
                printf("Failed to truncate AOF\n");
                exit(1);
            } else {
                printf("Successfully truncated AOF\n");
            }
        } else {
            printf("AOF is not valid\n");
            exit(1);
        }
    } else {
        printf("AOF is valid\n");
    }

    fclose(fp);
}

void formatOrScanAof() {
    struct redis_stat sb;
    FILE *fp = validateFile(&sb);
    if (!fp) exit(1);

    if (config.scan_aof && !config.scan_key) {
        printf("missing key name, use --key option.\n");
        usage();
        exit(1);
    }

    struct argss *fakeClient = (struct argss *)zmalloc(sizeof(struct argss));
    char opinfo_buf[2048] = {'\0'};

    while(1) {
        int argc, j;
        unsigned long len;
        sds *argv;
        char buf[128];
        sds argsds;
        int key_match = 0;

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

        argv = zmalloc(sizeof(sds)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                fakeClient->argc = j; /* Free up to j-1. */
                goto readerr;
            }
            if (buf[0] != '$') goto fmterr;
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(NULL,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* Free up to j-1. */
                goto readerr;
            }
            argv[j] = argsds;
            if (!key_match && config.scan_aof && j >= 1 &&
                !strcmp(argv[1], config.scan_key) &&
                (!config.scan_sub ||
                 (j > 1 && !strcmp(argv[j], config.scan_sub)))) {
                key_match = 1;
            }
            if (fread(buf,2,1,fp) == 0) {
                fakeClient->argc = j+1; /* Free up to j. */
                goto readerr; /* discard CRLF */
            }
        }

        if (!strcasecmp("opinfo", argv[0])) {
            redisOplogHeader *loaded_header = (redisOplogHeader *)argv[1];

            struct tm *tm;
            char buf[128] = {'\0'};
            time_t time = (time_t)loaded_header->timestamp;
            tm = localtime(&time);
            strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", tm);

            sprintf(opinfo_buf, "/* version: %d  server_id: %ld  timestamp: %d  time: %s"
                    "  opid: %ld  src_opid: %ld  dbid: %d  del_by_expire: %d  "
                    "del_by_eviction: %d  cmd_num: %d */\n",
                    loaded_header->version, loaded_header->server_id,
                    loaded_header->timestamp, buf,
                    loaded_header->opid,
                    loaded_header->src_opid, loaded_header->dbid,
                    (loaded_header->cmd_flag & REDIS_OPLOG_DEL_BY_EXPIRE_FLAG \
                     ? 1 : 0),
                    (loaded_header->cmd_flag & REDIS_OPLOG_DEL_BY_EVICTION_FLAG \
                     ? 1 : 0),
                    loaded_header->cmd_num);
            if (config.format_aof) printf("%s", opinfo_buf);
        } else {
            if (config.format_aof || (config.scan_aof && key_match)) {
                if (key_match) {
                    printf("%s", opinfo_buf);
                }
                for(int i = 0; i < argc; i++) {
                    printf("%s\n", argv[i]);
                }
                if (key_match) {
                    printf("\n------\n\n");
                }
            }
        }

        for(int i = 0; i < fakeClient->argc; i++)
            sdsfree(fakeClient->argv[i]);
    }

    return;

readerr:
    if (!feof(fp)) {
        fprintf(stderr,
                "Unrecoverable error reading the append only file: %s",
                strerror(errno));
        exit(1);
    }

fmterr: /* Format error. */
    fprintf(stderr,"Bad file format reading the append only file: "
            "make a backup of your AOF file, then use "
            "./redis-check-aof --fix <filename>");
    exit(1);

}

int main(int argc, const char **argv) {
    parseOptions(argc,argv);

    if (config.fix_aof || config.check_aof) {
        checkOrFixAof();
    } else if (config.format_aof || config.scan_aof) {
        formatOrScanAof();
    }

    return 0;
}
