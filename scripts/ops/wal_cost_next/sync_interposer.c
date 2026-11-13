#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stddef.h>
#include <time.h>

/* Diagnostic only. Never preload into the frozen acceptance comparator. */
#define RECORD_LIMIT 128u

struct sync_record {
    uint64_t duration_ns;
    int32_t result;
    int32_t error_number;
    uint8_t operation; /* 1=fsync, 2=fdatasync */
    uint8_t reserved[7];
};

static int (*real_fsync)(int);
static int (*real_fdatasync)(int);
static struct sync_record records[RECORD_LIMIT];
static size_t record_count;
static size_t record_overflow;
static pthread_mutex_t record_lock = PTHREAD_MUTEX_INITIALIZER;

__attribute__((constructor)) static void resolve_symbols(void) {
    real_fsync = (int (*)(int))dlsym(RTLD_NEXT, "fsync");
    real_fdatasync = (int (*)(int))dlsym(RTLD_NEXT, "fdatasync");
}

int alopex_sync_smoke_ready(void) {
    return real_fsync != NULL && real_fdatasync != NULL;
}

static uint64_t nanoseconds(const struct timespec *start, const struct timespec *end) {
    if (end->tv_sec < start->tv_sec ||
        (end->tv_sec == start->tv_sec && end->tv_nsec < start->tv_nsec)) return UINT64_MAX;
    return (uint64_t)(end->tv_sec - start->tv_sec) * 1000000000u +
           (uint64_t)(end->tv_nsec - start->tv_nsec);
}

static void save_record(uint8_t operation, int result, int error_number,
                        const struct timespec *start, const struct timespec *end) {
    pthread_mutex_lock(&record_lock);
    if (record_count < RECORD_LIMIT) {
        struct sync_record *record = &records[record_count++];
        record->duration_ns = nanoseconds(start, end);
        record->result = result;
        record->error_number = error_number;
        record->operation = operation;
    } else {
        record_overflow++;
    }
    pthread_mutex_unlock(&record_lock);
}

/* The real function is called exactly once. Recording happens only after return. */
static int observed_sync(int fd, int (*real_call)(int), uint8_t operation) {
    if (real_call == NULL) { errno = ENOSYS; return -1; }
    int before_errno = errno;
    struct timespec start = {0}, end = {0};
    int start_ok = clock_gettime(CLOCK_MONOTONIC, &start);
    errno = before_errno;
    int result = real_call(fd);
    int call_errno = errno;
    int end_ok = clock_gettime(CLOCK_MONOTONIC, &end);
    save_record(operation, result, call_errno,
                start_ok == 0 && end_ok == 0 ? &start : &(struct timespec){.tv_sec=1},
                start_ok == 0 && end_ok == 0 ? &end : &(struct timespec){0});
    errno = call_errno;
    return result;
}

int fsync(int fd) { return observed_sync(fd, real_fsync, 1); }
int fdatasync(int fd) { return observed_sync(fd, real_fdatasync, 2); }

/* Caller supplies 128 slots. No paths, descriptors, PIDs, or stdout emission. */
int alopex_sync_smoke_snapshot(struct sync_record *out, size_t capacity,
                               size_t *count, size_t *overflow) {
    if (out == NULL || count == NULL || overflow == NULL || capacity < RECORD_LIMIT) return -1;
    pthread_mutex_lock(&record_lock);
    for (size_t i = 0; i < record_count; i++) out[i] = records[i];
    *count = record_count;
    *overflow = record_overflow;
    pthread_mutex_unlock(&record_lock);
    return 0;
}
