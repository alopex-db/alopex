
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
typedef void *test_timer_id;
typedef struct { test_timer_id id; int initialized; } CollectorTimer;
struct timespec { long tv_sec; long tv_nsec; };
struct itimerspec { struct timespec it_interval, it_value; };
struct sigevent { int sigev_notify, sigev_signo; union { void *sival_ptr; } sigev_value; struct { int _tid; } _sigev_un; };
typedef long time_t;
#define NANOSEC 1000000000L
#define CLOCK_THREAD_CPUTIME_ID 3
#define SIGEV_THREAD_ID 4
#define SIGEV_SIGNAL 0
#define SIGPROF 27
#define TprintfT(...) ((void)0)
#define NULL_PTR(name) 0
#define CALL_REAL(name) fake_##name
static int created,armed,read_count,deleted,fail_create,fail_set,fail_get,fail_delete;
static test_timer_id next_id,last_id;
static CollectorTimer local;
static int itimer_period_requested=10000,dispatcher_key=1;
static int init_interposition_intf(void){return 0;}
static int __collector_gettid(void){return 1;}
static void *__collector_tsd_get_by_key(int key){assert(key==dispatcher_key);return &local;}
static int fake_timer_create(int clock,struct sigevent *event,test_timer_id *id){
 assert(clock==CLOCK_THREAD_CPUTIME_ID && event->sigev_notify==SIGEV_THREAD_ID);
 ++created;if(fail_create){errno=EAGAIN;return -1;}*id=next_id;return 0;
}
static int fake_timer_settime(test_timer_id id,int flags,const struct itimerspec *value,void *old){
 (void)flags;(void)old;assert(value->it_interval.tv_nsec==0 || value->it_interval.tv_nsec==10000000);
 ++armed;last_id=id;return fail_set?-1:0;
}
static int fake_timer_gettime(test_timer_id id,struct itimerspec *value){
 ++read_count;last_id=id;value->it_interval.tv_sec=0;value->it_interval.tv_nsec=10000000;return fail_get?-1:0;
}
static int fake_timer_delete(test_timer_id id){++deleted;last_id=id;return fail_delete?-1:0;}
static int collector_timer_create(CollectorTimer *);
static int collector_timer_gettime(CollectorTimer *);
static int collector_timer_settime(int,CollectorTimer *);
static int collector_timer_delete(CollectorTimer *);
static int
collector_timer_create (CollectorTimer *timer)
{
  struct sigevent sigev;
  test_timer_id created;
  /* Reuse the live timer; its opaque identifier can legitimately be zero. */
  if (timer->initialized)
    return 0;
  if (NULL_PTR (timer_create))
    init_interposition_intf ();
  TprintfT (DBG_LT2, "collector_timer_settime(): timer_create is %p\n", __real_timer_create);
  sigev.sigev_notify = SIGEV_THREAD_ID | SIGEV_SIGNAL;
  sigev.sigev_signo = SIGPROF;
  sigev.sigev_value.sival_ptr = timer;
#if !defined(__MUSL_LIBC)
  sigev._sigev_un._tid = __collector_gettid ();
#endif
  if (CALL_REAL (timer_create)(CLOCK_THREAD_CPUTIME_ID, &sigev, &created) == -1)
    {
      TprintfT (DBG_LT2, "collector_timer_settime() failed! errno=%d\n", errno);
      return -1;
    }
  timer->id = created;
  timer->initialized = 1;
  return 0;
}

static int
collector_timer_gettime (CollectorTimer *timer)
{
  int timer_period;
  struct itimerspec itimer;
  if (!timer->initialized)
    return (0); // timer was not initialized
  if (CALL_REAL (timer_gettime)(timer->id, &itimer) == -1)
    {
      /* this should never reasonably fail, so not worth logging */
      TprintfT (DBG_LT1, "WARNING: timer_gettime failed: errno=%d\n", errno);
      return (-1);
    }
  timer_period = ((itimer.it_interval.tv_sec * NANOSEC) +
		  itimer.it_interval.tv_nsec) / 1000;
  TprintfT (DBG_LT2, "collector_timer_gettime (period=%d)\n", timer_period);
  return (timer_period);
}

static int
collector_timer_settime (int period, CollectorTimer *timer)
{
  struct itimerspec itimer;
  if (!timer->initialized)
    {
      errno = EINVAL;
      return -1;
    }
  if (NULL_PTR (timer_settime))
    init_interposition_intf ();
  TprintfT (DBG_LT2, "collector_timer_settime(period=%d)\n", period);
  time_t NPM = 1000;
  itimer.it_interval.tv_sec = NPM * period / NANOSEC;
  itimer.it_interval.tv_nsec = (NPM * period) % NANOSEC;
  itimer.it_value = itimer.it_interval;
  if (CALL_REAL (timer_settime)(timer->id, 0, &itimer, NULL) == -1)
    {
      TprintfT (DBG_LT2, "collector_timer_settime(%d) failed! errno=%d\n", period, errno);
      return -1;
    }
  return 0;
}

static int
collector_timer_delete (CollectorTimer *timer)
{
  if (!timer->initialized)
    return 0;
  if (CALL_REAL (timer_delete)(timer->id) == -1)
    return -1;
  timer->initialized = 0;
  return 0;
}

void
__collector_ext_dispatcher_thread_timer_suspend ()
{
  CollectorTimer * timeridptr = __collector_tsd_get_by_key (dispatcher_key);
  if (timeridptr != NULL && timeridptr->initialized)
    (void) collector_timer_settime (0, timeridptr);
  return;
}

int
__collector_ext_dispatcher_thread_timer_resume ()
{
  CollectorTimer * timeridptr = __collector_tsd_get_by_key (dispatcher_key);
  if (timeridptr == NULL)
    return -1;
  if (!timeridptr->initialized)
    { // timer id not initialized yet
      TprintfT (DBG_LT2, "__collector_ext_dispatcher_thread_timer_resume: timer not initialized yet, create it\n");
      if (collector_timer_create (timeridptr) == -1)
	{
	  TprintfT (0, "__collector_ext_dispatcher_thread_timer_resume(): WARNING: No timer created\n");
	  return -1;
	}
    }
  return collector_timer_settime (itimer_period_requested, timeridptr);
}

int main(void){
 CollectorTimer timer={0};
 /* No syscall is made for absent timers; setting them is rejected. */
 assert(collector_timer_gettime(&timer)==0 && read_count==0);
 assert(collector_timer_delete(&timer)==0 && deleted==0);
 assert(collector_timer_settime(10000,&timer)==-1 && armed==0 && errno==EINVAL);
 /* Creation failure never creates a valid timer. */
 fail_create=1;assert(collector_timer_create(&timer)==-1 && !timer.initialized);fail_create=0;
 /* Opaque ID zero is a real timer and is reused. */
 next_id=0;assert(collector_timer_create(&timer)==0 && timer.initialized && timer.id==0);
 assert(collector_timer_create(&timer)==0 && created==2);
 assert(collector_timer_gettime(&timer)==10000 && read_count==1 && last_id==0);
 assert(collector_timer_settime(10000,&timer)==0 && armed==1 && last_id==0);
 /* Real per-thread suspend/resume functions preserve timer zero. */
 local=timer;__collector_ext_dispatcher_thread_timer_suspend();
 assert(armed==2 && local.initialized && last_id==0);
 assert(__collector_ext_dispatcher_thread_timer_resume()==0 && armed==3 && created==2);
 /* Kernel failures propagate; failed deletion keeps ownership. */
 fail_get=1;assert(collector_timer_gettime(&timer)==-1);fail_get=0;
 fail_set=1;assert(collector_timer_settime(10000,&timer)==-1);fail_set=0;
 fail_delete=1;assert(collector_timer_delete(&timer)==-1 && timer.initialized);fail_delete=0;
 assert(collector_timer_delete(&timer)==0 && !timer.initialized && last_id==0);
 assert(collector_timer_delete(&timer)==0 && deleted==2);
 /* Resume of an uninitialized timer handles failure then nonzero success. */
 local.initialized=0;fail_create=1;
 assert(__collector_ext_dispatcher_thread_timer_resume()==-1 && !local.initialized);
 fail_create=0;next_id=(test_timer_id)(uintptr_t)7;
 assert(__collector_ext_dispatcher_thread_timer_resume()==0 && local.initialized && last_id==next_id);
 assert(collector_timer_delete(&local)==0 && !local.initialized);
 puts("timer validity, zero/nonzero IDs, failures, suspend/resume and deletion passed");
 return 0;
}
