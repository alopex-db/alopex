"""Apply the fixed-version collector timer validity correction."""
import hashlib
from pathlib import Path

SOURCE_SHA256 = 'ae7ad2e5e7b509546949176644a84c5018f49fbd659a965e8e4e95506341d742'

def replace_once(text, old, new):
    assert text.count(old) == 1, 'Upstream timer source differs'
    return text.replace(old, new, 1)

def patch(raw):
    assert hashlib.sha256(raw).hexdigest() == SOURCE_SHA256, 'Unexpected upstream dispatcher'
    s=raw.decode()
    s=replace_once(s,'static int collector_timer_create (timer_t * ptimerid);', '''typedef struct
{
  timer_t id;
  int initialized;
} CollectorTimer;

static int collector_timer_create (CollectorTimer *timer);''')
    s=s.replace('static int collector_timer_settime (int period, timer_t timerid);', 'static int collector_timer_settime (int period, CollectorTimer *timer);')
    s=s.replace('static int collector_timer_gettime (timer_t timerid);', 'static int collector_timer_gettime (CollectorTimer *timer);\nstatic int collector_timer_delete (CollectorTimer *timer);')
    s=s.replace('static timer_t collector_master_thread_timerid = NULL;', 'static CollectorTimer collector_master_thread_timer = {0};')
    s=s.replace('sizeof (timer_t), NULL, NULL', 'sizeof (CollectorTimer), NULL, NULL')
    s=s.replace('collector_master_thread_timerid', 'collector_master_thread_timer')
    s=s.replace('collector_master_thread_timer == NULL', '!collector_master_thread_timer.initialized')
    s=s.replace('collector_master_thread_timer != NULL', 'collector_master_thread_timer.initialized')
    s=s.replace('timer_t *timeridptr =', 'CollectorTimer *timeridptr =').replace('timer_t * timeridptr =', 'CollectorTimer * timeridptr =')
    s=s.replace('collector_timer_gettime (collector_master_thread_timer)', 'collector_timer_gettime (&collector_master_thread_timer)')
    s=s.replace('collector_timer_settime (itimer_period_requested, collector_master_thread_timer)', 'collector_timer_settime (itimer_period_requested, &collector_master_thread_timer)')
    s=s.replace('collector_timer_settime (period, collector_master_thread_timer)', 'collector_timer_settime (period, &collector_master_thread_timer)')
    s=s.replace('collector_timer_settime (0, collector_master_thread_timer)', 'collector_timer_settime (0, &collector_master_thread_timer)')
    s=replace_once(s,'\t    collector_master_thread_timer);','\t    collector_master_thread_timer.id);')
    s=s.replace('timeridptr != NULL && *timeridptr != NULL', 'timeridptr != NULL && timeridptr->initialized')
    s=s.replace('if (*timeridptr == NULL)', 'if (!timeridptr->initialized)')
    s=s.replace('collector_timer_settime (0, *timeridptr)', 'collector_timer_settime (0, timeridptr)')
    s=s.replace('collector_timer_settime (itimer_period_requested, *timeridptr)', 'collector_timer_settime (itimer_period_requested, timeridptr)')
    old='''      (void) CALL_REAL (timer_delete)(collector_master_thread_timer);
      collector_master_thread_timer = NULL;'''
    assert s.count(old)==2
    # Teardown may follow fork (POSIX timers are not inherited). The retiring
    # dispatcher forgets its state after attempting deletion.
    s=s.replace(old,'''      (void) collector_timer_delete (&collector_master_thread_timer);
      collector_master_thread_timer.initialized = 0;''')
    s=replace_once(s,'collector_timer_gettime (timer_t timerid)\n{','collector_timer_gettime (CollectorTimer *timer)\n{')
    s=replace_once(s,'if (timerid == NULL)\n    return (0);', 'if (!timer->initialized)\n    return (0);')
    s=replace_once(s,'CALL_REAL (timer_gettime)(timerid, &itimer)', 'CALL_REAL (timer_gettime)(timer->id, &itimer)')
    s=replace_once(s,'collector_timer_create (timer_t * ptimerid)\n{', 'collector_timer_create (CollectorTimer *timer)\n{')
    s=replace_once(s,'  struct sigevent sigev;\n  if (NULL_PTR (timer_create))', '''  struct sigevent sigev;
  timer_t created;
  /* Reuse the live timer; its opaque identifier can legitimately be zero. */
  if (timer->initialized)
    return 0;
  if (NULL_PTR (timer_create))''')
    s=replace_once(s,'sigev.sigev_value.sival_ptr = ptimerid;', 'sigev.sigev_value.sival_ptr = timer;')
    s=replace_once(s,'CALL_REAL (timer_create)(CLOCK_THREAD_CPUTIME_ID, &sigev, ptimerid)', 'CALL_REAL (timer_create)(CLOCK_THREAD_CPUTIME_ID, &sigev, &created)')
    marker='''  return 0;
}

static int
collector_timer_settime (int period, timer_t timerid)'''
    s=replace_once(s,marker,'''  timer->id = created;
  timer->initialized = 1;
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

static int
collector_timer_settime (int period, CollectorTimer *timer)''')
    s=replace_once(s,'  struct itimerspec itimer;\n  if (NULL_PTR (timer_settime))', '''  struct itimerspec itimer;
  if (!timer->initialized)
    {
      errno = EINVAL;
      return -1;
    }
  if (NULL_PTR (timer_settime))''')
    s=replace_once(s,'CALL_REAL (timer_settime)(timerid, 0, &itimer, NULL)', 'CALL_REAL (timer_settime)(timer->id, 0, &itimer, NULL)')
    s=replace_once(s,'  timer_t timerid = NULL;', '  CollectorTimer timer = {0};')
    s=replace_once(s,'''      collector_timer_create (timeridptr);
      if (*timeridptr != NULL)
\tcollector_timer_settime (itimer_period_requested, timeridptr);
      timerid = *timeridptr;''', '''      if (collector_timer_create (timeridptr) == 0)
\tcollector_timer_settime (itimer_period_requested, timeridptr);
      timer = *timeridptr;''')
    s=replace_once(s,'''  if (timerid != NULL)
    CALL_REAL (timer_delete)(timerid);''', '''  (void) collector_timer_delete (&timer);''')
    assert 'timerid == NULL' not in s and '*timeridptr != NULL' not in s and '*timeridptr == NULL' not in s
    return s.encode()

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('source',type=Path);args=parser.parse_args()
    args.source.write_bytes(patch(args.source.read_bytes()))
