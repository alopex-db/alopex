import unittest

from wal_commit_diagnostic import BEGIN, END, events, memory, observe, summarize_trace


def raw(anon=20, file=12, shared=0):
    return dict(status=f"VmRSS: {anon+file+shared} kB\nRssAnon: {anon} kB\n"
                       f"RssFile: {file} kB\nRssShmem: {shared} kB\n",
                smaps_rollup=f"Rss: {anon+file+shared} kB\nAnonymous: {anon} kB\n"
                             f"Pss_Anon: {anon} kB\nPss_File: {file//2} kB\nPss_Shmem: 0 kB\n")


def line(call, timestamp="10.000100", pid=7, duration="0.000010"):
    return f"{pid} {timestamp} {call} <{duration}>\n"


def marked(value, timestamp):
    escaped=value.decode().replace("\n", r"\n")
    return line(f'write(2</private/log>, "{escaped}", {len(value)}) = {len(value)}', timestamp)


def trace(middle=None):
    return (line('fsync(4</db.alopex.d/overflow/prior.tmp>) = 0', '9.000000') +
            marked(BEGIN, '10.000000') +
            (middle if middle is not None else
             line('write(4</db.alopex.d/overflow/pending.tmp>, "abc", 3) = 3') +
             line('fsync(4</db.alopex.d/overflow/pending.tmp>) = 0', '10.000200', duration='0.000040') +
             line('fsync(4</db.alopex.d/overflow>) = 0', '10.000300', duration='0.000020')) +
            line('write(5</db.alopex.d/lsm.wal>, "ref", 3) = 3', '10.000400') +
            line('fdatasync(5</db.alopex.d/lsm.wal>) = 0', '10.000500', duration='0.000030') +
            marked(END, '10.001000') +
            line('write(1</private/log>, "result", 6) = 6', '10.002000'))


class MemoryContract(unittest.TestCase):
    def test_rss_and_proportional_memory_are_distinct(self):
        parsed=memory(raw())
        self.assertEqual(parsed['status_bytes']['RssFile'],12288)
        self.assertEqual(parsed['smaps_rollup_bytes']['Pss_File'],6144)

    def test_missing_duplicate_wrong_unit_negative_or_inconsistent_rejected(self):
        for original,replacement in [('RssFile: 12 kB\n',''),('RssFile: 12 kB','RssFile: -12 kB'),
                                     ('RssFile: 12 kB','RssFile: 12 MB'),
                                     ('RssFile: 12 kB','RssFile: 12 kB\nRssFile: 12 kB'),
                                     ('VmRSS: 32 kB','VmRSS: 31 kB')]:
            with self.subTest(replacement=replacement),self.assertRaises(ValueError):
                value=raw();value['status']=value['status'].replace(original,replacement);memory(value)
        for original,replacement in [('Anonymous: 20 kB','Anonymous: 33 kB'),('Pss_File: 6 kB\n','')]:
            with self.subTest(replacement=replacement),self.assertRaises(ValueError):
                value=raw();value['smaps_rollup']=value['smaps_rollup'].replace(original,replacement);memory(value)

    def test_timer_surrounds_only_action_and_negative_rss_delta_is_valid(self):
        order=[];reads=iter([raw(),raw(16)])
        def read():order.append('memory');return next(reads)
        def write(fd,data):self.assertEqual(fd,2);order.append(data);return len(data)
        def action():order.append('action');return 'done'
        def timer(fn):order.append('start');result=fn();order.append('end');return result,42
        result,report=observe(action,timer,read_memory=read,write=write)
        self.assertEqual(order,['memory',BEGIN,'start','action','end',END,'memory'])
        self.assertEqual(result,('done',42))
        self.assertEqual(report['delta_bytes']['status_bytes']['RssAnon'],-4096)
        self.assertFalse(report['performance_acceptance'])

    def test_original_failure_is_not_converted_to_success(self):
        writes=[]
        def failure():raise OSError('commit EIO')
        def write(fd,data):writes.append(data);return len(data)
        with self.assertRaisesRegex(OSError,'commit EIO'):
            observe(failure,lambda fn:(fn(),1),read_memory=raw,write=write)
        self.assertEqual(writes,[BEGIN])

    def test_short_marker_stops_before_commit(self):
        with self.assertRaisesRegex(ValueError,'short begin'):
            observe(lambda:self.fail('commit invoked'),lambda fn:fn(),read_memory=raw,write=lambda fd,b:0)


class TraceContract(unittest.TestCase):
    def test_prior_and_result_writes_excluded_and_duration_units_exact(self):
        result=summarize_trace(trace(),'/db.alopex')
        self.assertEqual(len(result['events']),5)
        self.assertEqual(result['totals']['spool/fsync'],dict(count=1,duration_ns=40000))
        self.assertEqual(result['totals']['directory/fsync']['duration_ns'],20000)
        self.assertEqual(result['totals']['wal/fdatasync']['duration_ns'],30000)
        self.assertEqual(result['marker_interval_ns'],990000)
        self.assertFalse(result['performance_acceptance'])

    def test_interleaved_other_thread_resumed_sync_is_preserved(self):
        middle=('7 10.000100 fsync(4</db.alopex.d/overflow/pending.tmp> <unfinished ...>\n'+
                line('write(6</db.alopex.d/overflow/other.tmp>, "x", 1) = 1', '10.000150',pid=8)+
                '7 10.000250 <... fsync resumed>) = 0 <0.000150>\n')
        report=summarize_trace(trace(middle),'/db.alopex')
        self.assertEqual(report['totals']['spool/fsync']['duration_ns'],150000)
        self.assertEqual({e['pid'] for e in report['events']},{7,8})

    def test_incomplete_duplicate_or_wrong_process_markers_rejected(self):
        original=trace()
        variants=[original.replace(marked(END,'10.001000'),''),
                  original+marked(END,'10.003000'),
                  original.replace('7 10.001000','8 10.001000'),
                  original.replace(f') = {len(END)} <',') = 0 <')]
        for variant in variants:
            with self.subTest(variant=variant),self.assertRaises(ValueError):
                summarize_trace(variant,'/db.alopex')

    def test_missing_duration_or_clock_reversal_rejected(self):
        for variant in [trace().replace(' <0.000040>',''),
                        trace().replace('10.000500','9.999999')]:
            with self.assertRaises(ValueError):summarize_trace(variant,'/db.alopex')

    def test_truncated_or_mismatched_resumed_call_rejected(self):
        for value in ['7 10.000000 fsync(4 <unfinished ...>\n',
                      '7 10.000000 <... fsync resumed>) = 0 <0.000001>\n',
                      '7 10.000000 fsync(4 <unfinished ...>\n7 10.000001 <... write resumed>) = 0 <0.000001>\n']:
            with self.assertRaises(ValueError):events(value)

    def test_syscall_crossing_marker_is_not_silently_omitted(self):
        with self.assertRaisesRegex(ValueError,'crosses'):
            summarize_trace(trace().replace('<0.000030>','<0.001000>'),'/db.alopex')

    def test_error_short_write_unknown_target_or_missing_sync_rejected(self):
        for old,new in [(') = 3',') = 2'),(') = 0 <0.000040>',') = -1 EIO (I/O error) <0.000040>'),
                        ('overflow/pending.tmp','unknown'),('fdatasync(', 'fsync('),
                        ('write(4<','writev(4<')]:
            with self.subTest(new=new),self.assertRaises(ValueError):
                summarize_trace(trace().replace(old,new),'/db.alopex')

    def test_unknown_or_oversized_trace_rejected(self):
        for text in ['nonsense\n','x'*1048577]:
            with self.assertRaises(ValueError):events(text)


if __name__=='__main__':unittest.main()
