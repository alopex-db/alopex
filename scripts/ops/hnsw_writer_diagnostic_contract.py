"""Accept diagnostic counters only alongside the complete native SQL smoke."""
from hnsw_completion_smoke import verify as verify_completion

SCHEMA = 'alopex.hnsw-writer-diagnostic/v1'
ASSET_SCHEMA = 'alopex.hnsw-writer-diagnostic-assets/v1'
BUILD_SCHEMA = 'alopex.hnsw-writer-diagnostic-build/v1'
PATHS = ('crates/alopex-sql/src/storage/bridge.rs',
         'crates/alopex-sql/src/storage/bridge/manual_hnsw_writer_profile.rs',
         'crates/alopex-embedded/examples/issue681_hnsw_first_write.rs')


def verify(identity, report, events, mode, binary_sha256, runner_sha256):
    result = verify_completion(identity, report, events, mode, binary_sha256, runner_sha256)
    samples = {}
    for offset, phase in enumerate(('first_insert', 'second_insert')):
        rows = [event['data'] for event in events
                if event['phase'] == phase and event['data'].get('event') == 'end']
        if len(rows) != 1:
            raise ValueError('missing diagnostic INSERT')
        data = rows[0]
        value = data.get('writer_internal', {})
        if (set(value) != {'schema', 'fork_calls', 'fork_ns', 'stage_calls', 'stage_ns',
                          'scope', 'performance_qualification'}
                or value['schema'] != SCHEMA
                or value['scope'] != 'synchronous INSERT on the calling thread'
                or value['performance_qualification'] is not False):
            raise ValueError('diagnostic scope differs')
        if (any(type(value[field]) is not int or value[field] < 0
                for field in ('fork_calls', 'fork_ns', 'stage_calls', 'stage_ns'))
                or value['fork_calls'] != int(offset == 0) or value['stage_calls'] != 1
                or (offset == 1 and value['fork_ns'] != 0)
                or type(data.get('elapsed_ns')) is not int or data['elapsed_ns'] <= 0
                or value['fork_ns'] + value['stage_ns'] > data['elapsed_ns']):
            raise ValueError('diagnostic count or duration differs')
        samples[phase] = dict(value)
    result.update(writer_internal=samples, performance_qualification=False)
    return result
