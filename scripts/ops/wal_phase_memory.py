"""Manual-only memory boundaries; observations are not performance acceptance."""
import ast
import copy
import hashlib
import json
import re
import time

from wal_commit_diagnostic import memory, snapshot

PREFIX = ('after_wrapper_import', 'after_identity', 'after_wrapper_native_digest')
CHILD = ('after_child_import', 'after_child_native_digest', 'after_options',
         'after_prepare', 'after_open', 'after_schema_index', 'after_prior_commit',
         'after_begin', 'after_stage', 'before_pending_commit', 'after_pending_commit')
PHASES = PREFIX + CHILD
SCHEMA = 'alopex.wal-phase-memory/v1'
MAX_RAW_BYTES = 64 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


class Observer:
    def __init__(self, *, read=snapshot, clock=time.monotonic_ns):
        self.read = read
        self.clock = clock
        self.rows = []

    def capture(self, phase):
        index = len(self.rows)
        require(index < len(PHASES) and phase == PHASES[index], 'phase order differs')
        start = self.clock()
        raw = self.read()
        finish = self.clock()
        require(type(start) is int and type(finish) is int and 0 <= start <= finish,
                'invalid observation clock')
        require(not self.rows or self.rows[-1]['finished_ns'] <= start, 'observation clock decreased')
        require(type(raw) is dict and set(raw) == {'status', 'smaps_rollup'} and
                all(type(value) is str for value in raw.values()), 'raw memory shape differs')
        require(sum(len(value.encode()) for value in raw.values()) <= MAX_RAW_BYTES,
                'memory record exceeds bound')
        self.rows.append(dict(phase=phase, started_ns=start, finished_ns=finish, raw=raw))

    def report(self):
        require(tuple(row['phase'] for row in self.rows) == PHASES, 'memory phases incomplete')
        pids = [re.findall(r'^Pid:\s*([0-9]+)\s*$', row['raw']['status'], re.M) for row in self.rows]
        require(all(len(value) == 1 and int(value[0]) > 0 for value in pids) and
                len({value[0] for value in pids}) == 1, 'memory process identity differs')
        rows = [dict(row, memory=memory(row['raw'])) for row in self.rows]
        transitions = []
        for previous, current in zip(rows, rows[1:]):
            transitions.append(dict(before=previous['phase'], after=current['phase'],
                delta_bytes={group: {key: current['memory'][group][key] - value
                                    for key, value in fields.items()}
                             for group, fields in previous['memory'].items()}))
        return dict(schema=SCHEMA, rows=rows, transitions=transitions,
                    performance_acceptance=False,
                    scope='Process boundary observations including observer allocations and cache effects; not allocation attribution, interval peak or production timing',
                    units='status RSS is approximate; smaps Anonymous is RSS; Pss fields are proportional memory',
                    no_high_water_reset=True)

    def save(self, destination):
        value = self.report()
        with destination.open('x') as stream:
            json.dump(value, stream, allow_nan=False)
            stream.write('\n')
        destination.chmod(0o600)


def validate_report(value):
    require(type(value) is dict and type(value.get('rows')) is list and
            len(value['rows']) == len(PHASES), 'report phases incomplete')
    observer = Observer()
    for row in value['rows']:
        require(type(row) is dict and set(row) == {'phase', 'started_ns', 'finished_ns', 'raw', 'memory'},
                'report row shape differs')
        times = iter((row['started_ns'], row['finished_ns']))
        observer.clock = lambda: next(times)
        observer.read = lambda: row['raw']
        observer.capture(row['phase'])
    require(observer.report() == value, 'report differs from raw observations')
    return value


def instrument(source):
    """Insert calls only between original statements, outside original timers."""
    tree = ast.parse(source)
    original = copy.deepcopy(tree)
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'child']
    require(len(functions) == 1, 'one workload child required')
    child = functions[0]
    require(not any(isinstance(node, ast.Name) and node.id == '_wal_phase_capture'
                    for node in ast.walk(tree)), 'observer name already used')

    def shape(code):
        return ast.dump(ast.parse(code).body[0], include_attributes=False)

    def insert_after(nodes, code, phase, *, guarded=False):
        matches = [i for i, node in enumerate(nodes) if ast.dump(node, include_attributes=False) == shape(code)]
        require(len(matches) == 1, 'workload boundary missing or ambiguous: ' + phase)
        call = ast.parse(f'_wal_phase_capture({phase!r})').body[0]
        if guarded:
            call = ast.If(test=ast.Compare(left=ast.Name(id='phase', ctx=ast.Load()),
                          ops=[ast.Eq()], comparators=[ast.Constant(value='write')]), body=[call], orelse=[])
        nodes.insert(matches[0] + 1, call)

    insert_after(child.body, 'from alopex import Database, TxnMode, WalOverflowOptions, _alopex', 'after_child_import', guarded=True)
    insert_after(child.body, 'require(digest(_alopex.__file__) == expected_native, "wrong native extension")', 'after_child_native_digest', guarded=True)
    insert_after(child.body, 'options = WalOverflowOptions(**OPTIONS)', 'after_options', guarded=True)
    writers = [node for node in child.body if isinstance(node, ast.If) and
               ast.dump(node.test) == ast.dump(ast.parse('phase == "write"', mode='eval').body) and
               any(isinstance(x, ast.Assign) for x in node.body)]
    require(len(writers) == 1, 'one writer block required')
    body = writers[0].body
    for statement, phase in (
        ('record["prepare_ns"] = time.perf_counter_ns() - start', 'after_prepare'),
        ('db, record["open_ns"] = measure(lambda: Database.open(str(path), wal_overflow=options))', 'after_open'),
        ('record["prior_ns"] = time.perf_counter_ns() - start', 'after_prior_commit'),
        ('tx, record["begin_ns"] = measure(lambda: db.begin(TxnMode.READ_WRITE))', 'after_begin'),
        ('record["stage_ns"] = time.perf_counter_ns() - start', 'after_stage'),
        ('_, record["commit_ns"] = measure(tx.commit)', 'after_pending_commit')):
        insert_after(body, statement, phase)
    schemas = [i for i, node in enumerate(body) if isinstance(node, ast.If) and
               ast.dump(node.test) == ast.dump(ast.parse('surface == "sql"', mode='eval').body)]
    require(len(schemas) == 1, 'one schema/index block required')
    body.insert(schemas[0] + 1, ast.parse('_wal_phase_capture("after_schema_index")').body[0])
    commits = [i for i, node in enumerate(body) if ast.dump(node, include_attributes=False) ==
               shape('_, record["commit_ns"] = measure(tx.commit)')]
    require(len(commits) == 1, 'one pending commit required')
    body.insert(commits[0], ast.parse('_wal_phase_capture("before_pending_commit")').body[0])

    def observer_call(node):
        return isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == '_wal_phase_capture'

    class Strip(ast.NodeTransformer):
        def visit_If(self, node):
            if len(node.body) == 1 and observer_call(node.body[0]) and not node.orelse:
                return None
            return self.generic_visit(node)

        def visit_Expr(self, node):
            return None if observer_call(node) else self.generic_visit(node)

    restored = Strip().visit(copy.deepcopy(tree))
    require(ast.dump(restored, include_attributes=False) == ast.dump(original, include_attributes=False),
            'workload statements changed')
    ast.fix_missing_locations(tree)
    return ast.unparse(tree) + '\n', dict(original_sha256=hashlib.sha256(source.encode()).hexdigest(),
                                         original_ast_preserved=True, injected_phases=list(CHILD),
                                         performance_acceptance=False)
