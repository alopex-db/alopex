"""Diagnostic CPU/I/O boundaries outside the original public commit timer."""
import math
import resource

from dataframe_phase_observer import counters

FIELDS=('ru_utime','ru_stime','ru_nvcsw','ru_nivcsw')


def observe(action,measure,*,snapshot=counters,usage=resource.getrusage):
    before=snapshot();cpu_before=usage(resource.RUSAGE_SELF)
    result=measure(action)
    cpu_after=usage(resource.RUSAGE_SELF);after=snapshot()
    if before['waited_children']!=after['waited_children']:
        raise RuntimeError('commit child accounting changed')
    delta={name:getattr(cpu_after,name)-getattr(cpu_before,name) for name in FIELDS}
    delta.update({key:after['io'][key]-before['io'][key] for key in before['io']})
    if not all(type(v) in (int,float) and math.isfinite(v) and v>=0 for v in delta.values()):
        raise RuntimeError('commit counter decreased or invalid')
    return result,dict(before=dict(before,cpu={k:getattr(cpu_before,k) for k in FIELDS}),
        after=dict(after,cpu={k:getattr(cpu_after,k) for k in FIELDS}),delta=delta,
        timer_nanoseconds=result[1],scope='pending public commit only; observers outside original timer; CPU includes timer-call overhead',
        io_scope='physical byte counters; cancelled writes separate; not syscall duration')
