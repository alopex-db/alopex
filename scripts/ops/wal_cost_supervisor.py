"""Bounded supervision that distinguishes terminal proc records from live gaps."""
import os
from pathlib import Path
import signal
import subprocess
import time

from common import atomic_json
from phase_rss import PhaseRssError, parse_status


def require(value,message):
    if not value:raise RuntimeError(message)


def sample(child):
    try:
        raw=Path(f'/proc/{child.pid}/status').read_text()
    except FileNotFoundError:
        raw=None
    if raw is not None:
        try:return parse_status(raw)['rss_bytes'],None
        except PhaseRssError:
            # Partial, malformed and inconsistent counters remain failures.
            fields=dict(line.split(':',1) for line in raw.splitlines() if ':' in line)
            if 'VmRSS' in fields or 'VmHWM' in fields:raise
            require(fields.get('Pid','').strip()==str(child.pid),'process identity differs')
    # Linux omits mm counters once the exiting task has released its mm.
    # Only an independently reaped exit permits a missing terminal sample.
    try:child.wait(timeout=.25)
    except subprocess.TimeoutExpired:raise RuntimeError('live process counters disappeared') from None
    return None,dict(status_record=raw,returncode=child.returncode,reaped=True)


def run_cell(command,env,directory,budget,timeout,rss_limit):
    budget.check();started=time.monotonic();peak=None;samples=0;terminal=[];failure=None
    with (directory/'worker.log').open('xb') as log:
        child=subprocess.Popen(command,stdout=log,stderr=log,env=env,start_new_session=True)
        try:
            while child.poll() is None:
                require(time.monotonic()-started<timeout,'cell deadline exceeded');budget.check()
                value,ended=sample(child)
                if ended is not None:terminal.append(ended)
                else:
                    samples+=1;peak=value if peak is None else max(peak,value)
                    require(value<=rss_limit,'live process RSS bound exceeded')
                try:child.wait(timeout=.25)
                except subprocess.TimeoutExpired:pass
            require(child.returncode==0,'worker failed')
            require(samples>0,'no live RSS observation')
        except BaseException as error:
            failure=dict(error=repr(error),detail=getattr(error,'detail',''))
            raise
        finally:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGTERM)
                try:child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)
            atomic_json(directory/'supervision.json',dict(returncode=child.returncode,reaped=child.poll() is not None,
                elapsed_seconds=time.monotonic()-started,sampled_peak_rss_bytes=peak,live_samples=samples,
                terminal_samples_excluded=terminal,failure=failure,
                sampled_peak_scope='supervisor samples across startup, identity checks, workload and result saving'))
