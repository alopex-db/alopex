"""Private, fail-closed static admission for UC-684-S04-PROFILE-SEMANTICS.

This does not inspect a live host or qualify the selected job. Callers supply
trusted run, manifest, network and case-order identities separately.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
from typing import Any

SCHEMA_PATH = Path(__file__).with_suffix('.schema.json')
MAX_PROFILE_BYTES = 1024 * 1024
ALLOWED_KEYWORDS = {'$schema', '$id', '$defs', 'title', 'description', '$ref',
                    'type', 'required', 'properties', 'additionalProperties',
                    'const', 'enum', 'pattern', 'minimum', 'maximum',
                    'exclusiveMinimum', 'minLength', 'minItems', 'maxItems',
                    'items', 'prefixItems', 'oneOf', 'format'}
REQUIRED_ROOT_KINDS = {'workspace', 'worktrees', 'temp', 'cargo_build',
    'cargo_cache', 'copied_binaries', 'wheels_venvs', 'local_installations',
    'container_images', 'container_build_cache', 'container_volumes',
    'container_logs', 'recovery_reserve'}

class ProfileFailure(Exception):
    """Failure exposed to logs only as a fixed code."""

    def __init__(self, code: str = 'S04_PROFILE_INVALID') -> None:
        super().__init__(code)
        self.code = code


def _pairs_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ProfileFailure()
        result[key] = value
    return result


def _load_json(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs_no_duplicates,
                          parse_constant=lambda _: (_ for _ in ()).throw(ProfileFailure()))
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise ProfileFailure() from None


def _audit_schema(node: Any, root: dict[str, Any]) -> None:
    if not isinstance(node, dict) or set(node) - ALLOWED_KEYWORDS:
        raise ProfileFailure('S04_SCHEMA_UNSUPPORTED')
    if '$ref' in node:
        ref = node['$ref']
        if not isinstance(ref, str) or not ref.startswith('#/$defs/') or ref.split('/')[-1] not in root.get('$defs', {}):
            raise ProfileFailure('S04_SCHEMA_UNSUPPORTED')
    if node.get('format', 'date-time') != 'date-time':
        raise ProfileFailure('S04_SCHEMA_UNSUPPORTED')
    if 'additionalProperties' in node and node['additionalProperties'] is not False:
        raise ProfileFailure('S04_SCHEMA_UNSUPPORTED')
    for field in ('properties', '$defs'):
        for child in node.get(field, {}).values():
            _audit_schema(child, root)
    for field in ('items',):
        if field in node:
            if node[field] is not False:
                _audit_schema(node[field], root)
    for field in ('oneOf', 'prefixItems'):
        for child in node.get(field, []):
            _audit_schema(child, root)


def _matches(value: Any, schema: dict[str, Any], root: dict[str, Any]) -> bool:
    if '$ref' in schema:
        return _matches(value, root['$defs'][schema['$ref'].split('/')[-1]], root)
    if type(value) is float and not math.isfinite(value):
        return False
    kind = schema.get('type')
    kinds = {'object': lambda x: isinstance(x, dict), 'array': lambda x: isinstance(x, list),
             'string': lambda x: isinstance(x, str), 'integer': lambda x: type(x) is int,
             'number': lambda x: type(x) in (int, float), 'boolean': lambda x: type(x) is bool,
             'null': lambda x: x is None}
    if kind and (kind not in kinds or not kinds[kind](value)):
        return False
    if 'const' in schema and (type(value) is not type(schema['const']) or value != schema['const']):
        return False
    if 'enum' in schema and not any(type(value) is type(v) and value == v for v in schema['enum']):
        return False
    if isinstance(value, dict):
        props = schema.get('properties', {})
        if not set(schema.get('required', [])) <= set(value):
            return False
        if schema.get('additionalProperties') is False and set(value) - set(props):
            return False
        if any(not _matches(v, props[k], root) for k, v in value.items() if k in props):
            return False
    if isinstance(value, list):
        if len(value) < schema.get('minItems', 0) or len(value) > schema.get('maxItems', float('inf')):
            return False
        if 'prefixItems' in schema:
            if len(value) > len(schema['prefixItems']):
                return False
            if any(not _matches(v, s, root) for v, s in zip(value, schema['prefixItems'])):
                return False
        elif 'items' in schema and any(not _matches(v, schema['items'], root) for v in value):
            return False
    if isinstance(value, str):
        if len(value) < schema.get('minLength', 0):
            return False
        if 'pattern' in schema and re.fullmatch(schema['pattern'], value) is None:
            return False
        if schema.get('format') == 'date-time':
            try:
                parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
                if parsed.tzinfo is None or parsed.utcoffset() is None:
                    return False
            except ValueError:
                return False
    if type(value) in (int, float):
        if value < schema.get('minimum', -float('inf')) or value > schema.get('maximum', float('inf')):
            return False
        if 'exclusiveMinimum' in schema and value <= schema['exclusiveMinimum']:
            return False
    if 'oneOf' in schema and sum(_matches(value, s, root) for s in schema['oneOf']) != 1:
        return False
    return True


def _canonical_path(path: str) -> bool:
    return (path.startswith('/') and path == str(PurePosixPath(path))
            and path != '/' and all(part not in ('', '.', '..') for part in path.split('/')[1:])
            and '\\' not in path)


def _semantic(profile: dict[str, Any], *, source_sha: str, run_id: int,
              run_attempt: int, manifest_sha256: str, physical_host_id: str,
              approved_network_edges: set[tuple[Any, ...]],
              manifest_pairs: tuple[tuple[int, str, str, str, str], ...]) -> None:
    b, s, r = profile['binding'], profile['storage'], profile['runtime']
    if (b['source_sha'], b['run_id'], b['run_attempt'], b['physical_host_id']) != (source_sha, run_id, run_attempt, physical_host_id):
        raise ProfileFailure()
    if s['physical_host_id'] != physical_host_id or r['process_owner_uid'] != b['profile_owner_uid']:
        raise ProfileFailure()
    if profile['assets']['case_manifest_sha256'] != manifest_sha256:
        raise ProfileFailure()
    if profile['assets']['image_architecture'] != b['selected_architecture']:
        raise ProfileFailure()
    roots = s['roots']
    kinds = [x['kind'] for x in roots]
    if not REQUIRED_ROOT_KINDS <= set(kinds) or len(kinds) != len(set(kinds)):
        raise ProfileFailure()
    paths = [x['canonical_path'] for x in roots]
    extra_paths = [r['socket_path'], r['daemon_root'], profile['workload']['case_manifest_path']]
    if not all(map(_canonical_path, paths + extra_paths)):
        raise ProfileFailure()
    if any(a == b or a.startswith(b + '/') or b.startswith(a + '/') for i, a in enumerate(paths) for b in paths[i+1:]):
        raise ProfileFailure()
    if any(x['maximum_scan_age_seconds'] > s['sample_max_age_seconds'] or not x['includes_descendants'] for x in roots):
        raise ProfileFailure()
    if s['monitor_interval_seconds'] > s['sample_max_age_seconds']:
        raise ProfileFailure()
    reserves = sum(s[k] for k in ('collection_reserve_bytes', 'cleanup_reserve_bytes', 'recovery_reserve_bytes'))
    if (s['measured_baseline_bytes'] + s['planned_peak_extra_bytes'] + reserves + s['maximum_growth_between_samples_bytes'] >= s['stop_bytes']
            or s['planned_peak_extra_bytes'] + reserves > s['backing_free_bytes']
            or s['stop_bytes'] >= s['absolute_ceiling_bytes']):
        raise ProfileFailure()
    checks = profile['security']['checks']
    all_ids = [v for c in checks.values() for v in (c['positive_check_id'], c['negative_check_id'])]
    if len(all_ids) != len(set(all_ids)) or any(c['positive_check_id'] == c['negative_check_id'] or
            not _canonical_path(c['config_location']) or not _canonical_path(c['evidence_location']) for c in checks.values()):
        raise ProfileFailure()
    edges = profile['network']['edges']
    edge_tuples = [tuple(e[k] for k in ('source', 'destination', 'protocol', 'port', 'authentication', 'stage')) for e in edges]
    if len(edge_tuples) != len(set(edge_tuples)) or not set(edge_tuples) <= approved_network_edges:
        raise ProfileFailure()
    if any((e['protocol'] == 'unix') != (e['port'] is None) for e in edges):
        raise ProfileFailure()
    assets = profile['assets']['products']
    if [a['role'] for a in assets] != ['baseline', 'candidate'] or assets[0]['sha256'] == assets[1]['sha256'] or any(a['architecture'] != b['selected_architecture'] for a in assets):
        raise ProfileFailure()
    pairs = profile['workload']['pairs']
    if (len(manifest_pairs) != 27 or len({expected[1] for expected in manifest_pairs}) != 27
            or [p['order'] for p in pairs] != list(range(1, 28))):
        raise ProfileFailure()
    cell_ids = [cell['cell_id'] for p in pairs for cell in (p['baseline'], p['candidate'])]
    if len(set(cell_ids)) != 54:
        raise ProfileFailure()
    for p, expected in zip(pairs, manifest_pairs):
        a, c = p['baseline'], p['candidate']
        if (p['order'], p['pair_id'], a['case_id'], a['input_sha256'], a['parameters_sha256']) != expected:
            raise ProfileFailure()
        if a['arm'] != 'baseline' or c['arm'] != 'candidate' or a['asset_sha256'] != assets[0]['sha256'] or c['asset_sha256'] != assets[1]['sha256'] or a['case_id'] != c['case_id'] or a['input_sha256'] != c['input_sha256'] or a['parameters_sha256'] != c['parameters_sha256']:
            raise ProfileFailure()
    t = profile['time_budget']
    stages = t['stages']
    total = sum(x['conservative_seconds'] for x in stages.values())
    if (any(x['rate_evidence'][k] != b[k] for x in stages.values() for k in ('source_sha', 'run_id', 'run_attempt'))
            or any(x['conservative_seconds'] < x['expected_seconds'] for x in stages.values())
            or stages['pilot']['work_count'] <= 0
            or stages['pilot']['input_bytes'] <= 0
            or stages['measurement_54']['work_count'] != 54
            or total != t['conservative_total_seconds']
            or total + t['margin_seconds'] > t['job_limit_seconds']
            or t['job_limit_seconds'] != r['job_limit_seconds']
            or t['abort_if_projected_over_seconds'] > r['stop_before_seconds']
            or t['abort_if_projected_over_seconds'] > t['job_limit_seconds'] - t['margin_seconds']
            or r['stop_before_seconds'] > r['job_limit_seconds'] - t['margin_seconds']
            or max(x['peak_extra_bytes'] for x in stages.values()) > s['planned_peak_extra_bytes']):
        raise ProfileFailure()


def validate_profile(path: Path, *, source_sha: str, run_id: int, run_attempt: int,
                     manifest_sha256: str, physical_host_id: str,
                     approved_network_edges: set[tuple[Any, ...]],
                     manifest_pairs: tuple[tuple[int, str, str, str, str], ...]) -> dict[str, Any]:
    """Return a private parsed profile or raise ProfileFailure with a fixed code."""
    try:
        schema = _load_json(SCHEMA_PATH.read_bytes())
        _audit_schema(schema, schema)
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_size > MAX_PROFILE_BYTES):
                raise ProfileFailure()
            with os.fdopen(fd, 'rb', closefd=False) as profile_file:
                raw = profile_file.read(MAX_PROFILE_BYTES + 1)
                if len(raw) > MAX_PROFILE_BYTES:
                    raise ProfileFailure()
                profile = _load_json(raw)
        finally:
            os.close(fd)
        if not _matches(profile, schema, schema):
            raise ProfileFailure()
        if info.st_uid != profile['binding']['profile_owner_uid']:
            raise ProfileFailure()
        _semantic(profile, source_sha=source_sha, run_id=run_id,
                  run_attempt=run_attempt, manifest_sha256=manifest_sha256,
                  physical_host_id=physical_host_id,
                  approved_network_edges=approved_network_edges,
                  manifest_pairs=manifest_pairs)
        return profile
    except ProfileFailure:
        raise
    except (OSError, KeyError, TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ProfileFailure() from None
