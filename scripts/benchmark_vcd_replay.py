#!/usr/bin/env python3
"""Replay retained public waveform calls in fresh and reused parser sessions.

The input is read-only. Hashing primes the OS file cache before timing; "cold"
means a new parser, never disk-cold I/O. Output directories must not exist.
Use identical --wave/--calls inputs with --source-root selecting each checkout.
Optional --profile-parse adds only statement-boundary probes to a temporary
in-memory copy of _parse; those runs are diagnostic, not the timing comparison.
"""
from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import inspect
import json
import os
from pathlib import Path
import platform
import resource
import subprocess
import sys
import textwrap
import time


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def resources():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    status = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines())
    rss = int(status['VmRSS'].split()[0])
    io = dict(line.split(':', 1) for line in Path('/proc/self/io').read_text().splitlines())
    return dict(wall=time.perf_counter(), cpu=time.process_time(), rss_kib=rss,
                peak_rss_kib=int(status['VmHWM'].split()[0]), inherited_rusage_peak_kib=usage.ru_maxrss,
                read_bytes=int(io['read_bytes']),
                minor_faults=usage.ru_minflt, major_faults=usage.ru_majflt)


def instrument_parse(module, probes):
    """Probe read/split/materialization boundaries without per-event overhead."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(module.VCDParser._parse)))
    function = tree.body[0]
    def mark(label):
        return ast.Expr(ast.Call(ast.Name('_benchmark_probe', ast.Load()), [ast.Constant(label)], []))
    body = [mark('parse_enter')]
    saw_loop = False
    for statement in function.body:
        label = None
        if isinstance(statement, ast.With):
            label = 'file_read'
        elif isinstance(statement, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'tokens' for t in statement.targets):
            value = statement.value
            label = ('token_split' if isinstance(value, ast.Call)
                     and isinstance(value.func, ast.Attribute) and value.func.attr == 'split'
                     else 'token_iterator_setup')
        elif isinstance(statement, (ast.While, ast.For)):
            label = 'aliases' if saw_loop else 'event_materialization'
            saw_loop = True
        if label:
            body.append(mark(label + '_start'))
        body.append(statement)
        if label:
            body.append(mark(label + '_end'))
    body.append(mark('parse_body_end'))
    function.body = body
    tree = ast.fix_missing_locations(tree)
    namespace = dict(module.__dict__, _benchmark_probe=lambda label: probes.append(dict(label=label, **resources())))
    exec(compile(tree, '<benchmark-parse-probes>', 'exec'), namespace)
    module.VCDParser._parse = namespace['_parse']


def transition_fingerprint(parser):
    """Bound temporary serialization while checking every retained event."""
    digest = hashlib.sha256()
    count = 0
    for symbol, transitions in sorted(parser._transitions.items()):
        digest.update(json.dumps(symbol).encode())
        count += len(transitions)
        for offset in range(0, len(transitions), 65536):
            digest.update(json.dumps(transitions[offset:offset + 65536], separators=(',', ':')).encode())
    return dict(symbols=len(parser._transitions), paths=len(parser._path_to_sym),
                transitions=count, sha256=digest.hexdigest(),
                raw_tick_symbols=len(parser._raw_ticks), end_time_fs=parser._end_time_fs)


async def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    os.environ['TRACEWEAVE_CACHE_DIR'] = str(args.output/'cache')
    sys.path.insert(0, str(args.source_root.resolve()))
    import server
    from src import vcd_parser
    probes, metrics = [], []
    if args.profile_parse:
        instrument_parse(vcd_parser, probes)
    original_record = server.usage_telemetry.record_call
    def record(name, arguments, **kwargs):
        metrics.append(dict(tool=name, **kwargs.get('diagnostics', {})))
        original_record(name, arguments, **kwargs)
    server.usage_telemetry.record_call = record
    calls = json.loads(args.calls.read_text())
    if not calls:
        raise ValueError('At least one recorded call is required')
    for call in calls:
        call['arguments']['wave_path'] = str(args.wave.resolve())
    manifest = dict(wave=str(args.wave.resolve()), wave_bytes=args.wave.stat().st_size,
                    wave_sha256=sha(args.wave), calls_sha256=sha(args.calls),
                    source_root=str(args.source_root.resolve()),
                    commit=subprocess.check_output(['git','-C',str(args.source_root),'rev-parse','HEAD'], text=True).strip(),
                    parser_sha256=sha(args.source_root/'src/vcd_parser.py'),
                    python=sys.version, platform=platform.platform(),
                    cpu_affinity=sorted(os.sched_getaffinity(0)),
                    os_cache_condition='Input hash primes OS cache; no cache flush; cold means fresh parser',
                    profile_parse=args.profile_parse, warm_repeats=args.warm_repeats,
                    benchmark_includes='public handler, worker scheduling, schema validation, serialization; excludes transport',
                    calls=calls)
    (args.output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    async def invoke(name, arguments):
        result = json.loads((await server.call_tool(name, arguments))[0].text)
        if result.get('error_code'):
            raise RuntimeError(result)
        return result
    if args.debug_dir:
        root = args.debug_dir.resolve()
        receipts = [await invoke('get_sim_paths', dict(verif_root=str(root)))]
        compile_args = dict(compile_log=str(root/'compile.log'), simulator='verilator')
        receipts += await asyncio.gather(invoke('build_tb_hierarchy', compile_args), invoke('scan_structural_risks', compile_args))
        receipts.append(await invoke('parse_sim_log', dict(log_path=str(root/'simulation.log'), simulator='verilator')))
        (args.output/'prerequisites.json').write_text(json.dumps(receipts, indent=2)+'\n')
    measurements, facts = [], []
    for repeat in range(args.warm_repeats + 1):
        sequence_start = resources()
        for index, call in enumerate(calls):
            start = resources()
            result = await invoke(call['tool'], call['arguments'])
            end = resources()
            measurements.append(dict(repeat=repeat, call_index=index, tool=call['tool'],
                                     wall_seconds=end['wall']-start['wall'], cpu_seconds=end['cpu']-start['cpu'],
                                     start=start, end=end, metrics=metrics[-1]))
            facts.append(dict(repeat=repeat, call_index=index, result=result))
        sequence_end = resources()
        print(json.dumps(dict(repeat=repeat, sequence_wall_seconds=sequence_end['wall']-sequence_start['wall'],
                              rss_kib=sequence_end['rss_kib'], peak_rss_kib=sequence_end['peak_rss_kib'])), flush=True)
    measured_end = resources()
    if args.debug_dir:
        result = await invoke('recommend_failure_debug_next_steps', dict(log_path=str(args.debug_dir.resolve()/'simulation.log'), wave_path=str(args.wave.resolve()), simulator='verilator'))
        (args.output/'recommendation.json').write_text(json.dumps(result, indent=2)+'\n')
    fingerprint = transition_fingerprint(server._get_parser(str(args.wave.resolve()))) if args.fingerprint else None
    (args.output/'facts.json').write_text(json.dumps(facts, indent=2)+'\n')
    (args.output/'measurements.json').write_text(json.dumps(dict(calls=measurements, parse_probes=probes, measured_end=measured_end, transition_fingerprint=fingerprint), indent=2)+'\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--wave', type=Path, required=True)
    parser.add_argument('--calls', type=Path, required=True, help='JSON list of {tool, arguments}; same retained wave only')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--debug-dir', type=Path, help='Run discovery, parallel hierarchy/scan and log parsing before timed wave calls')
    parser.add_argument('--warm-repeats', type=int, default=2)
    parser.add_argument('--profile-parse', action='store_true')
    parser.add_argument('--fingerprint', action='store_true')
    args = parser.parse_args()
    if args.warm_repeats < 0:
        parser.error('--warm-repeats must be nonnegative')
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
