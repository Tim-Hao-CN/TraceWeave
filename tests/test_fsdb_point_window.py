"""Portable lifecycle faults plus opt-in same-trace FSDB integration."""

import os
import ctypes
from pathlib import Path
import random
import shutil
import subprocess

import pytest

from src.fsdb_parser import FSDBParser

ROOT = Path(__file__).resolve().parents[1]
NATIVE_FSDB = ROOT / "native" / "fsdb"


@pytest.fixture(scope="module")
def native_lifecycle_probe(tmp_path_factory):
    compiler = shutil.which("g++")
    if compiler is None:
        pytest.skip("C++ compiler unavailable")
    executable = tmp_path_factory.mktemp("point_read") / "lifecycle"
    subprocess.run([compiler, "-std=c++11", "-Wall", "-Wextra", "-Werror",
                    "-I", str(NATIVE_FSDB), str(ROOT / "tests/native/fsdb_point_read_test.cpp"),
                    "-o", str(executable)], check=True, capture_output=True, text=True)
    return executable


@pytest.mark.parametrize("scenario", ["success", "resident", "bounds", "disabled", "before", "after",
                                      "set", "unsupported", "add", "load", "create", "throw_load", "throw_body",
                                      "restore", "unload", "throw_add", "throw_create", "throw_free",
                                      "throw_unload", "throw_restore", "reset_initial", "reset_cleanup",
                                      "throw_reset_initial", "throw_reset_cleanup",
                                      "selection_dedup", "selection_transfer"])
def test_native_point_resource_lifecycle(native_lifecycle_probe, scenario):
    subprocess.run([str(native_lifecycle_probe), scenario], check=True, timeout=10)


@pytest.mark.eda_integration
def test_multiblock_points_preserve_following_reads():
    """Set TW_POINT_READ_FSDB to the 1M-step benchmark's converted FSDB.

    Generate using benchmark_waveform_reads.py, then convert with local
    vcd2fsdb and FSDB_ENV_WRITER_MEM_LIMIT=4 to exercise multiple flushes.
    """
    wave = os.environ.get("TW_POINT_READ_FSDB")
    if not wave:
        pytest.skip("set TW_POINT_READ_FSDB to a generated 1M-step FSDB")
    parser = FSDBParser(wave)
    duration = 10_000_000
    rng = random.Random(716)
    times = [0, 10, duration // 2, duration, duration + 100]
    times += [rng.randrange(duration) for _ in range(7)]
    try:
        before = parser.get_transitions("top.data", 0, 30)
        after = parser.get_transitions("top.data", duration - 30, duration)
        around = parser.get_signals_around_time(["top.data", "top.sparse"], 333, 30, 5)
        for timestamp in times:
            for leaf in ("data", "wide", "sparse"):
                step = min(timestamp // 10, 1_000_000)
                if leaf == "wide":
                    step = step // 1000 * 1000
                if leaf == "sparse":
                    step = 500_000 if step >= 500_000 else 0
                expected = f"{step * 2654435761 & 0xffffffff:032b}"
                if leaf == "wide":
                    expected *= 32
                if timestamp > duration:
                    expected = "x"  # Preserve the legacy FFR out-of-file seek.
                assert parser.get_value_at_time("top." + leaf, timestamp)["value"]["bin"] == expected, (leaf, timestamp)
            assert parser.get_summary()["simulation_duration_ps"] == duration
            assert parser.get_transitions("top.data", 0, 30) == before
            assert parser.get_transitions("top.data", duration - 30, duration) == after
        assert parser.get_signals_around_time(["top.data", "top.sparse"], 333, 30, 5) == around
    finally:
        parser.close()


@pytest.fixture(scope="module")
def native_reader_probe(tmp_path_factory):
    """Use real FFR selection/residency observations without changing public ABI."""
    verdi = os.environ.get("VERDI_HOME")
    if not os.environ.get("TW_POINT_READ_FSDB") or not verdi or not shutil.which("g++"):
        pytest.skip("requires TW_POINT_READ_FSDB, VERDI_HOME and g++")
    tmp_path = tmp_path_factory.mktemp("reader_probe")
    sdk = Path(verdi) / "share/FsdbReader"
    source = (NATIVE_FSDB / "fsdb_wrapper.cpp").read_text() + r'''
extern "C" int test_view(void *p, unsigned long long start, unsigned long long end) {
    FsdbCtx *ctx = (FsdbCtx*)p;
    fsdbTag64 a = _ToTag(ctx, start), b = _ToTag(ctx, end);
    return ctx->obj->ffrResetViewWindow((fsdbXTag*)&a, (fsdbXTag*)&b);
}
extern "C" int test_bounds(void *p, unsigned long long *out) {
    FsdbCtx *ctx = (FsdbCtx*)p;
    fsdbTag64 a, b;
    if (ctx->obj->ffrGetMinFsdbTag64(&a) != FSDB_RC_SUCCESS ||
        ctx->obj->ffrGetMaxFsdbTag64(&b) != FSDB_RC_SUCCESS) return -1;
    out[0] = _TagToPs(ctx, a); out[1] = _TagToPs(ctx, b);
    int count = 0;
    for (ffrSessionInfo *s = ctx->obj->ffrGetSessionListInfo(); s; s = s->next) ++count;
    return count;
}
extern "C" int test_session_boundaries(void *p, unsigned long long *out, int cap) {
    FsdbCtx *ctx = (FsdbCtx*)p;
    int n = 0;
    for (ffrSessionInfo *s = ctx->obj->ffrGetSessionListInfo(); s && n + 2 <= cap; s = s->next) {
        out[n++] = _TagToPs(ctx, *(fsdbTag64*)&s->start_xtag);
        out[n++] = _TagToPs(ctx, *(fsdbTag64*)&s->close_xtag);
    }
    return n;
}
extern "C" int test_signal_state(void *p, const char *path, unsigned long long *out) {
    FsdbCtx *ctx = (FsdbCtx*)p;
    auto found = ctx->path_to_sig.find(path);
    if (found == ctx->path_to_sig.end()) return -1;
    fsdbVarIdcode id = found->second.idcode;
    out[0] = ctx->obj->ffrIsInSignalList(id) ? 1 : 0;
    out[1] = ctx->obj->ffrGetVCCount(id);
    return 0;
}
'''
    cpp, library = tmp_path / "probe.cpp", tmp_path / "probe.so"
    cpp.write_text(source)
    subprocess.run(["g++", "-shared", "-fPIC", "-std=c++11", "-I" + str(NATIVE_FSDB),
                    "-I" + str(sdk), str(cpp), "-o", str(library),
                    "-L" + str(sdk / "linux64"), "-lnffr", "-lnsys", "-lz",
                    "-Wl,-rpath," + str(sdk / "linux64")], check=True, capture_output=True)
    return library


@pytest.mark.eda_integration
def test_real_reader_restores_an_existing_view(native_reader_probe, monkeypatch):
    """Point reads preserve an existing view and session-boundary values."""
    wave = os.environ["TW_POINT_READ_FSDB"]
    from src import fsdb_parser
    library = native_reader_probe
    monkeypatch.setattr(fsdb_parser, "_WRAPPER_SO", str(library))
    parser = FSDBParser(wave)
    try:
        parser._open()
        library = parser._lib
        library.test_view.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint64]
        library.test_bounds.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64)]
        library.test_session_boundaries.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64), ctypes.c_int]
        bounds = (ctypes.c_uint64 * 2)()
        assert library.test_bounds(parser._handle, bounds) > 1
        assert list(bounds) == [0, 10_000_000]
        assert library.test_view(parser._handle, 1_000_000, 9_000_000) == 0
        for timestamp in (8_000_003, 2_000_003, 7_000_003):
            assert parser.get_value_at_time("top.data", timestamp)["value"]["dec"] == (timestamp // 10 * 2654435761 & 0xffffffff)
            assert library.test_bounds(parser._handle, bounds) > 1
            assert list(bounds) == [1_000_000, 9_000_000]
        assert library.test_view(parser._handle, 0, 10_000_000) == 0
        boundaries = (ctypes.c_uint64 * 64)()
        count = library.test_session_boundaries(parser._handle, boundaries, 64)
        assert 2 < count < 64
        times = {t + offset for t in list(boundaries)[:count] for offset in (-1, 0, 1)
                 if 0 <= t + offset <= 10_000_000}
        for timestamp in sorted(times):
            for leaf in ("data", "sparse"):
                step = timestamp // 10
                if leaf == "sparse":
                    step = 500_000 if step >= 500_000 else 0
                assert parser.get_value_at_time("top." + leaf, timestamp)["value"]["dec"] == (step * 2654435761 & 0xffffffff)
        assert library.test_bounds(parser._handle, bounds) > 1
        assert list(bounds) == [0, 10_000_000]
    finally:
        parser.close()


@pytest.mark.eda_integration
@pytest.mark.parametrize("first_read", ["point", "transitions", "around", "group"])
def test_interleaved_reads_release_selection_and_residency(native_reader_probe, monkeypatch, first_read):
    """Dense A must not become a hidden dependency of subsequent sparse B reads."""
    from src import fsdb_parser
    monkeypatch.setattr(fsdb_parser, "_WRAPPER_SO", str(native_reader_probe))
    parser = FSDBParser(os.environ["TW_POINT_READ_FSDB"])
    fresh = FSDBParser(parser.file_path)
    dense, sparse = "top.data", "top.sparse"

    def state(path):
        out = (ctypes.c_uint64 * 2)()
        assert parser._lib.test_signal_state(parser._handle, path.encode(), out) == 0
        return tuple(out)

    def clean():
        assert state(dense) == (0, 0)
        assert state(sparse) == (0, 0)

    try:
        expected_transition = fresh.get_transitions(sparse, 3_000_000, 3_000_050)
        expected_point = fresh.get_value_at_time(sparse, 3_000_033)
        expected_around = fresh.get_signals_around_time([sparse], 3_000_033, 30, 5)
        fresh.close()
        parser._open()
        parser._lib.test_signal_state.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                                  ctypes.POINTER(ctypes.c_uint64)]
        if first_read == "point":
            parser.get_value_at_time(dense, 7_000_033)
        elif first_read == "transitions":
            parser.get_transitions(dense, 7_000_000, 7_000_050)
        elif first_read == "around":
            parser.get_signals_around_time([dense], 7_000_033, 30, 5)
        else:
            with parser.transition_group([dense]) as active:
                assert active
                parser.get_transitions(dense, 7_000_000, 7_000_050)
                assert state(dense)[0] == 1 and state(dense)[1] > 0
        clean()
        assert parser.get_transitions(sparse, 3_000_000, 3_000_050) == expected_transition
        clean()
        assert parser.get_value_at_time(sparse, 3_000_033) == expected_point
        clean()
        assert parser.get_signals_around_time([sparse], 3_000_033, 30, 5) == expected_around
        clean()
        with parser.transition_group([sparse, sparse]) as active:
            assert active
            group_state = state(sparse)
            assert group_state[0] == 1 and group_state[1] > 0
            assert state(dense) == (0, 0)
            assert parser.get_value_at_time(sparse, 3_000_033) == expected_point
            assert parser.get_transitions(sparse, 3_000_000, 3_000_050) == expected_transition
            assert parser.get_signals_around_time([sparse], 3_000_033, 30, 5) == expected_around
            assert state(sparse) == group_state
            for outside_read in (
                lambda: parser.get_value_at_time(dense, 3_000_033),
                lambda: parser.get_transitions(dense, 3_000_000, 3_000_050),
                lambda: parser.get_signals_around_time([sparse, dense], 3_000_033, 30, 5),
            ):
                with pytest.raises(RuntimeError, match="rc=-5"):
                    outside_read()
                assert state(sparse) == group_state
                assert state(dense) == (0, 0)
            with parser._event_pages(sparse, 3_000_000, 3_000_050) as reader:
                page = reader.read_page()
                assert page.complete
                assert page.predecessor.value == expected_transition["predecessor"]["value"]["bin"]
            assert state(sparse) == group_state
            assert state(dense) == (0, 0)
        clean()
        # An exceptional group body must still release selection and residency.
        with pytest.raises(RuntimeError, match="injected group interruption"):
            with parser.transition_group([dense, sparse]) as active:
                assert active
                with parser._event_pages(dense, 100, 200, max_events=2) as reader:
                    reader.read_page()
                    raise RuntimeError("injected group interruption")
        clean()
        assert parser.get_transitions(sparse, 3_000_000, 3_000_050) == expected_transition
        clean()
    finally:
        parser.close()
        fresh.close()
