"""Public prefix limits must preserve exact FSDB counts and reader ownership."""

import ctypes
import os
from pathlib import Path
import threading

import pytest

import server
from src.cancellation import OperationCancelled, pop_cancel_event, push_cancel_event
from tests.test_expression_observe import wave as expression_wave
from tests.test_fsdb_cursor_reclaim import (
    RAW_EVENTS, _assert_clean, _parser, _snapshot, reclaim_libraries,
)
from tests.test_fsdb_point_window import same_tick_fsdb
from tests.test_fsdb_runtime import _FakeLazyGroupLib, _fake_group_parser
from tests.test_waveform_selection import dump as selection_wave


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def reset_server():
    server.reset_session_state()
    yield
    server.reset_session_state()


def _int(value):
    return value.value if hasattr(value, "value") else value


class _PagedLib(_FakeLazyGroupLib):
    """In-memory C ABI oracle, independent of the capped Python algorithm.

    The native count includes its unread predecessor; rows are physical-time
    records, while legacy transitions apply the public ceil-ps window.
    """
    _traceweave_has_event_pages_v1 = True
    _traceweave_has_event_count_v1 = True

    def __init__(self, rows=None, *, scale_fs=1000):
        super().__init__()
        self.rows = list(rows if rows is not None else
                         [(t, str(t % 2)) for t in range(20)])
        self.scale_fs = scale_fs
        self.cursors = {}
        self.next_cursor = 0
        self.value_reads = self.count_reads = 0
        self.page_capacities = []
        self.after_count = None
        self.count_rc = 0
        self.legacy_truncated = False

    def fsdb_open(self, path):
        self.calls.append("reopen")
        return ctypes.c_void_p(1)

    def fsdb_get_scale_info(self, handle, buffer, size):
        buffer.value = b"1ps" if self.scale_fs == 1000 else b"100fs"
        return self.scale_fs

    def fsdb_event_end_tick_v1(self, handle, out):
        out._obj.value = self.rows[-1][0] if self.rows else 0
        return 0

    def fsdb_event_open_v1(self, handle, path, start, end, out):
        start, end = _int(start), _int(end)
        earlier = [i for i, (tick, _) in enumerate(self.rows)
                   if tick * self.scale_fs < start * 1000]
        index = earlier[-1] if earlier else 0
        self.next_cursor += 1
        self.cursors[self.next_cursor] = [index, start, end]
        out._obj.value = self.next_cursor
        self.calls.append(("cursor_open", start, end))
        return 0

    def _read(self, cursor, limit, receipt, buffer=None):
        state = self.cursors[_int(cursor)]
        index, start, end = state
        selected = []
        while index < len(self.rows) and len(selected) < _int(limit):
            tick, value = self.rows[index]
            if tick * self.scale_fs > end * 1000:
                break
            selected.append((tick, value))
            index += 1
        state[0] = index
        more = index < len(self.rows) and self.rows[index][0] * self.scale_fs <= end * 1000
        page = receipt._obj
        page.events = len(selected)
        page.complete, page.has_next = not more, more
        page.next_tick = self.rows[index][0] if more else 0
        page.truncated = 0
        if buffer is not None:
            text = "".join(
                f'{"P" if tick * self.scale_fs < start * 1000 else "T"}\t'
                f'{(tick * self.scale_fs + 999) // 1000}\t{tick}\t{value}\n'
                for tick, value in selected)
            buffer.value = text.encode()
            page.output_bytes = len(text)
            self.value_reads += len(selected)
        else:
            page.output_bytes = 0
            self.count_reads += len(selected)
        return 0

    def fsdb_event_page_v1(self, handle, cursor, limit, buffer, size, receipt, receipt_size):
        self.calls.append("page")
        self.page_capacities.append(_int(size))
        return self._read(cursor, limit, receipt, buffer)

    def fsdb_event_count_v1(self, handle, cursor, limit, receipt, receipt_size):
        self.calls.append("count")
        self._read(cursor, limit, receipt)
        if self.after_count:
            self.after_count()
        return self.count_rc

    def fsdb_event_close_v1(self, handle, cursor):
        self.calls.append("cursor_close")
        self.cursors.pop(_int(cursor), None)
        return 0

    def fsdb_end_transition_group(self, handle, profile):
        self.cursors.clear()
        return super().fsdb_end_transition_group(handle, profile)

    def fsdb_close(self, handle):
        self.cursors.clear()
        super().fsdb_close(handle)

    def fsdb_get_transitions_profiled(self, handle, path, start, end, buffer, size, profile):
        self.calls.append("legacy")
        start, end = _int(start), _int(end)
        rows = [((tick * self.scale_fs + 999) // 1000, value) for tick, value in self.rows]
        previous = [row for row in rows if row[0] < start]
        found = [row for row in rows if start <= row[0] <= end]
        text = (f"@PREDECESSOR\t{previous[-1][0]}\t{previous[-1][1]}\n" if previous else "")
        text += "".join(f"{ps}\t{value}\n" for ps, value in found)
        if self.legacy_truncated:
            text += "@TRUNCATED\n"
        buffer.value = text.encode()
        profile._obj.transition_count = len(found)
        return len(found)


def _fake_parser(lib=None):
    parser = _fake_group_parser(lib or _PagedLib())
    parser._scale_fs = parser._lib.scale_fs
    parser.get_signal_width = lambda path: len(parser._lib.rows[0][1]) if parser._lib.rows else 1
    # Legacy fallback needs a writable buffer, but no test needs a 64 MiB allocation.
    parser._buf = ctypes.create_string_buffer(65536)
    return parser


async def _public(parser, monkeypatch, **args):
    monkeypatch.setattr(server, "_get_parser", lambda path: parser)
    return await server._dispatch("get_signal_transitions", {
        "wave_path": parser.file_path, "signal_path": "top.data", **args})


@pytest.mark.anyio
@pytest.mark.parametrize("size,cap", [(0, 1), (1, 1), (2, 2), (3, 2), (1001, 1000)])
async def test_public_cap_keeps_exact_total_without_materializing_suffix(monkeypatch, size, cap):
    lib = _PagedLib([(t, str(t % 2)) for t in range(size)])
    parser = _fake_parser(lib)
    try:
        result = await _public(parser, monkeypatch, max_transitions=cap)
        assert result.transition_count == size
        assert len(result.transitions) == min(size, cap)
        assert [row["time_ps"] for row in result.transitions] == list(range(min(size, cap)))
        assert result.truncated == (size > cap)
        assert not result.transition_count_is_lower_bound
        assert result.predecessor is None
        assert lib.value_reads <= cap
        assert lib.value_reads + lib.count_reads == size
        assert "legacy" not in lib.calls
        assert not parser._transition_group_active and not lib.cursors
        assert lib.calls.index("cursor_close") < lib.calls.index("end")
        if size > cap:
            assert str(size) in result.hint and "max_transitions" in result.hint
        else:
            assert result.hint is None
    finally:
        parser.close()


@pytest.mark.anyio
async def test_default_cap_is_1000_and_predecessor_does_not_use_return_budget(monkeypatch):
    lib = _PagedLib([(t, str(t % 2)) for t in range(1010)])
    parser = _fake_parser(lib)
    try:
        result = await _public(parser, monkeypatch, start_time_ps=5)
        assert len(result.transitions) == 1000
        assert result.transition_count == 1005
        assert result.predecessor["time_ps"] == 4
        assert result.transitions[0]["time_ps"] == 5
        assert result.transitions[-1]["time_ps"] == 1004
        assert result.truncated and not result.transition_count_is_lower_bound
        assert "1005" in result.hint
    finally:
        parser.close()


@pytest.mark.anyio
@pytest.mark.parametrize("cap", [0, -1, True, 1.5, "2"])
async def test_invalid_cap_is_rejected_before_parser_lookup(monkeypatch, cap):
    monkeypatch.setattr(server, "_get_parser", lambda path: pytest.fail("invalid cap performed waveform I/O"))
    with pytest.raises(ValueError, match="max_transitions"):
        await server._dispatch("get_signal_transitions", dict(wave_path="/fake/test.fsdb",
            signal_path="top.data", max_transitions=cap))


@pytest.mark.anyio
@pytest.mark.parametrize("missing", ["event_count_v1", "event_pages_v1", "lazy_transition_group"])
@pytest.mark.parametrize("lower_bound", [False, True])
async def test_optional_abi_fallback_retains_legacy_count_and_coverage(monkeypatch, missing, lower_bound):
    lib = _PagedLib()
    setattr(lib, "_traceweave_has_" + missing, False)
    lib.legacy_truncated = lower_bound
    parser = _fake_parser(lib)
    try:
        result = await _public(parser, monkeypatch, max_transitions=2)
        assert len(result.transitions) == 2 and result.transition_count == 20
        assert result.truncated and result.transition_count_is_lower_bound == lower_bound
        assert "legacy" in lib.calls and "page" not in lib.calls and "count" not in lib.calls
        assert ("lower bound" in result.hint) == lower_bound
    finally:
        parser.close()


@pytest.mark.anyio
@pytest.mark.parametrize("width,uses_pages", [(300_000, True), (1_048_577, False)])
async def test_wide_records_grow_page_capacity_or_preserve_legacy_support(monkeypatch, width, uses_pages):
    lib = _PagedLib([(0, "0" * width), (10, "1" * width), (20, "x" * width)])
    parser = _fake_parser(lib)
    parser._buf = ctypes.create_string_buffer(3 * width + 256)
    try:
        result = await _public(parser, monkeypatch, max_transitions=1)
        assert result.transition_count == 3 and len(result.transitions) == 1
        assert result.transitions[0]["value"]["bin"] == "0" * width
        assert result.truncated and not result.transition_count_is_lower_bound
        if uses_pages:
            assert lib.page_capacities and min(lib.page_capacities) > 262144
            assert max(lib.page_capacities) <= 1048576
            assert lib.value_reads == 1 and lib.count_reads == 2
            assert "legacy" not in lib.calls
        else:
            assert lib.calls == ["legacy"]
    finally:
        parser.close()


@pytest.mark.anyio
async def test_group_begin_failure_falls_back_without_leaving_owned_state(monkeypatch):
    lib = _PagedLib()
    parser = _fake_parser(lib)
    monkeypatch.setattr(lib, "fsdb_begin_lazy_transition_group_v1", lambda *args: -2)
    try:
        result = await _public(parser, monkeypatch, max_transitions=2)
        assert result.transition_count == 20 and len(result.transitions) == 2
        assert not result.transition_count_is_lower_bound
        assert lib.calls == ["legacy"]
        assert not parser._transition_group_active and not lib.cursors
    finally:
        parser.close()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["selection", "expression"])
async def test_adapters_keep_derived_changes_and_receipts_instead_of_forwarding_cap(monkeypatch, tmp_path, kind):
    base = selection_wave(tmp_path) if kind == "selection" else expression_wave(tmp_path)
    # Both adapters forward unknown attributes via __getattr__. A generic
    # hasattr-based fast path would accidentally call this backing-reader API.
    monkeypatch.setattr(base, "get_transitions_capped",
                        lambda *a, **kw: pytest.fail("derived signal bypassed its adapter"), raising=False)
    if kind == "selection":
        signal = {"path": "tb.bus[15:8]", "lsb": 8, "width": 4}
        start, end = 5, 30
    else:
        signal = dict(expr="a[i] + e", typing="wave_bits", bindings={
            "a": "tb.data", "i": "tb.index", "e": "tb.expected"})
        start, end = 5, 25
    result = await _public(base, monkeypatch, signal_path=signal, start_time_ps=start,
                           end_time_ps=end, max_transitions=1)
    assert result.transition_count == 2 and len(result.transitions) == 1
    assert result.truncated and not result.transition_count_is_lower_bound
    if kind == "selection":
        assert result.transitions[0]["time_ps"] == 20
        assert result.transitions[0]["value"]["bin"] == "xxxx"
        assert result.predecessor["value"]["bin"] == "xz10"
        assert result.selections[0].bits == [11, 10, 9, 8]
        assert result.predecessor_kind == "declaration_anchor"
    else:
        assert result.transitions[0]["time_ps"] == 10
        assert result.transitions[0]["value"]["dec"] == 5
        assert result.predecessor["value"]["dec"] == 6
        assert result.expressions[0].coverage_status == "complete"
        assert result.predecessor_kind == "dependency_anchor"


@pytest.mark.parametrize("rc,cancelled", [(0, True), (-3, False), (-6, False), (-3, True), (-6, True)])
def test_count_cancellation_and_errors_release_resources_before_reuse(rc, cancelled):
    lib = _PagedLib()
    parser = _fake_parser(lib)
    event = threading.Event()
    token = push_cancel_event(event)
    lib.count_rc = rc
    lib.after_count = event.set if cancelled else None
    try:
        with pytest.raises(OperationCancelled if cancelled else RuntimeError):
            parser.get_transitions_capped("top.data", max_transitions=2)
        assert "count" in lib.calls
        assert not lib.cursors and not parser._transition_group_active
        if rc == -6:
            assert parser._handle is None and "close" in lib.calls
        else:
            assert lib.calls.index("cursor_close") < lib.calls.index("end")
        event.clear()
        lib.count_rc, lib.after_count = 0, None
        result = parser.get_transitions_capped("top.data", max_transitions=2)
        assert result["transition_count"] == 20
        assert len(result["transitions"]) == 2
        assert not lib.cursors and not parser._transition_group_active
    finally:
        pop_cancel_event(token)
        parser.close()


def test_count_identity_change_cannot_return_a_successful_prefix():
    lib = _PagedLib()
    parser = _fake_parser(lib)
    original, changed = object(), object()
    parser._file_identity = original
    parser._stat_identity = lambda: original
    lib.after_count = lambda: setattr(parser, "_stat_identity", lambda: changed)
    try:
        with pytest.raises(RuntimeError, match="changed"):
            parser.get_transitions_capped("top.data", max_transitions=2)
        assert not lib.cursors and not parser._transition_group_active
        assert "count" in lib.calls
    finally:
        parser.close()


@pytest.mark.eda_integration
@pytest.mark.anyio
@pytest.mark.parametrize("start,end", [(0, 4), (1, 1), (2, 2), (3, 3), (4, 4), (5, 6), (1, 2), (0, 0)])
@pytest.mark.parametrize("cap", [1, 2, 7, 1000])
async def test_real_100fs_public_window_matches_uncapped_and_generator(
        reclaim_libraries, same_tick_fsdb, monkeypatch, start, end, cap):
    with _parser(reclaim_libraries("records2"), same_tick_fsdb, monkeypatch) as parser:
        full = parser.get_transitions("top.data", start, end)
        public = [((tick + 9) // 10, value) for tick, value in RAW_EVENTS]
        expected = [row for row in public if start <= row[0] <= end]
        assert [(row["time_ps"], row["value"]["bin"]) for row in full["transitions"]] == expected
        result = await _public(parser, monkeypatch, start_time_ps=start,
                               end_time_ps=end, max_transitions=cap)
        assert result.transitions == full["transitions"][:cap]
        assert result.predecessor == full["predecessor"]
        assert result.transition_count == len(expected)
        assert result.truncated == (len(expected) > cap)
        assert not result.transition_count_is_lower_bound
        _assert_clean(parser)


@pytest.mark.eda_integration
@pytest.mark.anyio
@pytest.mark.parametrize("filename,path", [
    ("scale_1ns.fsdb", "scale_1ns_tb.addr[31:0]"),
    ("scale_100fs.fsdb", "scale_100fs_tb.addr[31:0]"),
    ("wide_bus.fsdb", "tb.dat1[1023:0]"),
])
async def test_real_scale_and_wide_values_keep_complete_first_record(
        reclaim_libraries, monkeypatch, filename, path):
    with _parser(reclaim_libraries("default"), FIXTURES / filename, monkeypatch) as parser:
        full = parser.get_transitions(path)
        parser._lib.test_probe_reset()
        result = await _public(parser, monkeypatch, signal_path=path, max_transitions=1)
        assert result.transitions == full["transitions"][:1]
        assert result.transition_count == full["transition_count"]
        assert result.truncated and not result.transition_count_is_lower_bound
        assert _snapshot(parser)["get_vc"] == 1
        _assert_clean(parser, (path,))


@pytest.mark.eda_integration
@pytest.mark.anyio
async def test_real_ten_million_public_prefix_has_exact_count_and_bounded_residency(
        reclaim_libraries, monkeypatch, record_property):
    wave = os.environ.get("TW_LAZY_LARGE_FSDB")
    if not wave:
        pytest.skip("set TW_LAZY_LARGE_FSDB to activity_10000000.fsdb")
    with _parser(reclaim_libraries("default"), wave, monkeypatch) as parser:
        result = await _public(parser, monkeypatch)
        assert result.transition_count == 10_000_001
        assert len(result.transitions) == 1000
        assert result.truncated and not result.transition_count_is_lower_bound
        assert "10000001" in result.hint
        assert [(row["time_ps"], row["value"]["bin"]) for row in result.transitions] == [
            (index * 10, f"{index * 2654435761 & 0xffffffff:032b}") for index in range(1000)]
        stats = _snapshot(parser)
        assert stats["get_vc"] == 1000
        assert 65536 <= stats["peak_records"] < 1_000_000
        for name, value in stats.items():
            record_property(name, value)
        _assert_clean(parser, ("top.data", "top.clk", "top.wide", "top.sparse"))
