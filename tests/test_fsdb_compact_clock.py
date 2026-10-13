"""Compact FSDB clock indexing preserves cycle semantics and cache bounds."""

from array import array
from contextlib import contextmanager
import os
import random
import threading

import pytest

import server
from src import clock_edge_cache as storage, cycle_query
from src.cancellation import OperationCancelled, pop_cancel_event, push_cancel_event
from src.clock_edge_cache import ClockEdgeCache
from src.expression_observe import ExpressionParser
from src.waveform_selection import SelectionParser
from tests.test_expression_observe import wave as expression_wave
from tests.test_fsdb_capped_transitions import _PagedLib, _fake_parser
from tests.test_fsdb_cursor_reclaim import (
    RAW_EVENTS, _assert_clean, _parser, _snapshot, reclaim_libraries,
)
from tests.test_fsdb_point_window import same_tick_fsdb


@pytest.fixture
def cache(monkeypatch):
    instance = ClockEdgeCache(max_bytes=65536, entry_max_bytes=8192, max_entries=8)
    monkeypatch.setattr(storage, "cache", instance)
    return instance


@contextmanager
def _clock(rows, monkeypatch, *, scale_fs=1000, page_size=2):
    lib = _PagedLib(rows, scale_fs=scale_fs)
    parser = _fake_parser(lib)
    original = parser._event_pages

    @contextmanager
    def pages(*args, **kwargs):
        kwargs["max_events"] = page_size
        with original(*args, **kwargs) as reader:
            yield reader

    monkeypatch.setattr(parser, "_event_pages", pages)
    try:
        yield parser, lib
    finally:
        parser.close()


def _compact(parser, *, edge="posedge", start=0, end=-1, path="top.clk"):
    return cycle_query._read_fsdb_clock_edges(parser, path, edge, start, end)


def _query(parser, **kwargs):
    return cycle_query.get_signals_by_cycle(parser, "top.clk", [], **kwargs)


@pytest.mark.parametrize("page_size", [1, 2, 3])
@pytest.mark.parametrize("edge,expected,period", [
    ("posedge", [1, 5, 8, 8], 3), ("negedge", [4, 8], 4),
])
def test_after_edges_keep_same_tick_order_and_recover_after_unknowns(
        monkeypatch, cache, page_size, edge, expected, period):
    rows = [(0, "0"), (1, "1"), (2, "x"), (3, "1"), (4, "0"),
            (5, "1"), (6, "z"), (7, "0"), (8, "1"), (8, "0"), (8, "1")]
    with _clock(rows, monkeypatch, page_size=page_size) as (parser, lib):
        monkeypatch.setattr(parser, "get_transitions", lambda *a, **kw: pytest.fail("materialized clock"))
        edges, actual_period, complete = _compact(parser, edge=edge)
        assert isinstance(edges, array) and edges.typecode == "Q"
        assert list(edges) == expected and actual_period == period and complete
        assert lib.value_reads == len(rows) and lib.count_reads == 0
        assert not lib.cursors and not parser._transition_group_active
        assert cache.snapshot()["entries"] == 0  # Range helper itself is not a full-index cache.


@pytest.mark.parametrize("histogram_limit", [4096, 1])
@pytest.mark.parametrize("edges,expected", [
    ([], None), ([5], None), ([1, 1, 1, 2], 0),
    ([5, 5, 8, 13, 30], 4), ([0, 0, 1, 3, 7, 15, 31, 63, 127], 6),
    ([0, 2**55, 2**56 + 1], 2**55),
    ([10, 4, 8, 3, 5], 3), ([5, 4, 3], None),
])
def test_exact_compact_median_includes_zero_deltas_and_integer_rounding(
        monkeypatch, histogram_limit, edges, expected):
    monkeypatch.setattr(cycle_query, "_CLOCK_DELTA_HISTOGRAM_LIMIT", histogram_limit)
    assert cycle_query._compute_compact_clock_period_ps(array("Q", edges)) == expected


def test_radix_median_matches_independent_jitter_oracle(monkeypatch):
    monkeypatch.setattr(cycle_query, "_CLOCK_DELTA_HISTOGRAM_LIMIT", 2)
    rng = random.Random(20261013)
    for size in (2, 3, 19, 100, 257):
        deltas = [rng.randrange(1_000_001) for _ in range(size - 1)]
        edges = [0]
        for delta in deltas:
            edges.append(edges[-1] + delta)
        ordered = sorted(deltas)
        middle = len(ordered) // 2
        expected = (ordered[middle] if len(ordered) % 2 else
                    (ordered[middle - 1] + ordered[middle]) // 2)
        assert cycle_query._compute_compact_clock_period_ps(array("Q", edges)) == expected


def test_global_cycles_cache_complete_compact_index_and_keep_global_period(monkeypatch, cache):
    rows = [(0, "0"), (5, "1"), (5, "0"), (5, "1"), (7, "0"),
            (8, "1"), (12, "0"), (13, "1"), (29, "0"), (30, "1")]
    with _clock(rows, monkeypatch) as (parser, lib):
        monkeypatch.setattr(parser, "get_transitions", lambda *a, **kw: pytest.fail("materialized clock"))
        first = _query(parser, start_cycle=2, num_cycles=2)
        assert first["total_edges_found"] == 5 and first["clock_period_ps"] == 4
        assert first["clock_edges_complete"] and not first["truncated"]
        assert [(row["cycle"], row["time_ps"]) for row in first["cycles"]] == [(2, 8), (3, 13)]
        saved = cache.get(parser._clock_cache_token, ("top.clk", "posedge"))
        assert isinstance(saved[0], array) and list(saved[0]) == [5, 5, 8, 13, 30]
        calls = list(lib.calls)
        result = _query(parser, start_time_ps=5, end_time_ps=13, max_cycles=2)
        assert result["start_cycle"] == 0 and result["num_cycles_requested"] == 4
        assert result["capped"] and result["clock_period_ps"] == 4
        assert [row["time_ps"] for row in result["cycles"]] == [5, 5]
        assert lib.calls == calls
    assert cache.snapshot()["entries"] == 0


def test_partial_pages_never_publish_a_complete_clock_index(monkeypatch, cache):
    with _clock([(t, str(t % 2)) for t in range(10)], monkeypatch, page_size=4) as (parser, lib):
        original = lib.fsdb_event_page_v1

        def truncated(*args):
            rc = original(*args)
            receipt = args[-2]._obj
            receipt.truncated, receipt.complete = 1, 0
            return rc

        monkeypatch.setattr(lib, "fsdb_event_page_v1", truncated)
        for _ in range(2):
            result = _query(parser, num_cycles=2)
            assert result["total_edges_found"] == 2
            assert not result["clock_edges_complete"]
            assert [row["time_ps"] for row in result["cycles"]] == [1, 3]
            assert cache.snapshot()["entries"] == 0
        assert lib.calls.count("page") == 2 and not lib.cursors


@pytest.mark.parametrize("phase", ["page", "median"])
def test_cancelled_compact_build_releases_cursor_and_does_not_cache(monkeypatch, cache, phase):
    with _clock([(t, str(t % 2)) for t in range(10)], monkeypatch) as (parser, lib):
        event = threading.Event()
        token = push_cancel_event(event)
        owner = lib if phase == "page" else cycle_query
        name = "fsdb_event_page_v1" if phase == "page" else "_compute_compact_clock_period_ps"
        original = getattr(owner, name)

        def cancel(*args, **kwargs):
            result = original(*args, **kwargs)
            event.set()
            return result

        try:
            with monkeypatch.context() as patch:
                patch.setattr(owner, name, cancel)
                with pytest.raises(OperationCancelled):
                    _query(parser, num_cycles=2)
            assert cache.snapshot()["entries"] == 0
            assert not lib.cursors and not parser._transition_group_active
            event.clear()
            assert _query(parser, num_cycles=2)["total_edges_found"] == 5
            assert cache.snapshot()["entries"] == 1
        finally:
            pop_cancel_event(token)


@pytest.mark.parametrize("missing", ["event_pages_v1", "lazy_transition_group", "event_count_v1"])
def test_old_abi_uses_original_clock_path(monkeypatch, cache, missing):
    with _clock([(t, str(t % 2)) for t in range(10)], monkeypatch) as (parser, lib):
        setattr(lib, "_traceweave_has_" + missing, False)
        assert _compact(parser) is None
        result = _query(parser, num_cycles=2)
        assert result["total_edges_found"] == 5 and result["clock_period_ps"] == 2
        assert "legacy" in lib.calls and "page" not in lib.calls


def test_existing_group_keeps_its_owner_without_a_nested_compact_read(monkeypatch, cache):
    with _clock([(t, str(t % 2)) for t in range(6)], monkeypatch) as (parser, lib):
        edges, period, complete = _compact(parser)
        assert list(edges) == [1, 3, 5] and period == 2 and complete
        with parser.transition_group(["top.clk"]) as active:
            assert active
            before = list(lib.calls)
            assert _compact(parser) is None
            assert lib.calls == before and parser._transition_group_active


@pytest.mark.parametrize("kind", ["selection", "expression"])
def test_derived_clock_adapters_keep_their_evaluation_path(monkeypatch, cache, tmp_path, kind):
    base = expression_wave(tmp_path)
    if kind == "selection":
        parser = SelectionParser(base)
        clock = parser.bind({"path": "tb.data[7:0]", "bits": [3]})
        edge, expected = "negedge", [20]
    else:
        parser = ExpressionParser(base)
        clock = parser.bind({"expr": "!tb.clk", "typing": "wave_bits"})
        edge, expected = "posedge", [10, 20, 30, 40]
    assert _compact(parser, path=clock, edge=edge) is None
    result = cycle_query.get_signals_by_cycle(parser, clock, [], edge=edge, num_cycles=len(expected))
    assert [row["time_ps"] for row in result["cycles"]] == expected
    assert result["clock_edges_complete"]


def test_before_phase_keeps_raw_time_ambiguity_frontier(monkeypatch, cache):
    # Physical 0.1ps is a real edge; X at 0.2ps makes the remaining clock
    # prefix unsafe even though both values ceil to the same public ps.
    with _clock(RAW_EVENTS, monkeypatch, scale_fs=100, page_size=1) as (parser, lib):
        original = cycle_query._read_fsdb_clock_edges
        monkeypatch.setattr(cycle_query, "_read_fsdb_clock_edges",
                            lambda *a, **kw: pytest.fail("before used after-phase compact helper"))
        edges, period, frontier, complete = cycle_query._full_clock_edges(
            parser, "top.clk", "posedge", sample_phase="before")
        assert list(edges) == [100] and period is None
        assert frontier == 200 and not complete
        assert cache.snapshot()["entries"] == 0
        monkeypatch.setattr(cycle_query, "_read_fsdb_clock_edges", original)


class _DetectLib(_PagedLib):
    def __init__(self, signals):
        self.signals = signals
        super().__init__(next(iter(signals.values())))

    def fsdb_event_open_v1(self, handle, path, start, end, out):
        self.rows = self.signals[path.decode()]
        self.calls.append(("clock_open", path.decode(), start, end))
        return super().fsdb_event_open_v1(handle, path, start, end, out)


def _regular_clock(period, edges):
    return [(index * period // 2, str(index % 2)) for index in range(2 * edges + 1)]


@pytest.mark.parametrize("pseudo_only", [False, True])
def test_clock_detection_keeps_candidate_ranking_window_and_first_call_work(monkeypatch, cache, pseudo_only):
    signals = {"top.:jasper_formal_clock": _regular_clock(2, 10)}
    if not pseudo_only:
        signals.update({"top.clk": _regular_clock(10, 3),
                        "top.deep.clk": _regular_clock(10, 3),
                        "top.clock": _regular_clock(6, 4)})
    lib = _DetectLib(signals)
    parser = _fake_parser(lib)
    searches = []

    def search(keyword, max_results):
        searches.append((keyword, max_results))
        return {"results": [{"path": path, "width": 1} for path in reversed(signals)] +
                           [{"path": "top.wide_clock", "width": 32}]}

    monkeypatch.setattr(parser, "search_signals", search)
    monkeypatch.setattr(parser, "get_transitions", lambda *a, **kw: pytest.fail("materialized clock probe"))
    try:
        expected = ("top.:jasper_formal_clock", 2) if pseudo_only else ("top.clock", 6)
        assert server._detect_wave_clock(parser) == expected
        opens = [call for call in lib.calls if isinstance(call, tuple) and call[0] == "clock_open"]
        assert len(opens) == len(signals) and {call[1] for call in opens} == set(signals)
        assert [(call[1].count("."), len(call[1])) for call in opens] == sorted(
            (path.count("."), len(path)) for path in signals)
        assert all(call[2:] == (0, server.CLOCK_DETECT_SAMPLE_PS) for call in opens)
        assert searches == [("clk", 20), ("clock", 20)]
        assert cache.snapshot()["entries"] == 0
        calls = list(lib.calls)
        assert server._detect_wave_clock(parser) == expected
        assert lib.calls == calls and len(searches) == 2
    finally:
        parser.close()


@pytest.mark.parametrize("interruption", ["partial", "cancel"])
def test_interrupted_clock_detection_is_not_cached(monkeypatch, cache, interruption):
    lib = _DetectLib({"top.clk": _regular_clock(2, 5)})
    parser = _fake_parser(lib)
    monkeypatch.setattr(parser, "search_signals", lambda *a, **kw: {
        "results": [{"path": "top.clk", "width": 1}]})
    original = lib.fsdb_event_page_v1
    event = threading.Event()
    token = push_cancel_event(event)

    def interrupt(*args):
        rc = original(*args)
        if interruption == "cancel":
            event.set()
        else:
            args[-2]._obj.truncated, args[-2]._obj.complete = 1, 0
        return rc

    try:
        with monkeypatch.context() as patch:
            patch.setattr(lib, "fsdb_event_page_v1", interrupt)
            if interruption == "cancel":
                with pytest.raises(OperationCancelled):
                    server._detect_wave_clock(parser)
            else:
                assert server._detect_wave_clock(parser) == (None, None)
        assert getattr(parser, "_cached_clock_info", None) is None
        assert cache.snapshot()["entries"] == 0
        assert not lib.cursors and not parser._transition_group_active
        event.clear()
        assert server._detect_wave_clock(parser) == ("top.clk", 2)
        assert lib.calls.count("page") == 2
    finally:
        pop_cancel_event(token)
        parser.close()


@pytest.mark.eda_integration
@pytest.mark.parametrize("start,end,expected", [
    (0, 4, [1, 1, 2, 3]), (1, 1, [1, 1]), (2, 2, [2]), (3, 3, [3]), (4, 4, []),
])
def test_real_100fs_clock_preserves_public_ceil_edges_and_window_seed(
        reclaim_libraries, same_tick_fsdb, monkeypatch, cache, start, end, expected):
    with _parser(reclaim_libraries("records2"), same_tick_fsdb, monkeypatch) as parser:
        monkeypatch.setattr(parser, "get_transitions", lambda *a, **kw: pytest.fail("materialized clock"))
        edges, period, complete = _compact(parser, path="top.data", start=start, end=end)
        assert list(edges) == expected and complete
        assert period == (1 if (start, end) == (0, 4) else 0 if len(expected) == 2 else None)
        _assert_clean(parser)


@pytest.mark.eda_integration
def test_ten_million_clock_has_five_million_edges_and_late_global_cycles_without_cache(
        reclaim_libraries, monkeypatch, record_property):
    wave = os.environ.get("TW_LAZY_LARGE_FSDB")
    if not wave:
        pytest.skip("set TW_LAZY_LARGE_FSDB to activity_10000000.fsdb")
    cache = ClockEdgeCache()  # Production 8 MiB entry / 32 MiB process budgets.
    monkeypatch.setattr(storage, "cache", cache)
    with _parser(reclaim_libraries("default"), wave, monkeypatch) as parser:
        monkeypatch.setattr(parser, "get_transitions", lambda *a, **kw: pytest.fail("materialized clock"))
        result = _query(parser, start_cycle=4_999_990, num_cycles=10)
        assert result["total_edges_found"] == 5_000_000
        assert result["clock_edges_complete"] and result["clock_period_ps"] == 20
        assert result["start_cycle"] == 4_999_990 and result["num_cycles_returned"] == 10
        assert not result["truncated"]
        assert [(row["cycle"], row["time_ps"]) for row in result["cycles"]] == [
            (index, 20 * index + 10) for index in range(4_999_990, 5_000_000)]
        assert cache.snapshot() == {"entries": 0, "bytes": 0}
        stats = _snapshot(parser)
        assert stats["get_vc"] == 10_000_001
        assert 65536 <= stats["peak_records"] < 1_000_000
        for name, value in stats.items():
            record_property(name, value)
        _assert_clean(parser, ("top.clk", "top.data", "top.sparse", "top.wide"))
