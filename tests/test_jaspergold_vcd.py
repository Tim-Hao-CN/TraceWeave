from __future__ import annotations

from pathlib import Path

import pytest

import server
from src.vcd_parser import VCDParser
from src.waveform_hints import annotate_signal_search_result


_FIXTURE = Path(__file__).parent / "fixtures" / "jaspergold" / "jaspergold_wave.vcd"


def test_summary_reports_normalized_producer_without_raw_version():
    summary = VCDParser(str(_FIXTURE)).get_summary()

    assert summary["producer_hint"] == "jaspergold"
    assert summary["producer_evidence"] == "vcd_version_header"
    assert "version" not in summary
    assert "JasperGold" not in summary.values()


def test_unknown_vcd_version_has_no_producer_hint(tmp_path: Path):
    wave = tmp_path / "unknown.vcd"
    wave.write_text(
        "$version Other Formal Tool 1.2 $end\n"
        "$timescale 1ns $end\n"
        "$scope module top $end\n"
        "$var wire 1 ! clk $end\n"
        "$upscope $end\n"
        "$enddefinitions $end\n"
        "#0\n0!\n",
        encoding="utf-8",
    )

    summary = VCDParser(str(wave)).get_summary()

    assert summary["producer_hint"] is None
    assert summary["producer_evidence"] is None


def test_search_marks_only_exact_jaspergold_pseudo_signals():
    result = VCDParser(str(_FIXTURE)).search_signals("jasper")
    by_name = {row["name"]: row for row in result["results"]}

    assert by_name[":jasper_formal_clock"]["is_tool_pseudo_signal"] is True
    assert by_name[":jasper_formal_clock"]["tool_pseudo_role"] == "formal_clock"
    assert by_name[":jasper_formal_reset"]["tool_pseudo_role"] == "formal_reset"
    assert "is_tool_pseudo_signal" not in by_name["jasper_formal_clock_copy"]


def test_common_search_annotation_also_covers_non_vcd_backends():
    result = annotate_signal_search_result(
        {
            "keyword": "clock",
            "total_matched": 1,
            "results": [
                {
                    "path": "top.:jasper_formal_clock",
                    "name": ":jasper_formal_clock",
                    "width": 1,
                }
            ],
        }
    )

    assert result["results"][0]["tool_pseudo_role"] == "formal_clock"


def test_automatic_clock_prefers_real_rtl_clock_over_denser_pseudo_clock():
    parser = VCDParser(str(_FIXTURE))

    clock_path, period_ps = server._detect_wave_clock(parser)

    assert clock_path == "formal_top.clk"
    assert period_ps == 10_000


@pytest.mark.anyio
async def test_public_waveform_tools_keep_pseudo_clock_explicitly_usable():
    summary = await server._dispatch(
        "get_waveform_summary", {"wave_path": str(_FIXTURE)}
    )
    search = await server._dispatch(
        "search_signals", {"wave_path": str(_FIXTURE), "keyword": "jasper"}
    )
    cycles = await server._dispatch(
        "get_signals_by_cycle",
        {
            "wave_path": str(_FIXTURE),
            "clock_path": "formal_top.:jasper_formal_clock",
            "signal_paths": ["formal_top.state"],
            "num_cycles": 3,
            "sample_offset_ps": 0,
        },
    )

    assert summary.producer_hint == "jaspergold"
    assert any(row["is_tool_pseudo_signal"] for row in search.results)
    assert cycles.clock_path == "formal_top.:jasper_formal_clock"
    assert cycles.num_cycles_returned == 3
    assert "trace_kind" not in type(summary).model_fields
