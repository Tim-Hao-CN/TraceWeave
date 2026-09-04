from __future__ import annotations

from pathlib import Path

import pytest

from src.formal_path_discovery import FormalDiscoveryLimits, discover_formal_paths


def _write(path: Path, content: str = "placeholder\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _build_project(root: Path, name: str = "jg_run") -> tuple[Path, Path, Path]:
    project = root / name
    console = _write(project / "jg_console.log")
    session = _write(project / "sessionLogs" / "session_0" / "jg_session_0.log")
    _write(project / "sessionLogs" / "session_0" / "jg_session_0.cmd")
    _write(project / "bridge.log")
    return project, console, session


def test_discovers_current_jaspergold_project_and_log_roles(tmp_path: Path):
    project, console, session = _build_project(tmp_path)
    wave = _write(project / "exports" / "witness.vcd", "$timescale 1ns $end\n")

    result = discover_formal_paths(str(tmp_path))

    assert result["discovery_mode"] == "formal_root"
    assert result["detected_formal_tools"] == ["jaspergold"]
    assert result["projects"] == [
        {
            "path": str(project.resolve()),
            "detected_formal_tool": "jaspergold",
            "project_layout_state": "session_present",
            "detection_evidence": [
                "bridge_log",
                "current_session_command",
                "current_session_log",
                "jg_console_log",
            ],
        }
    ]
    roles = {entry["path"]: entry["role"] for entry in result["formal_logs"]}
    assert roles[str(console.resolve())] == "console"
    assert roles[str(session.resolve())] == "session"
    assert set(roles.values()) == {"command", "console", "infrastructure", "session"}
    assert result["wave_files"][0]["path"] == str(wave.resolve())
    assert result["available_runs"] == [
        {
            "name": "jg_run",
            "dir": str(project.resolve()),
            "detected_formal_tool": "jaspergold",
            "has_wave": True,
        }
    ]
    assert result["coverage"]["status"] == "complete"


def test_direct_project_root_is_classified_without_proof_semantics(tmp_path: Path):
    project, _console, _session = _build_project(tmp_path)

    result = discover_formal_paths(str(project))

    assert result["discovery_mode"] == "formal_project_dir"
    assert result["projects"][0]["path"] == str(project.resolve())
    assert not ({"property_status", "trace_kind", "reachability_basis"} & result.keys())


def test_console_only_project_is_markers_only(tmp_path: Path):
    project = tmp_path / "failed_launch"
    _write(project / "jg_console.log")

    result = discover_formal_paths(str(tmp_path))

    assert result["projects"][0]["project_layout_state"] == "markers_only"
    assert result["projects"][0]["detection_evidence"] == ["jg_console_log"]


def test_weak_marker_alone_does_not_identify_project(tmp_path: Path):
    _write(tmp_path / "maybe" / "bridge.log")
    _write(tmp_path / "maybe" / ".tmp" / ".initCmds.tcl")

    result = discover_formal_paths(str(tmp_path))

    assert result["discovery_mode"] == "unknown"
    assert result["projects"] == []
    assert result["formal_logs"] == []


def test_multiple_projects_are_deterministic(tmp_path: Path):
    second, *_ = _build_project(tmp_path, "z_run")
    first, *_ = _build_project(tmp_path, "a_run")

    result = discover_formal_paths(str(tmp_path))

    assert [entry["path"] for entry in result["projects"]] == [
        str(first.resolve()),
        str(second.resolve()),
    ]
    assert [entry["name"] for entry in result["available_runs"]] == ["a_run", "z_run"]


def test_backup_sessions_and_databases_are_excluded(tmp_path: Path):
    project, *_ = _build_project(tmp_path)
    backup = _write(project / "sessionLogs.bak" / "session_0" / "jg_session_0.log")
    apdb = _write(project / "sessionLogs" / "session_0" / "work" / "database.apdb")
    ddk = _write(project / "sessionLogs" / "session_0" / "work" / "database.ddk")
    profile = _write(
        project / "sessionLogs" / "session_0" / "profile" / "public" / "state"
    )
    preference = _write(
        project / "sessionLogs" / "session_0" / "settings" / "preferences.conf"
    )

    result = discover_formal_paths(str(tmp_path))
    returned_paths = {
        entry["path"] for entry in result["formal_logs"] + result["wave_files"]
    }

    assert str(backup.resolve()) not in returned_paths
    assert str(apdb.resolve()) not in returned_paths
    assert str(ddk.resolve()) not in returned_paths
    assert str(profile.resolve()) not in returned_paths
    assert str(preference.resolve()) not in returned_paths
    assert result["coverage"]["status"] == "complete"


def test_wave_only_does_not_infer_semantics_from_filename(tmp_path: Path):
    wave = _write(tmp_path / "cex.vcd", "$version JasperGold $end\n")

    result = discover_formal_paths(str(tmp_path))

    assert result["discovery_mode"] == "wave_only"
    assert result["detected_formal_tools"] == []
    assert result["wave_files"][0]["path"] == str(wave.resolve())
    assert "cex" not in result
    assert "trace_kind" not in result


def test_explicit_project_limits_automatic_discovery(tmp_path: Path):
    selected, *_ = _build_project(tmp_path, "selected")
    _build_project(tmp_path, "other")
    wave = _write(selected / "selected.fsdb")

    result = discover_formal_paths(str(tmp_path), project_dir="selected")

    assert result["discovery_mode"] == "explicit"
    assert result["selected_project_dir"] == str(selected.resolve())
    assert [item["path"] for item in result["projects"]] == [str(selected.resolve())]
    assert [item["path"] for item in result["wave_files"]] == [str(wave.resolve())]


def test_explicit_unclassified_log_is_returned_without_project_guess(tmp_path: Path):
    log = _write(tmp_path / "custom" / "formal-output.txt")

    result = discover_formal_paths(str(tmp_path), formal_log=str(log))

    assert result["formal_logs"][0]["role"] == "unclassified"
    assert result["formal_logs"][0]["project_dir"] is None


def test_explicit_unverified_project_is_not_labeled_as_jaspergold(tmp_path: Path):
    project = tmp_path / "custom_run"
    project.mkdir()

    result = discover_formal_paths(
        str(tmp_path), formal_tool="JasperGold", project_dir="custom_run"
    )

    assert result["requested_formal_tool"] == "jaspergold"
    assert result["projects"][0]["detected_formal_tool"] is None
    assert result["projects"][0]["project_layout_state"] == "explicit_unverified"
    assert any("no supported provider markers" in hint for hint in result["hints"])


def test_explicit_paths_must_stay_inside_formal_root(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    outside = _write(tmp_path / "outside.vcd")

    with pytest.raises(ValueError, match="outside formal_root"):
        discover_formal_paths(str(root), wave_file=str(outside))


def test_outside_symlink_is_skipped_and_degrades_coverage(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    outside = _write(tmp_path / "outside.vcd")
    (root / "external.vcd").symlink_to(outside)

    result = discover_formal_paths(str(root))

    assert result["wave_files"] == []
    assert result["coverage"]["status"] == "degraded"
    assert result["coverage"]["degradation_reasons"] == ["outside_root_symlink"]


def test_in_root_symlink_cycle_terminates(tmp_path: Path):
    project, *_ = _build_project(tmp_path)
    (project / "loop").symlink_to(tmp_path, target_is_directory=True)

    result = discover_formal_paths(str(tmp_path))

    assert len(result["projects"]) == 1
    assert result["coverage"]["status"] == "complete"


def test_depth_and_entry_limits_report_truncation(tmp_path: Path):
    _build_project(tmp_path)
    limits = FormalDiscoveryLimits(
        max_depth=1,
        max_entries=2,
        max_projects=8,
        max_files_per_role=8,
    )

    result = discover_formal_paths(str(tmp_path), limits=limits)

    assert result["coverage"]["status"] == "truncated"
    assert result["coverage"]["truncation_reasons"]


def test_project_and_file_caps_report_exact_reason(tmp_path: Path):
    _build_project(tmp_path, "a")
    _build_project(tmp_path, "b")
    limits = FormalDiscoveryLimits(
        max_depth=5,
        max_entries=128,
        max_projects=1,
        max_files_per_role=1,
    )

    result = discover_formal_paths(str(tmp_path), limits=limits)

    assert result["coverage"]["status"] == "truncated"
    assert "max_projects" in result["coverage"]["truncation_reasons"]


def test_proprietary_database_cannot_be_selected_explicitly(tmp_path: Path):
    database = _write(tmp_path / "database.apdb")

    with pytest.raises(ValueError, match="proprietary proof database"):
        discover_formal_paths(str(tmp_path), formal_log=str(database))


def test_unsupported_provider_is_rejected(tmp_path: Path):
    with pytest.raises(ValueError, match="unsupported formal_tool"):
        discover_formal_paths(str(tmp_path), formal_tool="vc_formal")
