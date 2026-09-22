"""Both supported platforms must get a command their shell can run."""

from __future__ import annotations

from src.reproduction_commands import SUPPORTED, UNIX, WINDOWS, command_for_profile, summary_line


def test_windows_is_offered_a_powershell_entry_point():
    """A bash-only instruction sends a Windows reviewer to a dead end."""
    assert WINDOWS.command.endswith("reproduce.ps1")
    assert WINDOWS.shell == "powershell"


def test_each_platform_uses_its_own_flag_syntax():
    assert command_for_profile(UNIX, "security-20-v4").endswith("--profile security-20-v4")
    assert command_for_profile(WINDOWS, "security-20-v4").endswith("-Profile security-20-v4")


def test_a_missing_profile_falls_back_to_the_bare_command():
    for command in SUPPORTED:
        assert command_for_profile(command, None) == command.command


def test_the_summary_names_both_platforms():
    line = summary_line()
    for command in SUPPORTED:
        assert command.platform in line
