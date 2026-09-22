"""The reproduction command, stated for the platform the reader is on.

The repository ships `reproduce.sh` and `reproduce.ps1`, and continuous
integration exercises both. Printing only the bash form sends a reviewer on
Windows to a command their shell cannot run, which is a failure of the
artifact rather than of their setup.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReproductionCommand:
    """One platform's entry point into the reproduction script."""

    platform: str
    shell: str
    command: str


UNIX = ReproductionCommand(
    platform="macOS and Linux",
    shell="bash",
    command="bash reproduce.sh",
)
WINDOWS = ReproductionCommand(
    platform="Windows",
    shell="powershell",
    command=".\\reproduce.ps1",
)

SUPPORTED = (UNIX, WINDOWS)


def command_for_profile(command: ReproductionCommand, profile_id: str | None) -> str:
    """The command a reader should run to reproduce a named profile."""
    if not profile_id:
        return command.command
    return (
        f"{command.command} -Profile {profile_id}"
        if command is WINDOWS
        else f"{command.command} --profile {profile_id}"
    )


def summary_line() -> str:
    """A one-line statement of both entry points, for tables and captions."""
    return " · ".join(f"{item.platform}: {item.command}" for item in SUPPORTED)
