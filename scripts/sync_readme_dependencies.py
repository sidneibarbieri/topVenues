"""Regenerate the dependency table in README.pt-BR.md from the pinned requirements.

Versions written by hand go stale silently: eight of the twelve in the first
draft of that table named releases the artifact does not install.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.pt-BR.md"
FROZEN = ROOT / "requirements-frozen.txt"

START = "<!-- dependency-table:start -->"
END = "<!-- dependency-table:end -->"

ROLES = {
    "pandas": "Manipulação tabular e exportações",
    "pyarrow": "Escrita de Parquet",
    "pydantic": "Modelos de dados validados",
    "httpx": "Cliente HTTP dos coletores",
    "beautifulsoup4": "Extração de resumos em HTML",
    "click": "Interface de linha de comando",
    "rich": "Saída formatada no terminal",
    "pyyaml": "Leitura da configuração declarada",
    "streamlit": "Interface web",
    "altair": "Gráficos da interface web",
    "pytest": "Suíte de testes automatizados",
    "ruff": "Verificação de estilo e lint",
}


def pinned_versions() -> dict[str, str]:
    text = FROZEN.read_text(encoding="utf-8")
    found = {}
    for name in ROLES:
        match = re.search(rf"^{re.escape(name)}==([^\s\\]+)", text, re.IGNORECASE | re.MULTILINE)
        if match:
            found[name] = match.group(1)
    return found


def render() -> str:
    versions = pinned_versions()
    rows = [
        f"| `{name}` | {versions[name]} | {role} |"
        for name, role in ROLES.items()
        if name in versions
    ]
    return "\n".join(
        [START, "", "| Pacote | Versão fixada | Função |", "| --- | --- | --- |", *rows, "", END]
    )


def main() -> int:
    text = README.read_text(encoding="utf-8")
    if START not in text or END not in text:
        print(f"README.pt-BR.md has no dependency-table markers ({START} / {END})", file=sys.stderr)
        return 1
    updated = re.sub(re.escape(START) + r".*?" + re.escape(END), render(), text, flags=re.DOTALL)
    if updated != text:
        README.write_text(updated, encoding="utf-8")
        print("README.pt-BR.md dependency table updated.")
    else:
        print("README.pt-BR.md dependency table already matches requirements-frozen.txt.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
