from __future__ import annotations
import json
from pathlib import Path

ROOT = Path(__file__).with_name("upstream")


def catalogue():
    lock = json.loads((ROOT / "source.json").read_text(encoding="utf-8"))
    return {
        "source": lock,
        "skills": [p.parent.name for p in sorted((ROOT / "skills").glob("*/SKILL.md"))],
    }


def relevant_guidance(message):
    # Small, transparent router; never execute vendored scripts or register MCP.
    selections = []
    text = message.lower()
    for words, skill in [
        (("workspace", "discover", "inspect"), "search-consumption-cli"),
        (("spark", "notebook", "lakehouse"), "spark-cli"),
        (("warehouse", "sql"), "sqldw-cli"),
        (("bronze", "silver", "medallion"), "e2e-medallion-architecture"),
    ]:
        if any(word in text for word in words):
            path = ROOT / "skills" / skill / "SKILL.md"
            if path.is_file():
                selections.append(
                    "Reference guidance for "
                    + skill
                    + ":\n"
                    + path.read_text(encoding="utf-8")[:10000]
                )
        if len(selections) == 2:
            break
    if any(word in text for word in ("incident", "failure", "root cause", "outage")):
        selections.append(
            (Path(__file__).with_name("playbooks") / "incident.md").read_text(
                encoding="utf-8"
            )
        )
    return "\n\n".join(selections)
