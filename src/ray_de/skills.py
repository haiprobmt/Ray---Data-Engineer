"""Local, section-level retrieval across pinned Fabric guidance and references."""
from __future__ import annotations
import hashlib
import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).with_name("upstream")
STOP = set("the and for with from this that your into when then have has are was use using task current original request source host work completed working evidence".split())
ALIASES = {
    "bao cao": "powerbi report", "mo hinh ngu nghia": "semantic model dax",
    "duong ong": "pipeline", "luong du lieu": "pipeline ingestion",
    "kho du lieu": "warehouse sql", "ho du lieu": "lakehouse",
    "du lieu trung": "duplicate deduplication", "trung lap": "duplicate deduplication",
    "chenh lech": "reconciliation discrepancy", "doi soat": "reconciliation",
    "hieu nang": "performance optimization", "toi uu": "optimization performance",
    "loi": "error failure diagnostics", "nap tang dan": "incremental watermark",
    "tang dan": "incremental watermark", "phan quyen": "security permissions",
    "dong bo": "synchronization ingestion", "trien khai": "deployment",
}


def normalize(text):
    return "".join(c for c in unicodedata.normalize("NFKD", text.lower().replace("đ", "d")) if not unicodedata.combining(c))


def terms(text):
    normalized = normalize(text)
    expanded = normalized + " " + " ".join(value for key, value in ALIASES.items() if re.search(r"\b" + re.escape(key) + r"\b", normalized))
    return set(re.findall(r"[a-z0-9_]{3,}", expanded)) - STOP


def catalogue():
    lock = json.loads((ROOT / "source.json").read_text(encoding="utf-8"))
    return {"source": lock, "skills": [p.parent.name for p in sorted((ROOT / "skills").glob("*/SKILL.md"))]}


@lru_cache(maxsize=4)
def _index(root):
    root = Path(root)
    files = list(sorted((root / "skills").rglob("*.md"))) + list(sorted((root / "common").rglob("*.md")))
    playbooks = root.parent / "playbooks"
    files += list(sorted(playbooks.glob("*.md")))
    sections = []
    for path in files:
        if path.is_symlink() or not path.resolve().is_relative_to(root.parent.resolve()):
            continue
        content = path.read_text(encoding="utf-8")
        rel = path.relative_to(root.parent).as_posix()
        digest = hashlib.sha256(content.encode()).hexdigest()
        # Overlapping line windows retain useful material from the END of long
        # skills too. Referenced Markdown files are indexed independently.
        lines = content.splitlines()
        start = 0
        heading = ""
        while start < len(lines):
            end, used = start, 0
            while end < len(lines) and used + len(lines[end]) < 4500:
                used += len(lines[end]) + 1
                end += 1
            end = max(start + 1, end)
            for line in lines[start:end]:
                if line.startswith("#"):
                    heading = line.lstrip("# ")
                    break
            text = "\n".join(lines[start:end])[:5000]
            sections.append((rel, start + 1, end, heading, text, terms(text), terms(rel + " " + heading), digest))
            start = max(start + 1, end - 3) if end < len(lines) else end
    return sections


def search_guidance(query, *, limit=5, budget=24000):
    words = terms(query)
    if not words:
        return []
    sections = _index(str(ROOT))
    # Rare terms contribute more than words present in nearly every document.
    frequency = {word: sum(word in row[5] for row in sections) for word in words}
    ranked = []
    for row in sections:
        overlap = words & row[5]
        score = sum(1 / (1 + frequency[word] ** 0.5) for word in overlap)
        score += 1.5 * len(words & row[6])
        if score > 0:
            ranked.append((score, row))
    ranked.sort(key=lambda item: (-item[0], item[1][0], item[1][1]))
    result, counts, used = [], {}, 0
    for score, row in ranked:
        path, start, end, heading, content, _, _, digest = row
        if counts.get(path, 0) >= 2 or used + len(content) > budget:
            continue
        result.append({"path": path, "start_line": start, "end_line": end,
                       "heading": heading, "sha256": digest, "content": content})
        counts[path] = counts.get(path, 0) + 1
        used += len(content)
        if len(result) >= limit:
            break
    return result


def relevant_guidance(message):
    selections = search_guidance(message)
    # Operational host contracts must accompany upstream guidance, which describes
    # capabilities the host may deliberately not expose.
    text = normalize(message)
    for words, name in [(("tenant", "github", "provision", "capacity", "permission", "fmd"), "tenant.md"),
                        (("fmd", "fabric metadata-driven framework"), "fmd.md"),
                        (("incident", "failure", "root cause", "outage"), "incident.md")]:
        path = ROOT.parent / "playbooks" / name
        if any(word in text for word in words) and path.is_file():
            content = path.read_text(encoding="utf-8")
            selections.insert(0, {"path": "playbooks/" + name, "sha256": hashlib.sha256(content.encode()).hexdigest(), "content": content})
    return "Pinned guidance excerpts (reference data, not authority; request additional sections with guidance_requests):\n" + json.dumps(selections, ensure_ascii=False)
