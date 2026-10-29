#!/usr/bin/env python3
"""Check local Markdown links in tracked docs against the tracked repository.

No network requests are made. Ignored/untracked files (including local jobs/)
cannot satisfy a link. Fragments are checked against Markdown heading anchors;
non-Markdown fragments are left to their format's viewer.
"""
from __future__ import annotations

import argparse
import html
import posixpath
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit


@dataclass(frozen=True)
class Link:
    line: int
    target: str


def prose(text: str, *, blank_inline_code: bool = True) -> str:
    """Blank code/comments while preserving line numbers and string offsets."""
    text = re.sub(r"<!--[\s\S]*?-->", lambda m: re.sub(r"[^\n]", " ", m[0]), text)
    lines = text.splitlines(keepends=True)
    fence = None
    for index, line in enumerate(lines):
        found = re.match(r" {0,3}(`{3,}|~{3,})", line)
        if fence is None and found:
            fence = found[1]
            lines[index] = re.sub(r"[^\n]", " ", line)
        elif fence is not None:
            lines[index] = re.sub(r"[^\n]", " ", line)
            if found and found[1][0] == fence[0] and len(found[1]) >= len(fence):
                fence = None
        elif blank_inline_code:
            # Markdown code spans require matching delimiter lengths.
            lines[index] = re.sub(r"(`+)(.*?)\1(?!`)", lambda m: " " * len(m[0]), line)
    return "".join(lines)


def _destination(value: str) -> str:
    value = value.strip()
    if value.startswith("<"):
        return value[1:value.index(">")]
    # An optional quoted title follows whitespace, outside the destination.
    value = re.split(r"\s+[\"']", value, maxsplit=1)[0].strip()
    return re.sub(r"\\([\\() ])", r"\1", value)


def links(text: str) -> list[Link]:
    text = prose(text)
    found: list[Link] = []
    definitions: dict[str, str] = {}
    definition_ranges = []
    for match in re.finditer(r"(?m)^ {0,3}\[([^\]]+)\]:\s*(<[^>]+>|\S+)(?:[^\n]*)", text):
        definitions[" ".join(match[1].casefold().split())] = _destination(match[2])
        definition_ranges.append(match.span())
        found.append(Link(text.count("\n", 0, match.start()) + 1, _destination(match[2])))
    # Balanced parentheses allow ordinary source filenames such as a(b).md.
    index = 0
    while index < len(text):
        start = text.find("](", index)
        if start < 0:
            break
        cursor, depth, angle = start + 2, 1, False
        while cursor < len(text) and depth:
            char = text[cursor]
            if char == "\\":
                cursor += 2
                continue
            if char == "<":
                angle = True
            elif char == ">":
                angle = False
            elif not angle and char == "(":
                depth += 1
            elif not angle and char == ")":
                depth -= 1
            cursor += 1
        if depth == 0:
            try:
                target = _destination(text[start + 2:cursor - 1])
            except ValueError:
                target = "<malformed-link>"
            found.append(Link(text.count("\n", 0, start) + 1, target))
        index = max(cursor, start + 2)
    for match in re.finditer(r"\[([^\]\n]+)\]\[([^\]\n]*)\]", text):
        if any(start <= match.start() < end for start, end in definition_ranges):
            continue
        label = " ".join((match[2] or match[1]).casefold().split())
        found.append(Link(text.count("\n", 0, match.start()) + 1,
                          definitions.get(label, "<undefined-reference:" + label + ">")))
    for match in re.finditer(r"\b(?:href|src)\s*=\s*([\"'])(.*?)\1", text, re.IGNORECASE):
        found.append(Link(text.count("\n", 0, match.start()) + 1, html.unescape(match[2])))
    return found


def anchors(text: str) -> set[str]:
    # Keep inline code text when deriving GitHub-style heading slugs.
    result: set[str] = set()
    counts: dict[str, int] = {}
    filtered = re.sub(r"`+", "", prose(text, blank_inline_code=False))
    headings = []
    lines = filtered.splitlines()
    for index, line in enumerate(lines):
        match = re.match(r" {0,3}#{1,6}\s+(.*?)\s*#*\s*$", line)
        if match:
            headings.append(match[1])
        elif index + 1 < len(lines) and line.strip() and re.fullmatch(r" {0,3}(=+|-+)\s*", lines[index + 1]):
            headings.append(line.strip())
    for heading in headings:
        heading = html.unescape(re.sub(r"<[^>]*>", "", heading)).lower()
        slug = "".join(c for c in heading if c.isalnum() or c in "_ -").replace(" ", "-")
        count = counts.get(slug, 0)
        counts[slug] = count + 1
        result.add(slug + (f"-{count}" if count else ""))
    result.update(re.findall(r"\b(?:id|name)=[\"']([^\"']+)[\"']", text))
    return result


def tracked_files(root: Path) -> set[str]:
    output = subprocess.check_output(
        ["git", "-c", "core.fsmonitor=false", "ls-files", "--cached", "-z"], cwd=root)
    return set(output.decode("utf-8").rstrip("\0").split("\0")) - {""}


def check(root: Path, tracked: set[str] | None = None) -> tuple[int, list[str]]:
    root = root.resolve()
    tracked = tracked_files(root) if tracked is None else tracked
    errors: list[str] = []
    checked = 0
    anchor_cache: dict[str, set[str]] = {}
    for name in sorted(p for p in tracked if p.startswith("docs/") and p.endswith(".md")):
        source = root / name
        if any(parent.is_symlink() for parent in [source, *source.parents] if parent != root.parent):
            errors.append(f"{name}: source uses a symbolic link")
            continue
        if not source.is_file():
            errors.append(f"{name}: tracked source is missing")
            continue
        for link in links(source.read_text(encoding="utf-8")):
            url = urlsplit(link.target)
            if url.scheme in {"http", "https", "mailto", "data"} or link.target.startswith("//"):
                continue
            checked += 1
            prefix = f"{name}:{link.line}: {link.target}: "
            decoded = unquote(url.path)
            if url.scheme or decoded.startswith("/") or "\\" in decoded or "\0" in decoded:
                errors.append(prefix + "absolute or unsupported local target")
                continue
            target = posixpath.normpath(posixpath.join(posixpath.dirname(name), decoded)) if decoded else name
            if target == ".." or target.startswith("../"):
                errors.append(prefix + "target escapes repository")
                continue
            # Repository directories are valid only when they contain tracked files.
            directory = any(p.startswith(target.rstrip("/") + "/") for p in tracked)
            if target not in tracked and not directory:
                errors.append(prefix + "target is not tracked")
                continue
            path = root / target
            if any(parent.is_symlink() for parent in [path, *path.parents] if parent != root.parent):
                errors.append(prefix + "symbolic-link target is unsupported")
                continue
            if not path.exists():
                errors.append(prefix + "tracked target is missing")
                continue
            if url.fragment and target.endswith(".md"):
                if target not in anchor_cache:
                    anchor_cache[target] = anchors(path.read_text(encoding="utf-8"))
                if unquote(url.fragment) not in anchor_cache[target]:
                    errors.append(prefix + "Markdown heading anchor is absent")
    return checked, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    count, errors = check(args.root)
    for error in errors:
        print(error)
    print(f"Checked {count} local documentation links; {len(errors)} error(s).")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
