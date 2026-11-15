#!/usr/bin/env python3
"""Check local Markdown paths and GitHub-style heading/HTML anchors, offline."""
import html
import pathlib
import re
import sys
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parent.parent


def prose(text):
    """Preserve line numbers while excluding fenced examples and comments."""
    fence = None
    lines = []
    for line in text.splitlines():
        match = re.match(r"\s{0,3}(`{3,}|~{3,})", line)
        if match:
            marker = match[1]
            if fence is None:
                fence = marker
            elif marker[0] == fence[0] and len(marker) >= len(fence):
                fence = None
            lines.append("")
        else:
            lines.append("" if fence else line)
    return re.sub(r"<!--.*?-->", lambda m: "\n" * m[0].count("\n"),
                  "\n".join(lines), flags=re.S)


def anchors(path):
    text = prose(path.read_text())
    found = set(re.findall(r'<(?:a|h[1-6])\b[^>]*\b(?:id|name)=[\'"]([^\'"]+)', text))
    seen = {}
    for heading in re.findall(r"^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$", text, re.M):
        heading = re.sub(r"!?\[([^]]+)\]\([^)]*\)", r"\1", heading)
        heading = html.unescape(re.sub(r"<[^>]+>", "", heading)).lower()
        slug = re.sub(r"[^\w\- ]", "", heading).replace(" ", "-")
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        found.add(slug + (f"-{count}" if count else ""))
    return found


def check():
    paths = sorted(set(ROOT.glob("*.md")) | set((ROOT / "docs").rglob("*.md")) |
                   set((ROOT / "skills").rglob("*.md")))
    cache = {}
    errors = []
    checked = 0
    for path in paths:
        text = prose(path.read_text())
        # Inline code can contain example [indexes][like_this], not links.
        text = re.sub(r"(`+)(.*?)\1", lambda m: " " * len(m[0]), text)
        definitions = dict((m[1].lower(), m[2]) for m in re.finditer(
            r"^ {0,3}\[([^]]+)\]:\s*<?([^\s>]+)>?", text, re.M))
        links = [(m.start(), m[1]) for m in re.finditer(
            r"!?\[[^\n]*?\]\(<?([^\s)>]+)>?(?:\s+[^)]*)?\)", text)]
        links += [(m.start(), m[1]) for m in re.finditer(r'\bhref=[\'"]([^\'"]+)', text)]
        links += [(m.start(), m[2]) for m in re.finditer(
            r"^ {0,3}\[([^]]+)\]:\s*<?([^\s>]+)>?", text, re.M)]
        for m in re.finditer(r"\[([^]\n]+)\]\[([^]\n]*)\]", text):
            key = (m[2] or m[1]).lower()
            links.append((m.start(), definitions.get(key, "MISSING-REFERENCE:" + key)))
        for offset, raw in links:
            url = urllib.parse.urlsplit(html.unescape(raw))
            if url.scheme or url.netloc:
                if raw.startswith("MISSING-REFERENCE:"):
                    errors.append(f"{path.relative_to(ROOT)}: undefined reference {raw}")
                continue
            checked += 1
            target = (ROOT / urllib.parse.unquote(url.path).lstrip("/") if url.path.startswith("/")
                      else path.parent / urllib.parse.unquote(url.path)) if url.path else path
            target = target.resolve()
            reason = None
            if not target.exists():
                reason = "missing path"
            elif url.fragment and target.suffix == ".md":
                if target not in cache:
                    cache[target] = anchors(target)
                if urllib.parse.unquote(url.fragment) not in cache[target]:
                    reason = "missing anchor"
            if reason:
                line = text.count("\n", 0, offset) + 1
                errors.append(f"{path.relative_to(ROOT)}:{line}: {reason}: {raw}")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Internal links: {checked} resolved across {len(paths)} Markdown files")
    return 0


if __name__ == "__main__":
    sys.exit(check())
