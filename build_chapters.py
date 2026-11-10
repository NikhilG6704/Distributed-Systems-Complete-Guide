#!/usr/bin/env python3
"""Render the distributed systems Markdown collection into linked chapter pages."""
from __future__ import annotations

import html
import json
import re
import shutil
import subprocess
from urllib.parse import quote, unquote
from pathlib import Path

import bleach

ROOT = Path(__file__).resolve().parent
CHAPTERS = json.loads((ROOT / "chapters.json").read_text())
OUT = ROOT / "chapters"
OUT.mkdir(exist_ok=True)
PANDOC = shutil.which("pandoc") or "/opt/anaconda3/bin/pandoc"


def summary_for(chapter: dict) -> str:
    raw = (ROOT / chapter["file"]).read_text(encoding="utf-8", errors="replace")
    purpose = re.search(
        r"^>\s*\*\*(?:Purpose\.?|Scope:?)\*\*\s*(.+)$",
        raw,
        flags=re.M | re.I,
    )
    if purpose:
        value = re.sub(r"[*_`#]", "", purpose.group(1)).strip()
        if len(value) > 190:
            value = value[:190].rsplit(" ", 1)[0] + "…"
        return value
    candidates = []
    for block in re.split(r"\n\s*\n", raw):
        value = block.strip()
        if not value or value.startswith("#") or value.startswith("```") or value.startswith("|"):
            continue
        if re.match(r"^(?:[-*+]\s|\d+[.)]\s)", value):
            continue
        if value.startswith(">"):
            value = re.sub(r"^\s*>\s?", "", value, flags=re.M)
        if re.match(r"\*\*(?:Document|Level|Scope|Status|Version|Last updated):\*\*", value, re.I):
            continue
        if re.match(r"(?:After (?:studying|completing) this chapter, you should be able to|You should be comfortable with|The reader should be comfortable with):?", value, re.I):
            continue
        value = re.sub(r"!?\[([^]]*)\]\([^)]*\)", r"\1", value)
        value = re.sub(r"[*_`#]", "", value)
        value = re.sub(r"\s+", " ", value).strip()
        if len(value) > 45:
            candidates.append(value)
        if len(" ".join(candidates)) > 200:
            break
    summary = " ".join(candidates)
    if not summary:
        return chapter.get("summary", "A visual guide to the ideas and trade-offs behind distributed systems.")
    summary = summary[:190].rsplit(" ", 1)[0]
    if len(" ".join(candidates)) > 190:
        summary += "…"
    return summary


for item in CHAPTERS:
    item["summary"] = summary_for(item)

ALLOWED_TAGS = set(bleach.sanitizer.ALLOWED_TAGS) | {
    "p", "div", "span", "h1", "h2", "h3", "h4", "h5", "h6", "hr",
    "pre", "code", "blockquote", "ul", "ol", "li", "table", "thead",
    "tbody", "tr", "th", "td", "img", "del", "sup", "sub", "details",
    "summary", "figure", "figcaption", "br", "kbd", "dl", "dt", "dd",
    "nav", "math", "semantics", "mrow", "mi", "mn", "mo", "mfrac", "msub",
    "msup", "msubsup", "munder", "mover", "munderover", "msqrt", "mroot",
    "mtext", "mspace", "menclose", "mtable", "mtr", "mtd", "mpadded",
    "mphantom", "mstyle", "merror", "mglyph", "annotation",
}
ALLOWED_ATTRS = {
    "*": ["id", "class", "title"],
    "a": ["href", "title"],
    "img": ["src", "alt", "title", "width", "height"],
    "th": ["align"],
    "td": ["align"],
    "math": ["xmlns", "display", "mathvariant"],
    "annotation": ["encoding"],
    "mo": ["stretchy", "fence", "separator", "lspace", "rspace"],
    "mfrac": ["linethickness"],
    "munder": ["accentunder"],
    "mover": ["accent"],
    "munderover": ["accent", "accentunder"],
}


def source_text(chapter: dict) -> str:
    path = ROOT / chapter["file"]
    text = path.read_text(encoding="utf-8", errors="replace")
    folder = path.parent.name
    # Pandoc MathML has a native negated implication entity, while the source
    # spelling used in one chapter relied on the amsmath centernot package.
    text = text.replace(r"\centernot\implies", r"\not\implies")
    # Move relative links from the source Markdown directory to its preserved
    # copy under content/, since the rendered pages live one directory above.
    def local_target(match: re.Match) -> str:
        target = match.group(2)
        if re.match(r"^(?:https?://|mailto:|#|/)", target, re.I):
            return match.group(0)
        decoded = unquote(target)
        candidate = path.parent / decoded
        if candidate.is_file():
            relative = candidate.relative_to(path.parent)
        else:
            matches = list(path.parent.rglob(Path(decoded).name))
            relative = matches[0].relative_to(path.parent) if len(matches) == 1 else Path(decoded)
        portable = quote(relative.as_posix(), safe="/-._~")
        return match.group(1) + "../content/" + folder + "/" + portable

    text = re.sub(r"(\]\(|^\[[^\]]+\]:\s*)([^)\s]+)", local_target, text, flags=re.MULTILINE)
    return text


def sidebar(active: str) -> str:
    items = []
    for index, item in enumerate(CHAPTERS, 1):
        cls = "chapter-link active" if item["id"] == active else "chapter-link"
        label = re.sub(r"^\d+\.\s*", "", item["title"])
        items.append(
            f'<a class="{cls}" href="{item["id"]}.html">'
            f'<span class="num">{index:02}</span><span>{html.escape(label)}</span></a>'
        )
    return "\n".join(items)


def render(index: int, chapter: dict) -> str:
    text = source_text(chapter)
    converted = subprocess.run(
        [PANDOC, "--from=markdown-yaml_metadata_block+tex_math_dollars+tex_math_single_backslash", "--to=html5", "--standalone", "--toc", "--toc-depth=3", "--mathml"],
        input=text,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    body_match = re.search(r"<body[^>]*>(.*?)</body>", converted, flags=re.S | re.I)
    body_html = body_match.group(1) if body_match else converted
    toc_match = re.search(r"<nav\s+id=\"TOC\".*?</nav>", body_html, flags=re.S | re.I)
    toc_html = toc_match.group(0) if toc_match else "<ul></ul>"
    if toc_match:
        body_html = body_html[:toc_match.start()] + body_html[toc_match.end():]
    toc_html = re.sub(r"^<nav[^>]*>(.*)</nav>$", r"\1", toc_html, flags=re.S | re.I)
    body = bleach.clean(
        body_html, tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRS,
        protocols={"http", "https", "mailto"}, strip=True,
    )
    toc = bleach.clean(
        toc_html or "<p>Sections appear as you read.</p>",
        tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRS,
        protocols={"http", "https", "mailto"}, strip=True,
    )
    toc = re.sub(r"^\s*<div class=\"toc\">(.*)</div>\s*$", r"\1", toc, flags=re.S)
    plain_title = re.sub(r"^\d+\.\s*", "", chapter["title"])
    plain_title = re.sub(r"^Chapter\s+\d+\s*[:—-]?\s*", "", plain_title, flags=re.I)
    prev_link = (
        f'<a href="{CHAPTERS[index-1]["id"]}.html"><span>← PREVIOUS CHAPTER</span>'
        f'<b>{html.escape(re.sub(r"^\d+\.\s*", "", CHAPTERS[index-1]["title"]))}</b></a>'
        if index else "<span></span>"
    )
    next_link = (
        f'<a href="{CHAPTERS[index+1]["id"]}.html"><span>NEXT CHAPTER →</span>'
        f'<b>{html.escape(re.sub(r"^\d+\.\s*", "", CHAPTERS[index+1]["title"]))}</b></a>'
        if index + 1 < len(CHAPTERS) else ""
    )
    words = len(re.findall(r"\S+", text))
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#101411"><meta name="description" content="{html.escape(chapter['summary'])}">
<title>{html.escape(plain_title)} · Distributed Systems Fieldnotes</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600;700&family=Newsreader:opsz,wght@6..72,400;6..72,500;6..72,600&display=swap" rel="stylesheet">
<link rel="stylesheet" href="../styles.css"><link rel="stylesheet" href="../chapter-tools.css"><link rel="stylesheet" href="../visual-tools.css"><link rel="stylesheet" href="../light-mode.css">
<style>.standalone-page{{max-width:1380px;margin:auto;padding:0 clamp(24px,5vw,76px)}}.chapter-home{{color:var(--acid);font:9px var(--mono);letter-spacing:.12em;text-decoration:none}}.chapter-home:hover{{text-decoration:underline}}.article-deck{{max-width:66ch}}.article-header h1{{max-width:21ch}}.article-header .article-deck{{max-width:65ch}}.toc{{position:sticky;top:105px}}.toc>ul{{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:3px}}.toc>ul li{{margin:0;padding:0}}.toc>ul a{{display:block;text-decoration:none;font:10px/1.4 var(--sans);color:#828d7e;padding:5px 7px 5px 0}}.toc>ul a:hover{{color:var(--acid)}}.toc>ul ul{{list-style:none;padding:0 0 0 10px}}.toc-bottom{{margin-top:18px}}math[display="block"]{{display:block;max-width:100%;overflow-x:auto;text-align:center;margin:1.35em 0;padding:8px 0;color:#f1f4e9}}.side-toggle{{width:auto;padding:0 10px;font-size:11px;color:#a0ab98;border:1px solid #303a31}}.side-toggle:hover{{color:var(--acid)}}.site-footer{{display:flex;justify-content:space-between;border-top:1px solid var(--line);padding:19px 0 28px;color:#687366;font:8px var(--mono);letter-spacing:.12em}}@media(max-width:850px){{.reader-layout{{display:block}}.toc{{position:static;max-height:none;overflow:visible;border:1px solid var(--line);border-radius:6px;padding:17px;margin:0 0 28px}}}}@media(max-width:560px){{.standalone-page{{padding-inline:19px}}}}</style>
</head><body><div class="app-shell"><aside class="sidebar"><a class="brand" href="../index.html"><span class="brand-mark"><svg viewBox="0 0 32 32" fill="none" aria-hidden="true"><path d="M7 8.5 16 4l9 4.5v15L16 28l-9-4.5v-15Z"/><path d="m7 8.5 9 4.7 9-4.7M16 13.2V28M11 6.5l9 4.7"/><circle cx="7" cy="8.5" r="2"/><circle cx="25" cy="8.5" r="2"/><circle cx="16" cy="28" r="2"/></svg></span><span><b>FIELDNOTES</b><small>Distributed systems</small></span></a><div class="side-label">THE COLLECTION <span>19 CHAPTERS</span></div><nav id="chapter-list" aria-label="Chapters">{sidebar(chapter['id'])}</nav><div class="side-bottom"><a class="chapter-home" href="../index.html">← BACK TO THE FIELD GUIDE</a></div></aside>
<main class="main-area"><header class="topbar"><button class="icon-btn side-toggle" data-side="left" title="Hide chapter sidebar" aria-label="Hide chapter sidebar">☰ Chapters</button><div class="breadcrumbs"><a href="../index.html">Library</a><span>/</span><span>{html.escape(plain_title)}</span></div><div class="top-actions"><button class="search-trigger" data-open-flow><span>Flow</span><b>↝</b></button><button class="search-trigger" data-open-map><span>Mind map</span><b>✳</b></button><button class="search-trigger" data-open-file><span>Open file</span><b>↥</b></button><button class="search-trigger" data-export-markdown title="Download this chapter as Markdown"><span>Export .md</span><b>↓</b></button><input id="local-file-input" type="file" hidden><button class="search-trigger" data-open-notes><span>My notes</span><b>＋</b></button><button class="search-trigger chapter-read-toggle" data-read-toggle="{chapter['id']}" aria-pressed="false" aria-label="Mark chapter unread">✓ Read</button><button class="search-trigger" data-theme-toggle>◐ Dark</button><button class="icon-btn side-toggle" data-side="right" title="Hide contents sidebar" aria-label="Hide contents sidebar">☷</button></div></header><div class="standalone-page"><div class="reader-layout"><article class="article"><header class="article-header"><div class="article-kicker">FIELD GUIDE &nbsp; / &nbsp; CHAPTER {index+1:02}</div><h1>{html.escape(plain_title)}</h1><div class="article-deck">{html.escape(chapter['summary'])}</div><div class="article-meta"><span>{words:,} WORDS</span><span>VISUAL FIELD NOTES</span><span>READ AT YOUR PACE</span></div></header><div class="prose">{body}</div></article><aside class="toc"><div class="toc-title">ON THIS PAGE</div>{toc}<div class="toc-bottom"><span class="toc-glyph">↳</span><span>Knowledge is replicated<br>by reading it twice.</span></div></aside></div><nav class="chapter-nav" aria-label="Chapter navigation">{prev_link}{next_link}</nav><footer class="site-footer"><span>FIELDNOTES / DISTRIBUTED SYSTEMS</span><a class="chapter-home" href="../index.html">RETURN TO THE CHAPTER INDEX ↑</a><a class="project-link" href="https://github.com/NikhilG6704/Distributed-Systems-Complete-Guide" target="_blank" rel="noopener noreferrer">VIEW ON GITHUB ↗</a><span>PLEASE CREDIT THIS PROJECT WHEN SHARING OR ADAPTING THESE NOTES.</span></footer></div></main></div><button class="reopen-side reopen-left" id="reopen-left" aria-label="Show chapter sidebar">☰ Chapters</button><button class="reopen-side reopen-right" id="reopen-right" aria-label="Show contents sidebar">☷ Contents</button><script src="../chapter-tools.js"></script><script>try{{document.body.classList.toggle('light',localStorage.getItem('ds-theme')==='light');document.querySelector('[data-theme-toggle]').textContent=document.body.classList.contains('light')?'◑ Light':'◐ Dark';const seen=new Set(JSON.parse(localStorage.getItem('ds-seen')||'[]'));localStorage.setItem('ds-last','{chapter['id']}');const isRead=seen.has('{chapter['id']}');const toggle=document.querySelector('[data-read-toggle]');toggle.setAttribute('aria-pressed',String(isRead));toggle.classList.toggle('is-read',isRead);toggle.textContent=isRead?'✓ Read':'○ Unread'}}catch(e){{}}</script></body></html>'''


for i, chapter in enumerate(CHAPTERS):
    (OUT / f"{chapter['id']}.html").write_text(render(i, chapter), encoding="utf-8")

# Embed the compact chapter index in the root file. This keeps the landing page
# usable with file://, where browsers intentionally block fetch() requests.
index = (ROOT / "index.html").read_text(encoding="utf-8")
embedded = json.dumps(CHAPTERS, ensure_ascii=False).replace("<", "\\u003c")
index, count = re.subn(
    r'(<script id="chapter-data" type="application/json">).*?(</script>)',
    lambda match: match.group(1) + embedded + match.group(2),
    index,
    count=1,
    flags=re.S,
)
if count == 0:
    index = index.replace("__CHAPTER_DATA__", embedded)
(ROOT / "index.html").write_text(index, encoding="utf-8")
(ROOT / "chapters.json").write_text(json.dumps(CHAPTERS, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"Rendered {len(CHAPTERS)} chapter pages into {OUT}")
