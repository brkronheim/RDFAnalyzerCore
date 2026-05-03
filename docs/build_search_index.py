#!/usr/bin/env python3
import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
IGNORE_FILES = {"README.md", "SEARCH.md", "search.js", "search.json", "build_search_index.py", "_config.yml"}
IGNORE_DIRS = {"_layouts", "_includes", "_data", "doxygen", "vendor"}

MARKDOWN_EXTENSIONS = {".md", ".markdown"}


def slugify_path(path: Path) -> str:
    rel = path.relative_to(ROOT)
    parts = [p for p in rel.parts]
    if parts[-1].lower() == "index.md":
        parts[-1] = "index.html"
    else:
        parts[-1] = parts[-1].rsplit(".", 1)[0] + ".html"
    return "/".join(parts)


def extract_title(text: str, default: str) -> str:
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#"):
            return line.lstrip("# ").strip()
    return default


def plain_text(text: str) -> str:
    text = re.sub(r"^---.*?---\s*", "", text, flags=re.S)
    text = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text)
    text = re.sub(r"!\[[^\]]*\]\([^\)]+\)", "", text)
    text = re.sub(r"[`*_>{}\-\[\]]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_index() -> list[dict[str, str]]:
    index = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS and not d.startswith(".")]
        for filename in filenames:
            if filename in IGNORE_FILES:
                continue
            path = Path(dirpath) / filename
            if path.suffix.lower() not in MARKDOWN_EXTENSIONS:
                continue
            text = path.read_text(encoding="utf-8")
            title = extract_title(text, path.stem.replace("_", " ").title())
            url = slugify_path(path)
            index.append({
                "title": title,
                "url": url,
                "text": plain_text(text),
            })
    return sorted(index, key=lambda item: item["title"].lower())


def main() -> None:
    output = ROOT / "search.json"
    output.write_text(json.dumps(build_index(), indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote search index to {output}")


if __name__ == "__main__":
    main()
