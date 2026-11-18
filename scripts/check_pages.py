"""Check the rendered documentation, including links emitted by the search index."""
import argparse
import json
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.anchors = set()

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        for key in ("id", "name"):
            if attrs.get(key):
                self.anchors.add(attrs[key])
        key = {"a": "href", "img": "src", "script": "src", "link": "href"}.get(tag)
        if key and attrs.get(key):
            self.links.append(attrs[key])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path)
    parser.add_argument("--base-path", default="/RDFAnalyzerCore")
    args = parser.parse_args()
    root = args.site.resolve()
    pages = {}
    for path in root.rglob("*.html"):
        if path.relative_to(root).parts[0] == "doxygen":
            continue
        page = Page()
        page.feed(path.read_text())
        pages[path] = page

    errors = []
    urls = [entry["url"] for entry in json.loads((root / "search.json").read_text())]
    if len(urls) != len(set(urls)):
        errors.append("Duplicate URL in search.json")
    pages[root / "SEARCH.html"].links.extend(urls)
    for path, page in pages.items():
        for link in page.links:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                continue
            name = unquote(url.path)
            if name.startswith("/"):
                base = args.base_path.rstrip("/")
                if name != base and not name.startswith(base + "/"):
                    errors.append(f"{path.relative_to(root)}: outside project site: {link}")
                    continue
                target = root / name[len(base):].lstrip("/")
            else:
                target = path.parent / name if name else path
            target = target.resolve()
            if target.is_dir():
                target = target / "index.html"
            if not target.is_relative_to(root) or not target.is_file():
                errors.append(f"{path.relative_to(root)}: missing target: {link}")
            elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].anchors:
                errors.append(f"{path.relative_to(root)}: missing anchor: {link}")
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"Checked {len(pages)} rendered pages and {len(urls)} search entries.")


if __name__ == "__main__":
    main()
