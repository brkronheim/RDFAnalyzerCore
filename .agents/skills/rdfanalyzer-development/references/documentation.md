# Repository documentation, search, and publishing

Use for maintaining the repository's docs and generated API/search outputs. Start from executable contracts and their public headers; update the relevant guide plus focused skill/reference when behavior changes. A documentation-only request does not require a native rebuild or deployment.

[docs/index.md](../../../../docs/index.md), [docs/INDEX.md](../../../../docs/INDEX.md), [docs/_config.yml](../../../../docs/_config.yml), and [docs/SEARCH.md](../../../../docs/SEARCH.md) provide different entry/navigation surfaces. Check callers and links before treating one as the sole index. Keep source-relative Markdown links and published `.html` navigation paths appropriate to their consumers.

## Generated API and search coverage

[Doxyfile](../../../../Doxyfile) currently selects `core/interface core/src`, disables recursive traversal, writes HTML directly under `docs/doxygen`, and does not treat warnings as errors. Plugin directories and nested API headers therefore are not automatically fully covered. Check actual INPUT/RECURSIVE/exclusion settings before claiming an API page exists. Use [DOXYGEN_GUIDE](../../../../docs/DOXYGEN_GUIDE.md) for authoring patterns, verified against configuration.

[build_search_index.py](../../../../docs/build_search_index.py) scans Markdown under docs, omits listed files/directories including generated Doxygen, strips frontmatter/markup, converts paths to `.html` URLs, and writes `docs/search.json`. [search.js](../../../../docs/search.js) consumes that artifact. For a new guide, verify title extraction, inclusion and the published URL. Skill files under `.agents/skills` are outside that search root; their discoverability comes from skill metadata and the main skill routes.

A bounded read-only check can import the index builder and inspect `build_index()` without running its write-producing `main()`. Run the generator when the requested docs change needs an updated artifact; avoid unrelated generated output churn.

## Publishing workflow

[jekyll-gh-pages.yml](../../../../.github/workflows/jekyll-gh-pages.yml) checks out the repo, installs/runs Doxygen, builds the search index, builds Jekyll from `docs` into `_site`, uploads the Pages artifact, then deploys. Read current triggers, permissions and concurrency. Local Markdown/link validation is different from a successful Doxygen/Jekyll build or a published site. Do not dispatch deployment solely to verify a prose change.

Verify relative links and the relevant generation step. For Doxygen use an isolated output configuration if generated artifacts should remain separate from the worktree. For search inspect entries and URL mappings. For publication report the actual build/deploy evidence; do not infer it from a committed YAML definition.
