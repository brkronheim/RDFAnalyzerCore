---
layout: default
title: Search
---

# Search Documentation

Use the search box below to find matching pages in the RDFAnalyzerCore documentation.

<div class="search-box">
  <input id="searchQuery" type="search" placeholder="Search docs..." autocomplete="off" aria-label="Search documentation">
  <p id="searchStatus" class="search-status">Type keywords to search all documentation pages.</p>
</div>

<div id="searchResults" class="search-results"></div>

<script src="search.js"></script>

<style>
.search-box {
  margin: 1rem 0 1.5rem;
}
#searchQuery {
  width: 100%;
  max-width: 40rem;
  padding: 0.75rem 1rem;
  font-size: 1rem;
  border: 1px solid #ccc;
  border-radius: 0.35rem;
}
.search-status {
  margin: 0.5rem 0 0;
  color: #555;
}
.search-result {
  border-top: 1px solid #e1e4e8;
  padding: 1rem 0;
}
.search-result:first-of-type {
  border-top: none;
}
.search-result-title {
  font-weight: 600;
  margin: 0 0 0.3rem;
}
.search-result-title a {
  color: #0366d6;
  text-decoration: none;
}
.search-result-title a:hover {
  text-decoration: underline;
}
.search-result-url {
  color: #586069;
  font-size: 0.9rem;
  margin-bottom: 0.4rem;
}
.search-result-snippet {
  margin: 0;
  color: #24292f;
}
</style>
