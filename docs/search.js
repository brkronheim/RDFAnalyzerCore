const indexUrl = "search.json";
const statusEl = document.getElementById("searchStatus");
const resultsEl = document.getElementById("searchResults");
const queryInput = document.getElementById("searchQuery");
let indexData = [];

function normalize(text) {
  return text ? text.toLowerCase() : "";
}

function snippetFor(item, terms) {
  const text = normalize(item.text);
  for (const term of terms) {
    const pos = text.indexOf(term);
    if (pos >= 0) {
      const start = Math.max(0, pos - 40);
      const snippet = item.text.substring(start, Math.min(item.text.length, pos + term.length + 80));
      return (start > 0 ? "..." : "") + snippet.trim() + (pos + term.length + 80 < item.text.length ? "..." : "");
    }
  }
  return item.text.slice(0, 180).trim() + (item.text.length > 180 ? "..." : "");
}

function scoreItem(item, terms) {
  const title = normalize(item.title);
  const text = normalize(item.text);
  let score = 0;
  for (const term of terms) {
    if (title.includes(term)) score += 10;
    if (text.includes(term)) score += 1;
  }
  return score;
}

function renderResults(items, query) {
  resultsEl.innerHTML = "";
  if (!query) {
    statusEl.textContent = "Type keywords to search all documentation pages.";
    return;
  }
  if (items.length === 0) {
    statusEl.textContent = `No results found for “${query}”.`;
    return;
  }

  statusEl.textContent = `Found ${items.length} matching page${items.length === 1 ? "" : "s"}.`;
  const fragment = document.createDocumentFragment();

  for (const item of items.slice(0, 25)) {
    const resultDiv = document.createElement("div");
    resultDiv.className = "search-result";

    const title = document.createElement("p");
    title.className = "search-result-title";
    title.innerHTML = `<a href="${item.url}">${item.title}</a>`;
    resultDiv.appendChild(title);

    const url = document.createElement("div");
    url.className = "search-result-url";
    url.textContent = item.url;
    resultDiv.appendChild(url);

    const snippet = document.createElement("p");
    snippet.className = "search-result-snippet";
    snippet.textContent = snippetFor(item, query.split(/\s+/).filter(Boolean));
    resultDiv.appendChild(snippet);

    fragment.appendChild(resultDiv);
  }

  resultsEl.appendChild(fragment);
}

function runSearch(query) {
  const normalizedQuery = normalize(query);
  if (!normalizedQuery) {
    renderResults([], query);
    return;
  }

  const terms = normalizedQuery.split(/\s+/).filter(Boolean);
  const matches = indexData
    .map(item => ({
      item,
      score: scoreItem(item, terms)
    }))
    .filter(entry => entry.score > 0)
    .sort((a, b) => b.score - a.score)
    .map(entry => entry.item);

  renderResults(matches, query);
}

fetch(indexUrl)
  .then(response => {
    if (!response.ok) throw new Error("Failed to load search index");
    return response.json();
  })
  .then(data => {
    indexData = data;
    statusEl.textContent = "Search index loaded. Enter keywords above to search.";
    queryInput.disabled = false;
  })
  .catch(() => {
    statusEl.textContent = "Unable to load the search index. Please try again later.";
    queryInput.disabled = true;
  });

queryInput.addEventListener("input", event => runSearch(event.target.value));
