use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

use crate::network_policy::{NetworkDomainPolicy, NetworkPolicyReceipt};

const DEFAULT_RESULTS: usize = 8;
const MAX_RESULTS: usize = 20;
const RRF_K: f64 = 60.0;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SearchIntent {
    Auto,
    General,
    Code,
    Research,
    Knowledge,
}

impl Default for SearchIntent {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SearchDepth {
    Quick,
    Standard,
    Deep,
}

impl Default for SearchDepth {
    fn default() -> Self {
        Self::Standard
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SearchRecency {
    Any,
    Day,
    Week,
    Month,
    Year,
}

impl Default for SearchRecency {
    fn default() -> Self {
        Self::Any
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WebSearchInput {
    pub(crate) query: String,
    pub(crate) allowed_domains: Option<Vec<String>>,
    pub(crate) blocked_domains: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) intent: SearchIntent,
    #[serde(default)]
    pub(crate) depth: SearchDepth,
    pub(crate) max_results: Option<usize>,
    pub(crate) locale: Option<String>,
    #[serde(default)]
    pub(crate) recency: SearchRecency,
}

#[derive(Debug, Serialize)]
pub(crate) struct WebSearchOutput {
    query: String,
    intent: SearchIntent,
    depth: SearchDepth,
    results: Vec<WebSearchResultItem>,
    sources: Vec<SearchReceipt>,
    #[serde(rename = "networkPolicy")]
    network_policy: NetworkPolicyReceipt,
    #[serde(rename = "durationSeconds")]
    duration_seconds: f64,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum WebSearchResultItem {
    SearchResult {
        tool_use_id: String,
        content: Vec<SearchHit>,
    },
    Commentary(String),
}

#[derive(Debug, Clone, Serialize)]
struct SearchHit {
    title: String,
    url: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    snippet: String,
    sources: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    freshness: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SearchReceipt {
    source: String,
    status: &'static str,
    result_count: usize,
    duration_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct SearchCandidate {
    title: String,
    url: String,
    snippet: String,
    source: String,
    rank: usize,
}

#[derive(Debug, Clone, Copy)]
enum SourceKind {
    Html,
    GithubRepositories,
    Crossref,
    MediaWiki,
}

#[derive(Debug, Clone)]
struct SourceRequest {
    name: &'static str,
    url: reqwest::Url,
    kind: SourceKind,
}

struct SourceResponse {
    receipt: SearchReceipt,
    candidates: Vec<SearchCandidate>,
}

pub(crate) fn execute_web_search(input: &WebSearchInput) -> Result<WebSearchOutput, String> {
    let query = input.query.trim();
    if query.len() < 2 {
        return Err(String::from(
            "web search query must contain at least two characters",
        ));
    }

    let started = Instant::now();
    let policy = NetworkDomainPolicy::from_env();
    let policy_receipt = policy.merge_call_filters(
        input.allowed_domains.as_deref(),
        input.blocked_domains.as_deref(),
    );
    if policy_receipt.denied || policy_receipt.requires_approval {
        return Ok(WebSearchOutput {
            query: query.to_string(),
            intent: resolve_intent(input.intent, query),
            depth: input.depth,
            results: vec![WebSearchResultItem::Commentary(
                if policy_receipt.denied {
                    "Network domain policy denied the search request."
                } else {
                    "Network domain policy requires approval before searching external sources."
                }
                .to_string(),
            )],
            sources: Vec::new(),
            network_policy: policy_receipt,
            duration_seconds: started.elapsed().as_secs_f64(),
        });
    }
    let intent = resolve_intent(input.intent, query);
    let requests = build_source_requests(input, intent)?;
    let client = build_search_client()?;
    let mut responses = std::thread::scope(|scope| {
        requests
            .into_iter()
            .map(|request| {
                let client = client.clone();
                scope.spawn(move || execute_source(&client, request))
            })
            .map(|handle| {
                handle.join().unwrap_or_else(|_| SourceResponse {
                    receipt: SearchReceipt {
                        source: String::from("unknown"),
                        status: "failed",
                        result_count: 0,
                        duration_ms: 0,
                        error: Some(String::from("source worker panicked")),
                    },
                    candidates: Vec::new(),
                })
            })
            .collect::<Vec<_>>()
    });
    responses.sort_by(|left, right| left.receipt.source.cmp(&right.receipt.source));

    let receipts = responses
        .iter()
        .map(|response| response.receipt.clone())
        .collect::<Vec<_>>();
    let candidates = responses
        .drain(..)
        .flat_map(|response| response.candidates)
        .filter(|candidate| candidate_allowed(input, candidate, &policy))
        .filter(|candidate| !is_search_backend_navigation(&candidate.url))
        .collect::<Vec<_>>();
    let limit = input
        .max_results
        .unwrap_or(DEFAULT_RESULTS)
        .clamp(1, MAX_RESULTS);
    let hits = fuse_candidates(candidates, limit, input.recency);

    if hits.is_empty() {
        let failures = receipts
            .iter()
            .map(|receipt| {
                format!(
                    "{}: {}{}",
                    receipt.source,
                    receipt.status,
                    receipt
                        .error
                        .as_deref()
                        .map(|error| format!(" ({error})"))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "web search returned no usable external results for {query:?}; sources: {failures}"
        ));
    }

    let successful = receipts
        .iter()
        .filter(|receipt| receipt.status == "ok")
        .map(|receipt| receipt.source.as_str())
        .collect::<Vec<_>>();
    let degraded = receipts
        .iter()
        .filter(|receipt| receipt.status != "ok")
        .count();
    let rendered_hits = hits
        .iter()
        .map(|hit| {
            let detail = if hit.snippet.is_empty() {
                String::new()
            } else {
                format!(" — {}", preview_text(&hit.snippet, 220))
            };
            format!(
                "- [{}]({}){} [{}]",
                hit.title,
                hit.url,
                detail,
                hit.sources.join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let summary = format!(
        "Federated search results for {query:?}. Sources: {}. Degraded sources: {degraded}. Include a Sources section in the final answer.\n{rendered_hits}",
        successful.join(", ")
    );

    Ok(WebSearchOutput {
        query: query.to_string(),
        intent,
        depth: input.depth,
        results: vec![
            WebSearchResultItem::Commentary(summary),
            WebSearchResultItem::SearchResult {
                tool_use_id: String::from("web_search_1"),
                content: hits,
            },
        ],
        sources: receipts,
        network_policy: policy_receipt,
        duration_seconds: started.elapsed().as_secs_f64(),
    })
}

fn resolve_intent(configured: SearchIntent, query: &str) -> SearchIntent {
    if configured != SearchIntent::Auto {
        return configured;
    }
    let lower = query.to_ascii_lowercase();
    if [
        "github",
        "repository",
        "repo",
        "crate",
        "npm",
        "sdk",
        "source code",
        "代码",
        "源码",
        "开源",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        SearchIntent::Code
    } else if [
        "paper", "research", "doi", "arxiv", "study", "论文", "研究", "文献",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        SearchIntent::Research
    } else if [
        "definition",
        "encyclopedia",
        "what is",
        "who is",
        "是什么",
        "百科",
        "定义",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        SearchIntent::Knowledge
    } else {
        SearchIntent::General
    }
}

fn build_source_requests(
    input: &WebSearchInput,
    intent: SearchIntent,
) -> Result<Vec<SourceRequest>, String> {
    if let Ok(base) = std::env::var("COWD_WEB_SEARCH_BASE_URL") {
        let mut url = reqwest::Url::parse(&base).map_err(|error| error.to_string())?;
        url.query_pairs_mut().append_pair("q", input.query.trim());
        return Ok(vec![SourceRequest {
            name: "custom",
            url,
            kind: SourceKind::Html,
        }]);
    }

    let mut requests = Vec::new();
    let general_count = match input.depth {
        SearchDepth::Quick => 1,
        SearchDepth::Standard => 2,
        SearchDepth::Deep => 3,
    };
    for (name, base, query_key, extra) in [
        ("duckduckgo", "https://html.duckduckgo.com/html/", "q", None),
        (
            "brave",
            "https://search.brave.com/search",
            "q",
            Some(("source", "web")),
        ),
        ("yahoo", "https://search.yahoo.com/search", "p", None),
    ]
    .into_iter()
    .take(general_count)
    {
        let mut url = reqwest::Url::parse(base).map_err(|error| error.to_string())?;
        url.query_pairs_mut()
            .append_pair(query_key, input.query.trim());
        if let Some((key, value)) = extra {
            url.query_pairs_mut().append_pair(key, value);
        }
        requests.push(SourceRequest {
            name,
            url,
            kind: SourceKind::Html,
        });
    }

    match intent {
        SearchIntent::Code => {
            requests.push(github_request(input)?);
            if input.depth == SearchDepth::Deep {
                requests.push(mediawiki_request(input, "en")?);
            }
        }
        SearchIntent::Research => {
            requests.push(crossref_request(input)?);
            if input.depth != SearchDepth::Quick {
                requests.push(mediawiki_request(input, preferred_wiki_language(input))?);
            }
        }
        SearchIntent::Knowledge => {
            requests.push(mediawiki_request(input, preferred_wiki_language(input))?);
            if input.depth == SearchDepth::Deep && preferred_wiki_language(input) != "en" {
                requests.push(mediawiki_request(input, "en")?);
            }
        }
        SearchIntent::Auto | SearchIntent::General => {}
    }
    Ok(requests)
}

fn github_request(input: &WebSearchInput) -> Result<SourceRequest, String> {
    let mut url = reqwest::Url::parse("https://api.github.com/search/repositories")
        .map_err(|error| error.to_string())?;
    url.query_pairs_mut()
        .append_pair("q", input.query.trim())
        .append_pair("per_page", "10");
    Ok(SourceRequest {
        name: "github",
        url,
        kind: SourceKind::GithubRepositories,
    })
}

fn crossref_request(input: &WebSearchInput) -> Result<SourceRequest, String> {
    let mut url =
        reqwest::Url::parse("https://api.crossref.org/works").map_err(|error| error.to_string())?;
    url.query_pairs_mut()
        .append_pair("query", input.query.trim())
        .append_pair("rows", "10")
        .append_pair("select", "DOI,title,abstract,URL,published");
    Ok(SourceRequest {
        name: "crossref",
        url,
        kind: SourceKind::Crossref,
    })
}

fn mediawiki_request(
    input: &WebSearchInput,
    language: &'static str,
) -> Result<SourceRequest, String> {
    let mut url = reqwest::Url::parse(&format!("https://{language}.wikipedia.org/w/api.php"))
        .map_err(|error| error.to_string())?;
    url.query_pairs_mut()
        .append_pair("action", "query")
        .append_pair("list", "search")
        .append_pair("format", "json")
        .append_pair("utf8", "1")
        .append_pair("srlimit", "10")
        .append_pair("srsearch", input.query.trim());
    Ok(SourceRequest {
        name: if language == "zh" {
            "wikipedia-zh"
        } else {
            "wikipedia-en"
        },
        url,
        kind: SourceKind::MediaWiki,
    })
}

fn preferred_wiki_language(input: &WebSearchInput) -> &'static str {
    if input
        .locale
        .as_deref()
        .is_some_and(|locale| locale.to_ascii_lowercase().starts_with("zh"))
        || input.query.chars().any(|character| {
            ('\u{4e00}'..='\u{9fff}').contains(&character)
                || ('\u{3400}'..='\u{4dbf}').contains(&character)
        })
    {
        "zh"
    } else {
        "en"
    }
}

fn build_search_client() -> Result<Client, String> {
    Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(8))
        .user_agent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/131.0 Safari/537.36 Cowd-Search/0.9",
        )
        .build()
        .map_err(|error| error.to_string())
}

fn execute_source(client: &Client, request: SourceRequest) -> SourceResponse {
    let started = Instant::now();
    let response = client.get(request.url).send();
    let (status, candidates, error) = match response {
        Ok(response) if response.status().is_success() => match response.text() {
            Ok(body) => {
                let candidates = parse_source(request.kind, request.name, &body);
                if candidates.is_empty() {
                    ("empty", candidates, Some(String::from("no usable results")))
                } else {
                    ("ok", candidates, None)
                }
            }
            Err(error) => (
                "failed",
                Vec::new(),
                Some(compact_error(&error.to_string())),
            ),
        },
        Ok(response) => (
            "failed",
            Vec::new(),
            Some(format!("HTTP {}", response.status())),
        ),
        Err(error) => (
            "failed",
            Vec::new(),
            Some(compact_error(&error.to_string())),
        ),
    };
    SourceResponse {
        receipt: SearchReceipt {
            source: request.name.to_string(),
            status,
            result_count: candidates.len(),
            duration_ms: started.elapsed().as_millis(),
            error,
        },
        candidates,
    }
}

fn parse_source(kind: SourceKind, source: &str, body: &str) -> Vec<SearchCandidate> {
    let hits = match kind {
        SourceKind::Html => extract_html_hits(body)
            .into_iter()
            .map(|(title, url, snippet)| (title, url, snippet))
            .collect(),
        SourceKind::GithubRepositories => parse_github(body),
        SourceKind::Crossref => parse_crossref(body),
        SourceKind::MediaWiki => parse_mediawiki(body),
    };
    hits.into_iter()
        .enumerate()
        .filter(|(_, (title, url, _))| !title.trim().is_empty() && is_http_url(url))
        .map(|(rank, (title, url, snippet))| SearchCandidate {
            title: collapse_whitespace(&decode_html_entities(&title)),
            url: decode_search_redirect(&url).unwrap_or(url),
            snippet: collapse_whitespace(&html_to_text(&snippet)),
            source: source.to_string(),
            rank: rank + 1,
        })
        .collect()
}

fn parse_github(body: &str) -> Vec<(String, String, String)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return Vec::new();
    };
    value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            Some((
                item.get("full_name")?.as_str()?.to_string(),
                item.get("html_url")?.as_str()?.to_string(),
                item.get("description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            ))
        })
        .collect()
}

fn parse_crossref(body: &str) -> Vec<(String, String, String)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return Vec::new();
    };
    value
        .pointer("/message/items")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let title = item
                .get("title")
                .and_then(serde_json::Value::as_array)
                .and_then(|titles| titles.first())
                .and_then(serde_json::Value::as_str)?
                .to_string();
            let url = item
                .get("URL")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
                .or_else(|| {
                    item.get("DOI")
                        .and_then(serde_json::Value::as_str)
                        .map(|doi| format!("https://doi.org/{doi}"))
                })?;
            let snippet = item
                .get("abstract")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            Some((title, url, snippet))
        })
        .collect()
}

fn parse_mediawiki(body: &str) -> Vec<(String, String, String)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return Vec::new();
    };
    value
        .pointer("/query/search")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let title = item.get("title")?.as_str()?.to_string();
            let page_id = item.get("pageid")?.as_i64()?;
            let snippet = item
                .get("snippet")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            // The API response does not repeat the host. The canonical page-id
            // route is language-neutral at parsing time and redirects safely.
            Some((
                title,
                format!("https://wikipedia.org/?curid={page_id}"),
                snippet,
            ))
        })
        .collect()
}

fn extract_html_hits(html: &str) -> Vec<(String, String, String)> {
    let mut hits = Vec::new();
    let mut remaining = html;
    while let Some(anchor_start) = remaining.find("<a") {
        let after_anchor = &remaining[anchor_start..];
        let Some(href_idx) = after_anchor.find("href=") else {
            remaining = &after_anchor[2..];
            continue;
        };
        let href_slice = &after_anchor[href_idx + 5..];
        let Some((url, rest)) = extract_quoted_value(href_slice) else {
            remaining = &after_anchor[2..];
            continue;
        };
        let Some(close_tag_idx) = rest.find('>') else {
            remaining = &after_anchor[2..];
            continue;
        };
        let after_tag = &rest[close_tag_idx + 1..];
        let Some(end_anchor_idx) = after_tag.find("</a>") else {
            remaining = &after_anchor[2..];
            continue;
        };
        let title = html_to_text(&after_tag[..end_anchor_idx]);
        if !title.is_empty() {
            if let Some(url) = decode_search_redirect(&url) {
                hits.push((title, url, String::new()));
            }
        }
        remaining = &after_tag[end_anchor_idx + 4..];
    }
    hits
}

fn candidate_allowed(
    input: &WebSearchInput,
    candidate: &SearchCandidate,
    policy: &NetworkDomainPolicy,
) -> bool {
    let policy_allows = policy.allow.is_empty() || host_matches_list(&candidate.url, &policy.allow);
    let call_allows = input
        .allowed_domains
        .as_ref()
        .is_none_or(|domains| host_matches_list(&candidate.url, domains));
    if !policy_allows || !call_allows {
        return false;
    }
    let policy_blocks = policy
        .block
        .iter()
        .any(|domain| host_matches_list(&candidate.url, &[domain.clone()]));
    let call_blocks = input
        .blocked_domains
        .as_ref()
        .is_some_and(|domains| host_matches_list(&candidate.url, domains));
    !policy_blocks && !call_blocks
}

fn fuse_candidates(
    candidates: Vec<SearchCandidate>,
    limit: usize,
    recency: SearchRecency,
) -> Vec<SearchHit> {
    struct Fused {
        hit: SearchHit,
        score: f64,
        publisher: String,
    }
    let mut fused = BTreeMap::<String, Fused>::new();
    for candidate in candidates {
        let Some(canonical) = canonical_url(&candidate.url) else {
            continue;
        };
        let score = 1.0 / (RRF_K + candidate.rank as f64);
        let freshness = estimated_freshness(&candidate.url, &candidate.snippet);
        if recency != SearchRecency::Any
            && freshness
                .as_deref()
                .and_then(|date| freshness_within_window(date, recency))
                == Some(false)
        {
            continue;
        }
        let entry = fused.entry(canonical.clone()).or_insert_with(|| Fused {
            hit: SearchHit {
                title: candidate.title.clone(),
                url: canonical,
                snippet: candidate.snippet.clone(),
                sources: Vec::new(),
                freshness: freshness.clone(),
            },
            score: 0.0,
            publisher: publisher_key(&candidate.url),
        });
        entry.score += score;
        if entry.publisher.is_empty() {
            entry.publisher = publisher_key(&candidate.url);
        }
        if candidate.title.len() > entry.hit.title.len() {
            entry.hit.title = candidate.title;
        }
        if candidate.snippet.len() > entry.hit.snippet.len() {
            entry.hit.snippet = candidate.snippet;
        }
        if entry.hit.freshness.is_none() {
            entry.hit.freshness = freshness;
        }
        if !entry.hit.sources.contains(&candidate.source) {
            entry.hit.sources.push(candidate.source);
            entry.hit.sources.sort();
        }
    }
    let mut values = fused.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.hit.url.cmp(&right.hit.url))
    });
    // Publisher deduplication: one result per publisher keeps the answer
    // diverse without hiding the highest-ranked source.
    let mut seen_publishers = std::collections::BTreeSet::new();
    values.retain(|value| {
        if value.publisher.is_empty() {
            return true;
        }
        seen_publishers.insert(value.publisher.clone())
    });
    values
        .into_iter()
        .map(|value| value.hit)
        .take(limit)
        .collect()
}

fn publisher_key(url: &str) -> String {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return String::new();
    };
    let host = parsed
        .host_str()
        .unwrap_or_default()
        .trim_start_matches("www.");
    let labels = host.split('.').collect::<Vec<_>>();
    let last_two = labels
        .get(labels.len().saturating_sub(2)..)
        .unwrap_or_default()
        .join(".");
    // Public-suffix aware: `example.co.uk` must not collapse into the
    // pseudo-publisher `co.uk` together with `other.co.uk`.
    if is_public_suffix(&last_two) && labels.len() >= 3 {
        labels[labels.len() - 3..].join(".")
    } else if !last_two.is_empty() {
        last_two
    } else {
        host.to_string()
    }
}

fn is_public_suffix(candidate: &str) -> bool {
    matches!(
        candidate,
        "co.uk"
            | "org.uk"
            | "ac.uk"
            | "gov.uk"
            | "co.jp"
            | "ne.jp"
            | "or.jp"
            | "co.kr"
            | "or.kr"
            | "com.cn"
            | "org.cn"
            | "net.cn"
            | "gov.cn"
            | "com.au"
            | "net.au"
            | "org.au"
            | "co.nz"
            | "com.br"
            | "com.mx"
            | "co.in"
            | "com.tw"
            | "co.id"
            | "co.za"
            | "com.hk"
    )
}

fn estimated_freshness(url: &str, snippet: &str) -> Option<String> {
    // Date in URL path, e.g. /2026/08/12/slug or news?id=20260812.
    let url_patterns = [
        r"(?P<y>20\d{2})/(?P<m>0[1-9]|1[0-2])/(?P<d>0[1-9]|[12]\d|3[01])",
        r"date[=/](?P<y>20\d{2})[-/]?(?P<m>0[1-9]|1[0-2])[-/]?(?P<d>0[1-9]|[12]\d|3[01])",
    ];
    for pattern in url_patterns {
        if let Some(captures) = regex::Regex::new(pattern)
            .ok()
            .and_then(|regex| regex.captures(url))
        {
            return Some(format!(
                "{}-{}-{}",
                &captures["y"], &captures["m"], &captures["d"]
            ));
        }
    }
    // ISO date inside a snippet, e.g. "Published Aug 12, 2026" is harder to
    // normalize; only the machine-readable form is used.
    let snippet_pattern = r"(?P<y>20\d{2})-(?P<m>0[1-9]|1[0-2])-(?P<d>0[1-9]|[12]\d|3[01])";
    if let Some(captures) = regex::Regex::new(snippet_pattern)
        .ok()
        .and_then(|regex| regex.captures(snippet))
    {
        return Some(format!(
            "{}-{}-{}",
            &captures["y"], &captures["m"], &captures["d"]
        ));
    }
    None
}

fn freshness_within_window(date: &str, window: SearchRecency) -> Option<bool> {
    let days = (chrono_now_days() as i64).saturating_sub(parse_days(date)?);
    Some(match window {
        SearchRecency::Any => true,
        SearchRecency::Day => days <= 1,
        SearchRecency::Week => days <= 7,
        SearchRecency::Month => days <= 31,
        SearchRecency::Year => days <= 366,
    })
}

fn parse_days(date: &str) -> Option<i64> {
    let parts = date.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let year = parts[0].parse::<i64>().ok()?;
    let month = parts[1].parse::<i64>().ok()?;
    let day = parts[2].parse::<i64>().ok()?;
    Some(civil_to_days(year, month, day))
}

fn chrono_now_days() -> i64 {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (seconds / 86_400) as i64
}

fn civil_to_days(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn canonical_url(value: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(value).ok()?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return None;
    }
    url.set_fragment(None);
    let retained = url
        .query_pairs()
        .filter(|(key, _)| {
            let key = key.to_ascii_lowercase();
            !key.starts_with("utm_")
                && !matches!(
                    key.as_str(),
                    "fbclid" | "gclid" | "ref" | "source" | "tracking"
                )
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    if !retained.is_empty() {
        url.query_pairs_mut().extend_pairs(retained);
    }
    if url.path().len() > 1 && url.path().ends_with('/') {
        let path = url.path().trim_end_matches('/').to_string();
        url.set_path(&path);
    }
    Some(url.to_string())
}

fn extract_quoted_value(input: &str) -> Option<(String, &str)> {
    let quote = input.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let rest = &input[quote.len_utf8()..];
    let end = rest.find(quote)?;
    Some((rest[..end].to_string(), &rest[end + quote.len_utf8()..]))
}

fn decode_search_redirect(url: &str) -> Option<String> {
    if let Ok(parsed) = reqwest::Url::parse(url) {
        let host = parsed
            .host_str()
            .unwrap_or_default()
            .trim_start_matches("www.");
        if host == "r.search.yahoo.com" {
            let encoded = parsed.path().split("/RU=").nth(1)?.split("/RK=").next()?;
            let decoded = urlencoding::decode(encoded).ok()?.into_owned();
            return is_http_url(&decoded).then_some(decoded);
        }
        if is_http_url(url) {
            return Some(decode_html_entities(url));
        }
    }
    let joined = if url.starts_with("//") {
        format!("https:{url}")
    } else if url.starts_with('/') {
        format!("https://duckduckgo.com{url}")
    } else {
        return None;
    };
    let parsed = reqwest::Url::parse(&joined).ok()?;
    if matches!(parsed.path(), "/l/" | "/l") {
        for (key, value) in parsed.query_pairs() {
            if key == "uddg" {
                return Some(decode_html_entities(value.as_ref()));
            }
        }
    }
    Some(joined)
}

fn host_matches_list(url: &str, domains: &[String]) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    domains.iter().any(|domain| {
        let normalized = normalize_domain_filter(domain);
        !normalized.is_empty() && (host == normalized || host.ends_with(&format!(".{normalized}")))
    })
}

fn normalize_domain_filter(domain: &str) -> String {
    let trimmed = domain.trim();
    reqwest::Url::parse(trimmed)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| trimmed.to_string())
        .trim()
        .trim_start_matches('.')
        .trim_end_matches('/')
        .to_ascii_lowercase()
}

fn is_search_backend_navigation(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return true;
    };
    matches!(
        parsed
            .host_str()
            .unwrap_or_default()
            .trim_start_matches("www.")
            .to_ascii_lowercase()
            .as_str(),
        "duckduckgo.com"
            | "html.duckduckgo.com"
            | "lite.duckduckgo.com"
            | "search.brave.com"
            | "search.yahoo.com"
            | "yahoo.com"
            | "bing.com"
            | "google.com"
    )
}

fn is_http_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn html_to_text(html: &str) -> String {
    let mut text = String::with_capacity(html.len());
    let mut in_tag = false;
    for character in html.chars() {
        match character {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => text.push(character),
            _ => {}
        }
    }
    collapse_whitespace(&decode_html_entities(&text))
}

fn decode_html_entities(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
}

fn collapse_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn preview_text(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    format!(
        "{}…",
        input.chars().take(max_chars).collect::<String>().trim_end()
    )
}

fn compact_error(value: &str) -> String {
    preview_text(&collapse_whitespace(value), 240)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_is_explicit_or_inferred_without_hidden_policy() {
        assert_eq!(
            resolve_intent(SearchIntent::General, "github rust"),
            SearchIntent::General
        );
        assert_eq!(
            resolve_intent(SearchIntent::Auto, "github rust repository"),
            SearchIntent::Code
        );
        assert_eq!(
            resolve_intent(SearchIntent::Auto, "最新研究论文"),
            SearchIntent::Research
        );
        assert_eq!(
            resolve_intent(SearchIntent::Auto, "量子计算是什么"),
            SearchIntent::Knowledge
        );
    }

    #[test]
    fn canonical_url_removes_tracking_without_collapsing_distinct_resources() {
        assert_eq!(
            canonical_url("https://Example.com/docs/?utm_source=x&a=1#top").as_deref(),
            Some("https://example.com/docs?a=1")
        );
        assert_ne!(
            canonical_url("https://example.com/docs?a=1"),
            canonical_url("https://example.com/docs?a=2")
        );
    }

    #[test]
    fn fusion_deduplicates_and_preserves_source_evidence() {
        let hits = fuse_candidates(
            vec![
                SearchCandidate {
                    title: "Short".into(),
                    url: "https://example.com/item?utm_source=a".into(),
                    snippet: String::new(),
                    source: "general".into(),
                    rank: 1,
                },
                SearchCandidate {
                    title: "A more complete title".into(),
                    url: "https://example.com/item".into(),
                    snippet: "Useful context".into(),
                    source: "knowledge".into(),
                    rank: 2,
                },
            ],
            8,
            SearchRecency::Any,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].title, "A more complete title");
        assert_eq!(hits[0].sources, vec!["general", "knowledge"]);
    }

    #[test]
    fn publisher_deduplication_keeps_one_result_per_publisher() {
        let hits = fuse_candidates(
            vec![
                SearchCandidate {
                    title: "Primary".into(),
                    url: "https://example.com/a".into(),
                    snippet: String::new(),
                    source: "general".into(),
                    rank: 1,
                },
                SearchCandidate {
                    title: "Mirror".into(),
                    url: "https://m.example.com/b".into(),
                    snippet: String::new(),
                    source: "knowledge".into(),
                    rank: 2,
                },
                SearchCandidate {
                    title: "Other publisher".into(),
                    url: "https://other.org/c".into(),
                    snippet: String::new(),
                    source: "general".into(),
                    rank: 3,
                },
            ],
            8,
            SearchRecency::Any,
        );
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|hit| hit.url.contains("other.org")));
    }

    #[test]
    fn recency_window_filters_stale_url_dates() {
        let hits = fuse_candidates(
            vec![
                SearchCandidate {
                    title: "Fresh".into(),
                    url: "https://example.com/2026/08/12/post".into(),
                    snippet: String::new(),
                    source: "general".into(),
                    rank: 1,
                },
                SearchCandidate {
                    title: "Stale".into(),
                    url: "https://example.org/2001/01/01/old".into(),
                    snippet: String::new(),
                    source: "general".into(),
                    rank: 2,
                },
            ],
            8,
            SearchRecency::Year,
        );
        assert_eq!(hits.len(), 1);
        assert!(hits[0].url.contains("2026"));
    }

    #[test]
    fn publisher_key_is_public_suffix_aware() {
        assert_eq!(publisher_key("https://example.com/a"), "example.com");
        assert_eq!(publisher_key("https://docs.example.com/b"), "example.com");
        assert_eq!(publisher_key("https://example.co.uk/a"), "example.co.uk");
        assert_eq!(publisher_key("https://other.co.uk/b"), "other.co.uk");
        assert_eq!(publisher_key("https://a.com.cn/x"), "a.com.cn");
        assert_ne!(
            publisher_key("https://example.co.uk/"),
            publisher_key("https://other.co.uk/")
        );
    }

    #[test]
    fn publisher_deduplication_keeps_distinct_co_uk_sites() {
        let hits = fuse_candidates(
            vec![
                SearchCandidate {
                    title: "First UK site".into(),
                    url: "https://example.co.uk/a".into(),
                    snippet: String::new(),
                    source: "general".into(),
                    rank: 1,
                },
                SearchCandidate {
                    title: "Second UK site".into(),
                    url: "https://other.co.uk/b".into(),
                    snippet: String::new(),
                    source: "general".into(),
                    rank: 2,
                },
            ],
            8,
            SearchRecency::Any,
        );
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn public_json_sources_are_parsed_without_credentials() {
        let github = parse_github(
            r#"{"items":[{"full_name":"cowd/runtime","html_url":"https://github.com/cowd/runtime","description":"AI runtime"}]}"#,
        );
        assert_eq!(github.len(), 1);
        let crossref = parse_crossref(
            r#"{"message":{"items":[{"title":["Paper"],"DOI":"10.1/test","URL":"https://doi.org/10.1/test"}]}}"#,
        );
        assert_eq!(crossref.len(), 1);
        let wiki = parse_mediawiki(
            r#"{"query":{"search":[{"pageid":42,"title":"Harness","snippet":"AI orchestration"}]}}"#,
        );
        assert_eq!(wiki.len(), 1);
    }

    #[test]
    #[ignore = "run scripts/test/public-search-live.sh; requires public Internet access"]
    fn live_no_key_sources_cover_code_research_and_knowledge() {
        for (query, intent) in [
            ("rust async runtime github", SearchIntent::Code),
            (
                "AI agent harness evaluation research",
                SearchIntent::Research,
            ),
            ("人工智能是什么", SearchIntent::Knowledge),
        ] {
            let output = execute_web_search(&WebSearchInput {
                query: query.to_string(),
                allowed_domains: None,
                blocked_domains: None,
                intent,
                depth: SearchDepth::Standard,
                max_results: Some(5),
                locale: Some("zh-CN".to_string()),
                recency: SearchRecency::Any,
            })
            .unwrap_or_else(|error| panic!("{intent:?} search failed: {error}"));
            assert!(!output.results.is_empty(), "{intent:?}");
            assert!(
                output
                    .sources
                    .iter()
                    .any(|receipt| receipt.status == "ok" && receipt.result_count > 0),
                "{intent:?} produced no successful source receipt: {:?}",
                output.sources
            );
        }
    }
}
