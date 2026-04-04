"""Quick parser check (no Playwright). Run: python scripts/smoke_linkedin_parser.py"""
from parsers.linkedin_parser import discover_selectors, parse_linkedin_jobs

html = """
<ul class="jobs-search__results-list">
  <li>
    <a class="base-card__full-link" href="/jobs/view/123456?trk=abc">
      <h3 class="base-search-card__title">Software Engineer</h3>
    </a>
    <h4 class="base-search-card__subtitle">Acme Corp</h4>
  </li>
</ul>
"""
jobs = parse_linkedin_jobs(html)
assert len(jobs) == 1, jobs
assert jobs[0]["url"] == "https://www.linkedin.com/jobs/view/123456", jobs[0]
r = discover_selectors(html)
assert r["pattern"] == "merged" and r["card_count"] == 1, r
print("smoke_linkedin_parser: OK", jobs[0])
