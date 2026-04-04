"""BeautifulSoup parsers for Tier 1 job boards."""

from parsers.indeed_parser import parse_indeed_jobs_html
from parsers.linkedin_parser import (
    discover_selectors,
    parse_linkedin_jobs,
    parse_linkedin_jobs_html,
)

__all__ = [
    "discover_selectors",
    "parse_indeed_jobs_html",
    "parse_linkedin_jobs",
    "parse_linkedin_jobs_html",
]
