"""
Unit tests for parsers/ats_posted_time.py.

This project had no test suite at all prior to this file. These tests cover
the pure, deterministic logic in this module (date parsing, freshness
filtering, role/title matching, listing-page detection, location matching)
without touching the network or a browser.
"""

from datetime import datetime, timedelta, timezone

from parsers.ats_posted_time import (
    _looks_isoish,
    _json_ld_date_posted,
    _meta_date,
    _relative_posted_phrase_to_iso_date,
    accept_normalized_posted_string,
    filter_ats_jobs_by_posted_within_days,
    is_probably_ats_listing_page,
    job_title_matches_search_role,
    location_matches_search_locations,
    parse_posted_time_to_utc_datetime,
    role_match_phrases,
    title_matches_search_roles,
)


# --- parse_posted_time_to_utc_datetime --------------------------------------


def test_parse_posted_time_iso_with_z():
    dt = parse_posted_time_to_utc_datetime("2026-06-01T12:00:00Z")
    assert dt == datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_posted_time_iso_with_offset():
    dt = parse_posted_time_to_utc_datetime("2026-06-01T12:00:00-05:00")
    assert dt == datetime(2026, 6, 1, 17, 0, 0, tzinfo=timezone.utc)


def test_parse_posted_time_date_only():
    dt = parse_posted_time_to_utc_datetime("2026-06-01")
    assert dt == datetime(2026, 6, 1, tzinfo=timezone.utc)


def test_parse_posted_time_naive_datetime_assumed_utc():
    dt = parse_posted_time_to_utc_datetime("2026-06-01T12:00:00")
    assert dt == datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_posted_time_empty_or_none_returns_none():
    assert parse_posted_time_to_utc_datetime("") is None
    assert parse_posted_time_to_utc_datetime(None) is None


def test_parse_posted_time_garbage_returns_none():
    assert parse_posted_time_to_utc_datetime("not a date") is None
    assert parse_posted_time_to_utc_datetime("2026/06/01") is None


def test_parse_posted_time_malformed_iso_returns_none():
    # Looks ISO-ish (has a 'T') but isn't actually parseable.
    assert parse_posted_time_to_utc_datetime("2026-13-99T99:99:99Z") is None


# --- filter_ats_jobs_by_posted_within_days ----------------------------------


def _iso(days_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).date().isoformat()


def test_filter_keeps_jobs_within_window():
    jobs = [{"posted_time": _iso(1)}, {"posted_time": _iso(3)}]
    kept, dropped_old, dropped_bad = filter_ats_jobs_by_posted_within_days(jobs, max_days=7)
    assert len(kept) == 2
    assert dropped_old == 0
    assert dropped_bad == 0


def test_filter_drops_stale_jobs():
    jobs = [{"posted_time": _iso(1)}, {"posted_time": _iso(30)}]
    kept, dropped_old, dropped_bad = filter_ats_jobs_by_posted_within_days(jobs, max_days=7)
    assert len(kept) == 1
    assert dropped_old == 1
    assert dropped_bad == 0


def test_filter_drops_unparseable_posted_time_rather_than_keeping_it():
    """Unknown dates must not bypass the freshness rule (per the function's docstring)."""
    jobs = [{"posted_time": ""}, {"posted_time": "garbage"}, {"posted_time": _iso(1)}]
    kept, dropped_old, dropped_bad = filter_ats_jobs_by_posted_within_days(jobs, max_days=7)
    assert len(kept) == 1
    assert dropped_bad == 2


def test_filter_max_days_zero_or_negative_returns_all_jobs_unfiltered():
    jobs = [{"posted_time": _iso(1000)}, {"posted_time": "garbage"}]
    kept, dropped_old, dropped_bad = filter_ats_jobs_by_posted_within_days(jobs, max_days=0)
    assert kept == jobs
    assert dropped_old == 0
    assert dropped_bad == 0


def test_filter_empty_job_list():
    kept, dropped_old, dropped_bad = filter_ats_jobs_by_posted_within_days([], max_days=7)
    assert kept == []
    assert dropped_old == 0
    assert dropped_bad == 0


# --- role_match_phrases / job_title_matches_search_role ---------------------


def test_role_match_phrases_splits_on_pipe_and_comma():
    assert role_match_phrases("Data Analyst | Data Scientist, ML Engineer") == [
        "data analyst",
        "data scientist",
        "ml engineer",
    ]


def test_role_match_phrases_expands_slash_shorthand():
    # "AI/ML Engineer" -> "ai engineer", "ml engineer" (shared right-hand word)
    assert role_match_phrases("AI/ML Engineer") == ["ai engineer", "ml engineer"]


def test_role_match_phrases_empty_input():
    assert role_match_phrases("") == []
    assert role_match_phrases(None) == []


def test_job_title_matches_search_role_whole_phrase_only():
    """'data analyst' should match 'Senior Data Analyst' but not 'Data Entry Officer'."""
    assert job_title_matches_search_role("Senior Data Analyst", "Data Analyst") is True
    assert job_title_matches_search_role("Data Entry Officer", "Data Analyst") is False


def test_job_title_matches_search_role_avoids_substring_false_positive():
    """'analyst' should not match just because 'Privacy Operations Analyst' shares a word
    with a different intended phrase — this checks the full phrase, not fragments."""
    assert job_title_matches_search_role("Privacy Operations Analyst", "Data Analyst") is False


def test_title_matches_search_roles_empty_roles_matches_everything():
    assert title_matches_search_roles("Anything At All", []) is True
    assert title_matches_search_roles("Anything At All", None) is True


def test_title_matches_search_roles_empty_title_with_roles_configured():
    assert title_matches_search_roles("", ["Data Analyst"]) is False


# --- is_probably_ats_listing_page -------------------------------------------


def test_is_listing_page_greenhouse_board_root():
    assert is_probably_ats_listing_page("https://job-boards.greenhouse.io/acme") is True


def test_is_listing_page_greenhouse_single_job_is_not_a_listing():
    assert is_probably_ats_listing_page("https://job-boards.greenhouse.io/acme/jobs/12345") is False


def test_is_listing_page_greenhouse_gh_jid_query_param_is_not_a_listing():
    assert is_probably_ats_listing_page("https://boards.greenhouse.io/acme?gh_jid=12345") is False


def test_is_listing_page_lever_company_root():
    assert is_probably_ats_listing_page("https://jobs.lever.co/acme") is True


def test_is_listing_page_lever_single_posting_uuid_is_not_a_listing():
    assert (
        is_probably_ats_listing_page(
            "https://jobs.lever.co/acme/123e4567-e89b-12d3-a456-426614174000"
        )
        is False
    )


def test_is_listing_page_generic_careers_suffix():
    assert is_probably_ats_listing_page("https://acme.com/careers") is True


def test_is_listing_page_empty_url():
    assert is_probably_ats_listing_page("") is False
    assert is_probably_ats_listing_page(None) is False


# --- location_matches_search_locations --------------------------------------


def test_location_matches_substring():
    assert location_matches_search_locations("San Francisco, CA", ["san francisco"]) is True


def test_location_matches_us_aliases():
    assert location_matches_search_locations("Remote - United States", ["us"]) is True
    assert location_matches_search_locations("Remote - United States", ["usa"]) is True


def test_location_no_match():
    assert location_matches_search_locations("Berlin, Germany", ["san francisco"]) is False


def test_location_empty_location_string():
    assert location_matches_search_locations("", ["us"]) is False


# --- _relative_posted_phrase_to_iso_date ------------------------------------


def test_relative_phrase_days_ago():
    result = _relative_posted_phrase_to_iso_date("Posted 5 days ago")
    expected = (datetime.now(timezone.utc) - timedelta(days=5)).date().isoformat()
    assert result == expected


def test_relative_phrase_hours_ago():
    result = _relative_posted_phrase_to_iso_date("Posted 3 hours ago")
    expected = (datetime.now(timezone.utc) - timedelta(hours=3)).date().isoformat()
    assert result == expected


def test_relative_phrase_caps_absurd_day_counts():
    """730-day cap: an absurd '99999 days ago' should not produce a wildly wrong date."""
    result = _relative_posted_phrase_to_iso_date("Posted 99999 days ago")
    expected = (datetime.now(timezone.utc) - timedelta(days=730)).date().isoformat()
    assert result == expected


def test_relative_phrase_no_match_returns_none():
    assert _relative_posted_phrase_to_iso_date("Apply now for this great role") is None


# --- accept_normalized_posted_string / _looks_isoish ------------------------


def test_accept_normalized_posted_string_valid():
    assert accept_normalized_posted_string("2026-06-01") == "2026-06-01"


def test_accept_normalized_posted_string_rejects_bogus_fragment():
    assert accept_normalized_posted_string("not-a-real-date") is None


def test_accept_normalized_posted_string_empty():
    assert accept_normalized_posted_string("") is None


def test_looks_isoish_true():
    assert _looks_isoish("2026-06-01T00:00:00Z") is True


def test_looks_isoish_rejects_urls():
    assert _looks_isoish("https://example.com/2026-06-01") is False


def test_looks_isoish_rejects_short_strings():
    assert _looks_isoish("2026-06") is False


# --- _json_ld_date_posted / _meta_date (HTML parsing) -----------------------


def test_json_ld_date_posted_extracts_from_job_posting():
    html = """
    <html><head>
    <script type="application/ld+json">
    {"@type": "JobPosting", "title": "Engineer", "datePosted": "2026-06-01"}
    </script>
    </head></html>
    """
    assert _json_ld_date_posted(html) == "2026-06-01"


def test_json_ld_date_posted_ignores_non_job_posting_types():
    html = """
    <script type="application/ld+json">
    {"@type": "Organization", "datePosted": "2026-06-01"}
    </script>
    """
    assert _json_ld_date_posted(html) is None


def test_json_ld_date_posted_no_script_tag():
    assert _json_ld_date_posted("<html><body>No JSON-LD here</body></html>") is None


def test_meta_date_extracts_article_published_time():
    html = '<meta property="article:published_time" content="2026-06-01T00:00:00Z">'
    assert _meta_date(html) == "2026-06-01T00:00:00Z"


def test_meta_date_rejects_non_isoish_content():
    html = '<meta property="article:published_time" content="last week">'
    assert _meta_date(html) is None


def test_meta_date_no_matching_meta_tag():
    assert _meta_date("<html><head></head></html>") is None
