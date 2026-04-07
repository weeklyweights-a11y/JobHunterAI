"""Run: python scripts/smoke_jobright_parser.py"""
from parsers.jobright_parser import parse_jobright_detail_html

h = r"""<html><script id="jobright-helper-job-detail-info">{"jobResult":{"jobTitle":"ML Engineer","jobLocation":"NYC","publishTime":"2026-04-04 20:58:08","publishTimeDesc":"5h ago","originalUrl":"https://example.com/jobs/1","jobId":"x","jobSummary":"Do AI","employmentType":"Full-time","jobSeniority":"Mid","salaryDesc":"$","workModel":"remote","isRemote":true,"applicantsCount":"10"},"companyResult":{"companyName":"Acme"},"displayScore":0.9}</script></html>"""
r = parse_jobright_detail_html(h, search_role="ML", search_location="US")
assert r and r["url"] == "https://example.com/jobs/1" and r["source"] == "jobright"
print("smoke_jobright_parser: OK", r["title"])
