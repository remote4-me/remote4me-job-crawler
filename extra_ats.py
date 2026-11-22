"""Additional ATS adapters ported from the MIT-licensed ashby-job-scraper project.

Remote4Me-specific policy is applied here: only customer-support-related titles,
India/USA locations, Remote/Hybrid arrangements, recent postings and direct job
URLs are allowed into the existing country/workplace tabs.
"""
import re
from urllib.parse import quote, urlparse

import requests


def _json(c, url, timeout=45, params=None, method="GET", body=None):
    try:
        response = c.SESSION.request(
            method, url, params=params, json=body, timeout=timeout
        )
        if response.status_code != 200:
            return None
        return response.json()
    except (requests.RequestException, ValueError):
        return None


def _clean_location(value):
    if isinstance(value, dict):
        return ", ".join(str(value.get(k) or "").strip() for k in
                         ("name", "city", "region", "country") if value.get(k))
    if isinstance(value, list):
        return ", ".join(_clean_location(x) for x in value if x)
    return str(value or "").strip()


def _job(c, ats, slug, raw, title, location, link, job_id,
         posted="", country_code="", workplace_type="", remote=False,
         description="", company=None, structured=None):
    title = c.clean_text(title)
    if not c.title_is_relevant(title):
        return None
    location = c.clean_text(location)
    workplace = c.classify_workplace(
        location=location,
        workplace_type=workplace_type,
        remote_flag=remote,
        description=description,
        structured_data=structured or raw,
    )
    country = c.classify_country(
        location=location,
        country_code=country_code,
        description=description,
        workplace=workplace,
    )
    if not c.location_is_allowed(country, workplace):
        return None
    posted_date = c.parse_date(posted)
    if not c.is_recent(posted_date):
        return None
    link = c.clean_text(link)
    if not link or not link.startswith(("https://", "http://")):
        return None
    if not job_id:
        job_id = c.normalize_url(link)
    return {
        "ats": ats,
        "company": company or c.humanize_company_slug(slug.split("/")[0]),
        "job_id": str(job_id),
        "title": title,
        "location": location,
        "country": country,
        "workplace": workplace,
        "posted_date": posted_date,
        "job_url": link,
    }


def _fetch_recruitee(c, slug):
    data = _json(c, f"https://{slug}.recruitee.com/api/offers/")
    if not isinstance(data, dict):
        return []
    out = []
    for raw in data.get("offers", []):
        if raw.get("status") not in (None, "published"):
            continue
        location = raw.get("location") or raw.get("location_name") or ""
        job = _job(c, "recruitee", slug, raw, raw.get("title"), location,
                   raw.get("careers_url") or raw.get("careers_apply_url"),
                   raw.get("id"), raw.get("published_at") or raw.get("created_at"),
                   workplace_type=raw.get("workplace_type") or raw.get("remote_type"),
                   remote=raw.get("remote", False),
                   description=raw.get("description") or "",
                   company=raw.get("company_name"))
        if job: out.append(job)
    return out


def _fetch_teamtailor(c, slug):
    data = _json(c, f"https://{slug}.teamtailor.com/jobs.json")
    if not isinstance(data, dict):
        return []
    out = []
    for raw in data.get("items", []):
        jp = raw.get("_jobposting") or {}
        locs = jp.get("jobLocation") or []
        if not isinstance(locs, list): locs = [locs]
        parts = []
        for loc in locs:
            addr = (loc or {}).get("address") or {}
            postal = addr.get("postalAddress") or addr
            parts.extend(str(postal.get(k) or "") for k in
                         ("addressLocality", "addressRegion", "addressCountry") if postal.get(k))
        location = ", ".join(dict.fromkeys(parts)) or _clean_location(raw.get("location"))
        workplace_type = jp.get("jobLocationType") or raw.get("workplaceType") or ""
        link = raw.get("url")
        job = _job(c, "teamtailor", slug, raw, raw.get("title"), location, link,
                   raw.get("id"), jp.get("datePosted") or raw.get("date_published"),
                   workplace_type=workplace_type,
                   remote=bool(re.search(r"telecommute|remote", str(workplace_type), re.I)),
                   description=raw.get("content_text") or raw.get("content_html") or jp.get("description") or "")
        if job: out.append(job)
    return out


def _fetch_pinpoint(c, slug):
    data = _json(c, f"https://{slug}.pinpointhq.com/postings.json")
    if not isinstance(data, dict):
        return []
    out = []
    for raw in data.get("data", []):
        loc = raw.get("location") or {}
        location = _clean_location(loc.get("name") or loc.get("city") or loc)
        workplace_type = raw.get("workplace_type_text") or raw.get("workplace_type") or ""
        link = raw.get("url")
        job = _job(c, "pinpoint", slug, raw, raw.get("title"), location, link,
                   raw.get("id"), raw.get("published_at") or raw.get("created_at"),
                   workplace_type=workplace_type,
                   remote=bool(re.search(r"remote", str(workplace_type), re.I)),
                   description=raw.get("description") or "")
        if job: out.append(job)
    return out


def _fetch_workday(c, slug):
    parts = slug.split("/")
    if len(parts) != 3:
        return []
    tenant, wd_host, site = parts
    base = f"https://{tenant}.{wd_host}.myworkdayjobs.com"
    endpoint = f"{base}/wday/cxs/{tenant}/{site}/jobs"
    out = []
    for page in range(10):
        data = _json(c, endpoint, timeout=60, method="POST",
                     body={"appliedFacets": {}, "limit": 20,
                           "offset": page * 20, "searchText": ""})
        if not isinstance(data, dict) or not isinstance(data.get("jobPostings"), list):
            break
        rows = data["jobPostings"]
        for raw in rows:
            path = raw.get("externalPath") or ""
            if not path: continue
            link = base + "/" + site + path
            location = raw.get("locationsText") or ""
            remote_type = raw.get("remoteType") or ""
            job = _job(c, "workday", slug, raw, raw.get("title"), location,
                       link, path.rstrip("/").split("/")[-1],
                       raw.get("postedOn"), workplace_type=remote_type,
                       remote=bool(re.search(r"remote", remote_type, re.I)))
            if job: out.append(job)
        if len(rows) < 20: break
    return out


def _fetch_keka(c, slug):
    board = f"https://{slug}.keka.com/careers/"
    try:
        response = c.SESSION.get(board, timeout=30)
        if response.status_code != 200: return []
        match = re.search(r"/ats/documents/([0-9a-f-]{36})/", response.text, re.I)
        if not match: return []
        org_id = match.group(1)
    except requests.RequestException:
        return []
    data = _json(c, f"{board}api/embedjobs/default/active/{org_id}", timeout=45)
    if not isinstance(data, list): return []
    out = []
    for raw in data:
        locs = raw.get("jobLocations") or []
        loc = locs[0] if locs else {}
        location = ", ".join(str(loc.get(k) or "") for k in
                             ("city", "name", "countryName") if loc.get(k))
        job_id = raw.get("id")
        if not job_id: continue
        link = f"{board}jobdetails/{job_id}"
        job = _job(c, "keka", slug, raw, raw.get("title"), location, link,
                   job_id, raw.get("publishedOn"),
                   workplace_type=raw.get("workplaceType") or raw.get("workplace_type") or "",
                   remote=bool(raw.get("remote", False)),
                   description=raw.get("description") or "")
        if job: out.append(job)
    return out


FETCHERS = {
    "recruitee": _fetch_recruitee,
    "teamtailor": _fetch_teamtailor,
    "pinpoint": _fetch_pinpoint,
    "workday": _fetch_workday,
    "keka": _fetch_keka,
}


def validate(c, ats, slug):
    try:
        # A structurally valid empty response is still a live board.
        if ats == "recruitee":
            d = _json(c, f"https://{slug}.recruitee.com/api/offers/")
            return isinstance(d, dict) and isinstance(d.get("offers"), list)
        if ats == "teamtailor":
            d = _json(c, f"https://{slug}.teamtailor.com/jobs.json")
            return isinstance(d, dict) and isinstance(d.get("items"), list)
        if ats == "pinpoint":
            d = _json(c, f"https://{slug}.pinpointhq.com/postings.json")
            return isinstance(d, dict) and isinstance(d.get("data"), list)
        if ats == "workday":
            parts = slug.split("/")
            if len(parts) != 3: return False
            tenant, wd_host, site = parts
            d = _json(c, f"https://{tenant}.{wd_host}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs",
                      timeout=45, method="POST",
                      body={"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""})
            return isinstance(d, dict) and isinstance(d.get("jobPostings"), list)
        if ats == "keka":
            board = f"https://{slug}.keka.com/careers/"
            r = c.SESSION.get(board, timeout=25)
            return r.status_code == 200 and bool(re.search(r"/ats/documents/[0-9a-f-]{36}/", r.text, re.I))
    except Exception:
        return False
    return False


def company_name(ats, slug):
    value = slug.split("/")[0] if ats == "workday" else slug.replace(".na", "")
    return re.sub(r"[-_]+", " ", value).strip().title()


def board_url(ats, slug):
    if ats == "recruitee": return f"https://{slug}.recruitee.com/"
    if ats == "teamtailor":
        host = f"{slug}.teamtailor.com" if not slug.endswith(".na") else f"{slug}.teamtailor.com"
        return f"https://{host}/jobs"
    if ats == "pinpoint": return f"https://{slug}.pinpointhq.com/postings"
    if ats == "workday":
        tenant, wd_host, site = slug.split("/")
        return f"https://{tenant}.{wd_host}.myworkdayjobs.com/{site}"
    if ats == "keka": return f"https://{slug}.keka.com/careers/"
    return ""
