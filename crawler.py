import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# REMOTE4.ME JOB CRAWLER
# ============================================================
#
# Purpose:
#   Discover public ATS boards and collect relevant jobs.
#
# Current niche:
#   Customer Support
#   Technical Support
#   Customer Success
#
# Current countries:
#   India
#   USA
#
# Supported ATS:
#   Greenhouse
#   Ashby
#   Lever
#
# Runs automatically through GitHub Actions.
# ============================================================


# ============================================================
# CONFIGURATION
# ============================================================

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")

GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")

# Maximum number of concurrent requests.
# Keep this moderate to avoid hammering public endpoints.
CONCURRENCY = 8

# How many days of newly discovered jobs to keep.
# We use 30 days because some ATS systems expose older postings.
MAX_JOB_AGE_DAYS = 30

# How many ATS boards to scan in one GitHub run.
# 0 = all discovered boards.
MAX_BOARDS_PER_RUN = 0

# Refresh board discovery every run.
# This helps discover newly appearing companies.
REFRESH_BOARDS = True


# ============================================================
# ATS SOURCES
# ============================================================

ATS_SOURCES = {
    "greenhouse": {
        "archive_domains": [
            "boards.greenhouse.io",
            "job-boards.greenhouse.io",
        ],
        "api": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
    },

    "ashby": {
        "archive_domains": [
            "jobs.ashbyhq.com",
        ],
        "api": "https://api.ashbyhq.com/posting-api/job-board/{slug}",
    },

    "lever": {
        "archive_domains": [
            "jobs.lever.co",
        ],
        "api": "https://api.lever.co/v0/postings/{slug}?mode=json",
    },
}


# ============================================================
# YOUR REMOTE4.ME NICHE
# ============================================================

TARGET_TITLE_PATTERNS = [
    # Customer Support
    r"\bcustomer support\b",
    r"\bcustomer service\b",
    r"\bcustomer care\b",
    r"\bcustomer experience\b",
    r"\bsupport specialist\b",
    r"\bsupport representative\b",
    r"\bsupport associate\b",
    r"\bclient support\b",
    r"\bmember support\b",

    # Technical Support
    r"\btechnical support\b",
    r"\btechnical support specialist\b",
    r"\btechnical support engineer\b",
    r"\bsupport engineer\b",
    r"\bproduct support\b",
    r"\bsoftware support\b",
    r"\bapplication support\b",
    r"\bit support\b",
    r"\bhelp desk\b",
    r"\bhelpdesk\b",
    r"\bservice desk\b",

    # Customer Success
    r"\bcustomer success\b",
    r"\bcustomer success specialist\b",
    r"\bcustomer success manager\b",
    r"\bclient success\b",
    r"\bcustomer onboarding\b",
    r"\bclient onboarding\b",
    r"\bonboarding specialist\b",
    r"\bimplementation specialist\b",
    r"\bcustomer implementation\b",
    r"\bclient implementation\b",
]


TITLE_REGEX = re.compile(
    "|".join(TARGET_TITLE_PATTERNS),
    re.IGNORECASE,
)


# ============================================================
# LOCATION KEYWORDS
# ============================================================

INDIA_PATTERNS = [
    r"\bindia\b",
    r"\bbengaluru\b",
    r"\bbangalore\b",
    r"\bhyderabad\b",
    r"\bpune\b",
    r"\bmumbai\b",
    r"\bdelhi\b",
    r"\bnew delhi\b",
    r"\bnoida\b",
    r"\bgurgaon\b",
    r"\bgurugram\b",
    r"\bchennai\b",
    r"\bkolkata\b",
    r"\bahmedabad\b",
    r"\bkochi\b",
    r"\bkerala\b",
    r"\bjaipur\b",
    r"\bindian\b",
]

USA_PATTERNS = [
    r"\bunited states\b",
    r"\busa\b",
    r"\bu\.s\.\b",
    r"\bunited states of america\b",
    r"\bcalifornia\b",
    r"\btexas\b",
    r"\bnew york\b",
    r"\bflorida\b",
    r"\bwashington\b",
    r"\billinois\b",
    r"\bmassachusetts\b",
    r"\bcolorado\b",
    r"\bgeorgia\b",
    r"\baustin\b",
    r"\bseattle\b",
    r"\bboston\b",
    r"\bchicago\b",
    r"\bsan francisco\b",
    r"\blos angeles\b",
    r"\bnew york city\b",
]

REMOTE_PATTERNS = [
    r"\bremote\b",
    r"\bwork from home\b",
    r"\bdistributed\b",
    r"\banywhere\b",
]


INDIA_REGEX = re.compile(
    "|".join(INDIA_PATTERNS),
    re.IGNORECASE,
)

USA_REGEX = re.compile(
    "|".join(USA_PATTERNS),
    re.IGNORECASE,
)

REMOTE_REGEX = re.compile(
    "|".join(REMOTE_PATTERNS),
    re.IGNORECASE,
)


# ============================================================
# HTTP SESSION
# ============================================================

SESSION = requests.Session()

SESSION.headers.update({
    "User-Agent": "Remote4.me Job Crawler/1.0",
    "Accept": "application/json,text/plain,*/*",
})


# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_google_sheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID GitHub secret.")

    if not GOOGLE_CREDENTIALS:
        raise RuntimeError("Missing GOOGLE_CREDENTIALS GitHub secret.")

    credentials_info = json.loads(GOOGLE_CREDENTIALS)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=scopes,
    )

    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = spreadsheet.worksheet("Jobs")
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title="Jobs",
            rows=1000,
            cols=10,
        )

    return worksheet


# ============================================================
# HELPERS
# ============================================================

def clean_text(value):
    if value is None:
        return ""

    value = str(value)

    value = re.sub(r"\s+", " ", value)

    return value.strip()


def normalize_url(url):
    if not url:
        return ""

    url = url.strip()

    # Remove tracking query strings where possible.
    parsed = urlparse(url)

    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    return clean.rstrip("/")


def make_job_key(ats, job_id, url):
    if job_id:
        raw = f"{ats}:{job_id}"
    else:
        raw = f"{ats}:{normalize_url(url)}"

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()


def parse_date(value):
    if not value:
        return ""

    value = str(value)

    # ISO timestamp
    try:
        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        return dt.date().isoformat()

    except Exception:
        pass

    # Lever epoch milliseconds
    try:
        number = int(value)

        if number > 100000000000:
            dt = datetime.fromtimestamp(
                number / 1000,
                tz=timezone.utc,
            )

            return dt.date().isoformat()

    except Exception:
        pass

    return ""


# ============================================================
# LOCATION CLASSIFICATION
# ============================================================

def classify_location(location, remote=False):
    location = clean_text(location)

    if INDIA_REGEX.search(location):
        return "India"

    if USA_REGEX.search(location):
        return "USA"

    # For genuinely remote jobs, we need to inspect the wording.
    #
    # We do NOT automatically accept every worldwide remote job,
    # because your current site focus is India + USA.
    #
    # "Worldwide", "Global", etc. are kept as ambiguous and
    # rejected unless the posting explicitly mentions India or USA.

    if remote or REMOTE_REGEX.search(location):
        return ""

    return ""


# ============================================================
# TITLE FILTER
# ============================================================

def is_target_title(title):
    if not title:
        return False

    return bool(TITLE_REGEX.search(title))


# ============================================================
# DATE FILTER
# ============================================================

def is_recent(posted_date):
    if not posted_date:
        # Unknown dates are allowed rather than falsely
        # assigning today's date.
        return True

    try:
        dt = datetime.strptime(
            posted_date,
            "%Y-%m-%d",
        ).date()

        cutoff = (
            datetime.now(timezone.utc).date()
            - timedelta(days=MAX_JOB_AGE_DAYS)
        )

        return dt >= cutoff

    except Exception:
        return True


# ============================================================
# ATS BOARD DISCOVERY
# ============================================================

def extract_slug_from_url(url, domain):
    try:
        parsed = urlparse(url)

        if parsed.netloc.lower() != domain.lower():
            return None

        path = parsed.path.strip("/")

        if not path:
            return None

        slug = unquote(
            path.split("/")[0]
        ).strip()

        if not slug:
            return None

        # Ignore obvious non-board paths.
        blocked = {
            "jobs",
            "job",
            "careers",
            "career",
            "about",
            "privacy",
            "terms",
            "login",
            "apply",
            "search",
            "assets",
            "static",
        }

        if slug.lower() in blocked:
            return None

        # Reasonable board slug validation.
        if len(slug) < 2 or len(slug) > 150:
            return None

        return slug

    except Exception:
        return None


def discover_from_wayback(domain):
    """
    Discover candidate board URLs from Internet Archive CDX.

    We request URL keys only, which keeps the response smaller.
    """

    candidates = set()

    url = (
        "https://web.archive.org/cdx/search/cdx"
        f"?url={domain}/*"
        "&output=json"
        "&fl=original"
        "&filter=statuscode:200"
        "&collapse=urlkey"
        "&limit=100000"
    )

    try:
        response = SESSION.get(
            url,
            timeout=60,
        )

        if response.status_code != 200:
            print(
                f"Wayback returned {response.status_code} "
                f"for {domain}"
            )

            return candidates

        data = response.json()

        for item in data:
            if isinstance(item, list):
                if not item:
                    continue

                original = item[0]
            else:
                original = str(item)

            slug = extract_slug_from_url(
                original,
                domain,
            )

            if slug:
                candidates.add(slug)

    except Exception as exc:
        print(
            f"Wayback discovery failed for {domain}: {exc}"
        )

    return candidates


def discover_boards_for_ats(ats):
    source = ATS_SOURCES[ats]

    all_candidates = set()

    print(
        f"\nDiscovering {ats} boards..."
    )

    for domain in source["archive_domains"]:

        candidates = discover_from_wayback(
            domain
        )

        print(
            f"  {domain}: "
            f"{len(candidates)} candidates"
        )

        all_candidates.update(
            candidates
        )

    print(
        f"  Total {ats} candidates: "
        f"{len(all_candidates)}"
    )

    return all_candidates


# ============================================================
# BOARD VALIDATION
# ============================================================

def validate_board(ats, slug):
    source = ATS_SOURCES[ats]

    url = source["api"].format(
        slug=slug
    )

    try:
        response = SESSION.head(
            url,
            timeout=15,
            allow_redirects=True,
        )

        if response.status_code == 200:
            return True

        # Some servers don't support HEAD properly.
        if response.status_code in (
            403,
            405,
            429,
        ):
            response = SESSION.get(
                url,
                timeout=20,
                stream=True,
            )

            return response.status_code == 200

    except Exception:
        pass

    return False


def validate_boards(
    ats,
    candidates,
):
    valid = []

    print(
        f"Validating {len(candidates)} "
        f"{ats} boards..."
    )

    with ThreadPoolExecutor(
        max_workers=CONCURRENCY
    ) as executor:

        futures = {
            executor.submit(
                validate_board,
                ats,
                slug,
            ): slug
            for slug in candidates
        }

        completed = 0

        for future in as_completed(
            futures
        ):

            slug = futures[future]

            try:
                if future.result():
                    valid.append(slug)

            except Exception:
                pass

            completed += 1

            if completed % 250 == 0:
                print(
                    f"  Validated "
                    f"{completed}/"
                    f"{len(candidates)}"
                )

    print(
        f"  LIVE {ats} boards: "
        f"{len(valid)}"
    )

    return sorted(
        set(valid),
        key=str.lower,
    )


# ============================================================
# JOB FETCHERS
# ============================================================

def fetch_greenhouse(slug):
    url = (
        ATS_SOURCES["greenhouse"]["api"]
        .format(slug=slug)
    )

    try:
        response = SESSION.get(
            url,
            timeout=30,
        )

        if response.status_code != 200:
            return []

        data = response.json()

        jobs = data.get(
            "jobs",
            [],
        )

        results = []

        for job in jobs:

            title = clean_text(
                job.get("title")
            )

            if not is_target_title(title):
                continue

            location_obj = (
                job.get("location")
                or {}
            )

            location = clean_text(
                location_obj.get("name")
            )

            # Greenhouse doesn't provide a
            # dedicated remote flag here.
            remote = bool(
                REMOTE_REGEX.search(
                    location
                )
            )

            country = classify_location(
                location,
                remote,
            )

            if country not in (
                "India",
                "USA",
            ):
                continue

            posted = parse_date(
                job.get("first_published")
                or job.get("updated_at")
            )

            if not is_recent(posted):
                continue

            job_id = str(
                job.get("id")
                or ""
            )

            link = (
                job.get("absolute_url")
                or ""
            )

            results.append({
                "ats": "greenhouse",
                "company": slug,
                "job_id": job_id,
                "title": title,
                "location": location,
                "country": country,
                "posted_date": posted,
                "job_url": link,
            })

        return results

    except Exception:
        return []


def fetch_ashby(slug):
    url = (
        ATS_SOURCES["ashby"]["api"]
        .format(slug=slug)
    )

    try:
        response = SESSION.get(
            url,
            timeout=30,
        )

        if response.status_code != 200:
            return []

        data = response.json()

        jobs = data.get(
            "jobs",
            [],
        )

        results = []

        for job in jobs:

            # Only listed public postings.
            if job.get(
                "isListed",
                True,
            ) is False:
                continue

            title = clean_text(
                job.get("title")
            )

            if not is_target_title(title):
                continue

            locations = []

            primary = job.get(
                "location"
            )

            if primary:
                locations.append(
                    clean_text(primary)
                )

            for secondary in (
                job.get(
                    "secondaryLocations"
                )
                or []
            ):
                if isinstance(
                    secondary,
                    dict,
                ):
                    loc = secondary.get(
                        "location"
                    )

                    if loc:
                        locations.append(
                            clean_text(loc)
                        )

            location = ", ".join(
                dict.fromkeys(
                    locations
                )
            )

            remote = bool(
                job.get(
                    "isRemote",
                    False,
                )
            )

            country = classify_location(
                location,
                remote,
            )

            if country not in (
                "India",
                "USA",
            ):
                continue

            posted = parse_date(
                job.get(
                    "publishedAt"
                )
            )

            if not is_recent(posted):
                continue

            job_id = str(
                job.get("id")
                or job.get("jobId")
                or ""
            )

            link = (
                job.get("jobUrl")
                or job.get("applyUrl")
                or ""
            )

            results.append({
                "ats": "ashby",
                "company": slug,
                "job_id": job_id,
                "title": title,
                "location": location,
                "country": country,
                "posted_date": posted,
                "job_url": link,
            })

        return results

    except Exception:
        return []


def fetch_lever(slug):
    url = (
        ATS_SOURCES["lever"]["api"]
        .format(slug=slug)
    )

    try:
        response = SESSION.get(
            url,
            timeout=30,
        )

        if response.status_code != 200:
            return []

        data = response.json()

        if not isinstance(
            data,
            list,
        ):
            return []

        results = []

        for job in data:

            title = clean_text(
                job.get("text")
            )

            if not is_target_title(title):
                continue

            categories = (
                job.get("categories")
                or {}
            )

            location = clean_text(
                categories.get(
                    "location"
                )
            )

            workplace = clean_text(
                job.get(
                    "workplaceType"
                )
            )

            remote = (
                workplace.lower()
                == "remote"
                or bool(
                    REMOTE_REGEX.search(
                        location
                    )
                )
            )

            country = classify_location(
                location,
                remote,
            )

            if country not in (
                "India",
                "USA",
            ):
                continue

            posted = parse_date(
                job.get(
                    "createdAt"
                )
            )

            if not is_recent(posted):
                continue

            job_id = str(
                job.get("id")
                or ""
            )

            urls = (
                job.get("urls")
                or {}
            )

            link = (
                urls.get("show")
                or job.get("hostedUrl")
                or job.get("applyUrl")
                or ""
            )

            results.append({
                "ats": "lever",
                "company": slug,
                "job_id": job_id,
                "title": title,
                "location": location,
                "country": country,
                "posted_date": posted,
                "job_url": link,
            })

        return results

    except Exception:
        return []


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "ashby": fetch_ashby,
    "lever": fetch_lever,
}


# ============================================================
# DISCOVER + VALIDATE
# ============================================================

def discover_all_boards():
    boards = {}

    for ats in ATS_SOURCES:

        candidates = discover_boards_for_ats(
            ats
        )

        # Safety limit for an individual discovery
        # operation is intentionally not applied here.
        #
        # We want maximum board discovery.

        valid = validate_boards(
            ats,
            candidates,
        )

        boards[ats] = valid

    return boards


# ============================================================
# CRAWL ALL BOARDS
# ============================================================

def crawl_boards(boards):

    all_jobs = []

    tasks = []

    for ats, slugs in boards.items():

        if MAX_BOARDS_PER_RUN:
            slugs = slugs[
                :MAX_BOARDS_PER_RUN
            ]

        for slug in slugs:
            tasks.append(
                (
                    ats,
                    slug,
                )
            )

    print(
        f"\nTotal boards to crawl: "
        f"{len(tasks)}"
    )

    with ThreadPoolExecutor(
        max_workers=CONCURRENCY
    ) as executor:

        future_map = {}

        for ats, slug in tasks:

            future = executor.submit(
                FETCHERS[ats],
                slug,
            )

            future_map[future] = (
                ats,
                slug,
            )

        completed = 0

        for future in as_completed(
            future_map
        ):

            ats, slug = future_map[
                future
            ]

            try:
                jobs = future.result()

                all_jobs.extend(
                    jobs
                )

            except Exception as exc:
                print(
                    f"Error crawling "
                    f"{ats}/{slug}: "
                    f"{exc}"
                )

            completed += 1

            if completed % 100 == 0:
                print(
                    f"  Crawled "
                    f"{completed}/"
                    f"{len(tasks)} boards"
                )

    return all_jobs


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate_jobs(jobs):

    unique = {}

    for job in jobs:

        job_key = make_job_key(
            job["ats"],
            job["job_id"],
            job["job_url"],
        )

        if job_key not in unique:
            unique[job_key] = job

    return list(
        unique.values()
    )


# ============================================================
# EXISTING GOOGLE SHEET JOBS
# ============================================================

def load_existing_keys(
    worksheet,
):

    existing = set()

    try:
        rows = worksheet.get_all_values()

        if not rows:
            return existing

        headers = rows[0]

        # We expect Job ID and ATS columns.
        try:
            ats_index = headers.index(
                "ATS"
            )
        except ValueError:
            ats_index = -1

        try:
            id_index = headers.index(
                "Job ID"
            )
        except ValueError:
            id_index = -1

        try:
            url_index = headers.index(
                "Job Link"
            )
        except ValueError:
            url_index = -1

        for row in rows[1:]:

            ats = (
                row[ats_index]
                if ats_index >= 0
                and len(row) > ats_index
                else ""
            )

            job_id = (
                row[id_index]
                if id_index >= 0
                and len(row) > id_index
                else ""
            )

            url = (
                row[url_index]
                if url_index >= 0
                and len(row) > url_index
                else ""
            )

            if ats and job_id:
                existing.add(
                    f"{ats}:{job_id}"
                )

            if url:
                existing.add(
                    normalize_url(url)
                )

    except Exception as exc:
        print(
            f"Could not read existing "
            f"Jobs sheet: {exc}"
        )

    return existing


# ============================================================
# WRITE TO GOOGLE SHEETS
# ============================================================

def save_jobs(jobs):

    worksheet = get_google_sheet()

    # Desired columns.
    headers = [
        "Company",
        "Job Title",
        "Location",
        "Country",
        "Posted Date",
        "Job Link",
        "ATS",
        "Job ID",
    ]

    existing_values = worksheet.get_all_values()

    if not existing_values:

        worksheet.append_row(
            headers
        )

    elif existing_values[0] != headers:

        # If the current Jobs sheet has another header
        # structure, preserve existing data and only use
        # the current structure when empty.
        print(
            "Existing Jobs sheet has a different "
            "header structure."
        )

    existing_keys = (
        load_existing_keys(
            worksheet
        )
    )

    new_rows = []

    for job in jobs:

        id_key = (
            f"{job['ats']}:{job['job_id']}"
            if job["job_id"]
            else ""
        )

        url_key = normalize_url(
            job["job_url"]
        )

        if (
            id_key
            and id_key in existing_keys
        ):
            continue

        if (
            url_key
            and url_key in existing_keys
        ):
            continue

        new_rows.append([
            job["company"],
            job["title"],
            job["location"],
            job["country"],
            job["posted_date"],
            job["job_url"],
            job["ats"],
            job["job_id"],
        ])

        if id_key:
            existing_keys.add(
                id_key
            )

        if url_key:
            existing_keys.add(
                url_key
            )

    if not new_rows:

        print(
            "\nNo new matching jobs."
        )

        return 0

    worksheet.append_rows(
        new_rows,
        value_input_option="RAW",
    )

    print(
        f"\nAdded {len(new_rows)} "
        f"new jobs to Google Sheets."
    )

    return len(new_rows)


# ============================================================
# MAIN
# ============================================================

def main():

    started = datetime.now(
        timezone.utc
    )

    print(
        "========================================"
    )
    print(
        "REMOTE4.ME JOB CRAWLER"
    )
    print(
        "========================================"
    )

    print(
        f"Started: {started.isoformat()}"
    )

    print(
        "\nNiche:"
    )

    print(
        "  Customer Support"
    )
    print(
        "  Technical Support"
    )
    print(
        "  Customer Success"
    )

    print(
        "\nCountries:"
    )

    print(
        "  India"
    )
    print(
        "  USA"
    )

    # --------------------------------------------------------
    # STEP 1: DISCOVER BOARDS
    # --------------------------------------------------------

    print(
        "\nSTEP 1 — Discovering ATS boards"
    )

    boards = discover_all_boards()

    total_boards = sum(
        len(x)
        for x in boards.values()
    )

    print(
        f"\nDiscovered/validated "
        f"{total_boards} live boards."
    )

    for ats, slugs in boards.items():

        print(
            f"  {ats}: {len(slugs)}"
        )

    if total_boards == 0:

        print(
            "\nNo live ATS boards were discovered."
        )

        return

    # --------------------------------------------------------
    # STEP 2: CRAWL JOBS
    # --------------------------------------------------------

    print(
        "\nSTEP 2 — Crawling jobs"
    )

    jobs = crawl_boards(
        boards
    )

    print(
        f"\nMatching jobs before "
        f"deduplication: {len(jobs)}"
    )

    # --------------------------------------------------------
    # STEP 3: DEDUPLICATE
    # --------------------------------------------------------

    print(
        "\nSTEP 3 — Deduplicating"
    )

    jobs = deduplicate_jobs(
        jobs
    )

    print(
        f"Unique jobs: {len(jobs)}"
    )

    # --------------------------------------------------------
    # STEP 4: GOOGLE SHEETS
    # --------------------------------------------------------

    print(
        "\nSTEP 4 — Updating Google Sheets"
    )

    added = save_jobs(
        jobs
    )

    # --------------------------------------------------------
    # FINISHED
    # --------------------------------------------------------

    finished = datetime.now(
        timezone.utc
    )

    duration = (
        finished - started
    ).total_seconds()

    print(
        "\n========================================"
    )

    print(
        "CRAWLER FINISHED"
    )

    print(
        f"New jobs added: {added}"
    )

    print(
        f"Runtime: {duration:.1f} seconds"
    )

    print(
        "========================================"
    )


if __name__ == "__main__":
    main()
