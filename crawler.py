import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote, quote

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# REMOTE4.ME JOB CRAWLER
# ============================================================
#
# Purpose:
#   Discover public ATS boards and collect relevant remote jobs.
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
#   Workable
#   SmartRecruiters
#
# Runs automatically through GitHub Actions.
# ============================================================


# ============================================================
# CONFIGURATION
# ============================================================

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")

# Maximum concurrent HTTP requests.
# 8 is intentionally moderate for public ATS endpoints.
CONCURRENCY = 8

# Keep jobs whose ATS publication date is within this window.
# If an ATS does not expose a date in its list endpoint, the job
# is allowed because it is currently returned as an active posting.
MAX_JOB_AGE_DAYS = 30

# 0 = all discovered live boards.
MAX_BOARDS_PER_RUN = 0

# Board discovery is refreshed every run.
REFRESH_BOARDS = True

# SmartRecruiters max page size is 100.
SMARTRECRUITERS_PAGE_SIZE = 100

# User agent for public ATS endpoints.
USER_AGENT = "Remote4.me Job Crawler/2.0"


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

    "workable": {
        "archive_domains": [
            "apply.workable.com",
        ],
        "api": "https://www.workable.com/api/accounts/{slug}",
    },

    "smartrecruiters": {
        "archive_domains": [
            "careers.smartrecruiters.com",
        ],
        "api": "https://api.smartrecruiters.com/v1/companies/{slug}/postings",
    },
}


# ============================================================
# REMOTE4.ME TARGET KEYWORDS
# ============================================================

TARGET_TITLE_PATTERNS = [

    # ----------------------------
    # Customer Support
    # ----------------------------
    r"\bcustomer support\b",
    r"\bcustomer support specialist\b",
    r"\bcustomer support associate\b",
    r"\bcustomer support representative\b",
    r"\bcustomer support agent\b",
    r"\bcustomer service\b",
    r"\bcustomer service specialist\b",
    r"\bcustomer service representative\b",
    r"\bcustomer service associate\b",
    r"\bcustomer service agent\b",
    r"\bcustomer care\b",
    r"\bcustomer care specialist\b",
    r"\bcustomer care representative\b",
    r"\bcustomer experience\b",
    r"\bcustomer experience specialist\b",
    r"\bcustomer experience associate\b",
    r"\bcustomer experience representative\b",
    r"\bclient support\b",
    r"\bclient support specialist\b",
    r"\bclient support representative\b",
    r"\bclient services\b",
    r"\bclient service\b",
    r"\bmember support\b",
    r"\bmember services\b",
    r"\buser support\b",
    r"\buser services\b",
    r"\buser support specialist\b",
    r"\buser support representative\b",
    r"\bsupport specialist\b",
    r"\bsupport associate\b",
    r"\bsupport representative\b",
    r"\bsupport agent\b",
    r"\bsupport coordinator\b",
    r"\bsupport advisor\b",
    r"\bsupport consultant\b",
    r"\bsupport executive\b",
    r"\bsupport analyst\b",
    r"\bsupport operations\b",
    r"\bcustomer operations\b",
    r"\bcustomer operations specialist\b",
    r"\bcustomer operations associate\b",
    r"\bclient operations\b",
    r"\bclient operations specialist\b",

    # ----------------------------
    # Technical Support
    # ----------------------------
    r"\btechnical support\b",
    r"\btechnical support specialist\b",
    r"\btechnical support associate\b",
    r"\btechnical support representative\b",
    r"\btechnical support engineer\b",
    r"\btechnical support analyst\b",
    r"\btechnical support consultant\b",
    r"\btechnical support agent\b",
    r"\btechnical customer support\b",
    r"\btechnical customer service\b",
    r"\btechnical assistance\b",
    r"\btechnical solutions support\b",
    r"\bproduct support\b",
    r"\bproduct support specialist\b",
    r"\bproduct support engineer\b",
    r"\bproduct support associate\b",
    r"\bproduct support representative\b",
    r"\bsoftware support\b",
    r"\bsoftware support specialist\b",
    r"\bsoftware support engineer\b",
    r"\bapplication support\b",
    r"\bapplication support specialist\b",
    r"\bapplication support analyst\b",
    r"\bapplication support engineer\b",
    r"\bplatform support\b",
    r"\bplatform support specialist\b",
    r"\bplatform support engineer\b",
    r"\bit support\b",
    r"\bit support specialist\b",
    r"\bit support engineer\b",
    r"\bit support analyst\b",
    r"\bhelp desk\b",
    r"\bhelpdesk\b",
    r"\bhelp desk specialist\b",
    r"\bhelp desk analyst\b",
    r"\bservice desk\b",
    r"\bservice desk analyst\b",
    r"\bservice desk specialist\b",
    r"\btechnical account support\b",
    r"\btechnical account specialist\b",
    r"\btechnical account manager\b",
    r"\btechnical success\b",
    r"\btechnical customer success\b",

    # ----------------------------
    # Customer Success
    # ----------------------------
    r"\bcustomer success\b",
    r"\bcustomer success specialist\b",
    r"\bcustomer success associate\b",
    r"\bcustomer success manager\b",
    r"\bcustomer success executive\b",
    r"\bcustomer success representative\b",
    r"\bcustomer success consultant\b",
    r"\bcustomer success advisor\b",
    r"\bcustomer success analyst\b",
    r"\bcustomer success operations\b",
    r"\bclient success\b",
    r"\bclient success specialist\b",
    r"\bclient success associate\b",
    r"\bclient success manager\b",
    r"\bclient success consultant\b",
    r"\bcustomer onboarding\b",
    r"\bcustomer onboarding specialist\b",
    r"\bcustomer onboarding manager\b",
    r"\bcustomer onboarding associate\b",
    r"\bclient onboarding\b",
    r"\bclient onboarding specialist\b",
    r"\bclient onboarding manager\b",
    r"\bonboarding specialist\b",
    r"\bonboarding manager\b",
    r"\bonboarding associate\b",
    r"\bonboarding consultant\b",
    r"\bimplementation specialist\b",
    r"\bimplementation manager\b",
    r"\bimplementation consultant\b",
    r"\bimplementation associate\b",
    r"\bcustomer implementation\b",
    r"\bclient implementation\b",
    r"\bcustomer enablement\b",
    r"\bcustomer education\b",
    r"\bcustomer advocacy\b",
    r"\bcustomer experience manager\b",
    r"\bcustomer experience specialist\b",
    r"\bclient experience\b",
    r"\bclient experience specialist\b",
]

TITLE_REGEX = re.compile(
    "|".join(TARGET_TITLE_PATTERNS),
    re.IGNORECASE,
)


# ============================================================
# METADATA KEYWORDS
# ============================================================

SUPPORT_METADATA_PATTERNS = [
    r"\bcustomer support\b",
    r"\bcustomer service\b",
    r"\bcustomer care\b",
    r"\bcustomer experience\b",
    r"\bclient support\b",
    r"\bclient service\b",
    r"\bmember support\b",
    r"\buser support\b",
    r"\bsupport\b",
    r"\btechnical support\b",
    r"\bproduct support\b",
    r"\bapplication support\b",
    r"\bsoftware support\b",
    r"\bplatform support\b",
    r"\bhelp desk\b",
    r"\bhelpdesk\b",
    r"\bservice desk\b",
]

SUCCESS_METADATA_PATTERNS = [
    r"\bcustomer success\b",
    r"\bclient success\b",
    r"\bcustomer onboarding\b",
    r"\bclient onboarding\b",
    r"\bonboarding\b",
    r"\bimplementation\b",
    r"\bcustomer enablement\b",
    r"\bcustomer education\b",
    r"\bcustomer advocacy\b",
]

METADATA_REGEX = re.compile(
    "|".join(
        SUPPORT_METADATA_PATTERNS
        + SUCCESS_METADATA_PATTERNS
    ),
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
    r"\bchandigarh\b",
    r"\bindian\b",
]

USA_PATTERNS = [
    r"\bunited states\b",
    r"\busa\b",
    r"\bu\.s\.\b",
    r"\bunited states of america\b",
    r"\balabama\b",
    r"\balaska\b",
    r"\barizona\b",
    r"\barkansas\b",
    r"\bcalifornia\b",
    r"\bcolorado\b",
    r"\bconnecticut\b",
    r"\bdelaware\b",
    r"\bflorida\b",
    r"\bgeorgia\b",
    r"\bhawaii\b",
    r"\bidaho\b",
    r"\billinois\b",
    r"\bindiana\b",
    r"\biowa\b",
    r"\bkansas\b",
    r"\bkentucky\b",
    r"\blouisiana\b",
    r"\bmaine\b",
    r"\bmaryland\b",
    r"\bmassachusetts\b",
    r"\bmichigan\b",
    r"\bminnesota\b",
    r"\bmississippi\b",
    r"\bmissouri\b",
    r"\bmontana\b",
    r"\bnebraska\b",
    r"\bnevada\b",
    r"\bnew hampshire\b",
    r"\bnew jersey\b",
    r"\bnew mexico\b",
    r"\bnew york\b",
    r"\bnorth carolina\b",
    r"\bnorth dakota\b",
    r"\bohio\b",
    r"\boklahoma\b",
    r"\boregon\b",
    r"\bpennsylvania\b",
    r"\brhode island\b",
    r"\bsouth carolina\b",
    r"\bsouth dakota\b",
    r"\btennessee\b",
    r"\btexas\b",
    r"\butah\b",
    r"\bvermont\b",
    r"\bvirginia\b",
    r"\bwashington\b",
    r"\bwest virginia\b",
    r"\bwisconsin\b",
    r"\bwyoming\b",
    r"\baustin\b",
    r"\bseattle\b",
    r"\bboston\b",
    r"\bchicago\b",
    r"\bsan francisco\b",
    r"\blos angeles\b",
    r"\bnew york city\b",
    r"\bsan diego\b",
    r"\bdenver\b",
    r"\batlanta\b",
    r"\bdallas\b",
    r"\bhouston\b",
    r"\bmiami\b",
    r"\bphoenix\b",
    r"\bphiladelphia\b",
    r"\bportland\b",
    r"\bwashington dc\b",
    r"\bwashington d\.c\.\b",
]

REMOTE_PATTERNS = [
    r"\bremote\b",
    r"\bwork from home\b",
    r"\bdistributed\b",
    r"\bhome[- ]based\b",
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
    "User-Agent": USER_AGENT,
    "Accept": "application/json,text/plain,*/*",
})


# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_google_sheet():
    if not SPREADSHEET_ID:
        raise RuntimeError(
            "Missing SPREADSHEET_ID GitHub secret."
        )

    if not GOOGLE_CREDENTIALS:
        raise RuntimeError(
            "Missing GOOGLE_CREDENTIALS GitHub secret."
        )

    credentials_info = json.loads(
        GOOGLE_CREDENTIALS
    )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = (
        Credentials.from_service_account_info(
            credentials_info,
            scopes=scopes,
        )
    )

    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(
        SPREADSHEET_ID
    )

    try:
        worksheet = spreadsheet.worksheet(
            "Jobs"
        )
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

    value = re.sub(
        r"<[^>]+>",
        " ",
        value,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value.strip()


def normalize_url(url):
    if not url:
        return ""

    url = str(url).strip()

    try:
        parsed = urlparse(url)

        clean = (
            f"{parsed.scheme}://"
            f"{parsed.netloc}"
            f"{parsed.path}"
        )

        return clean.rstrip("/")

    except Exception:
        return url.rstrip("/")


def make_job_key(
    ats,
    job_id,
    url,
):
    if job_id:
        raw = f"{ats}:{job_id}"
    else:
        raw = (
            f"{ats}:"
            f"{normalize_url(url)}"
        )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()


def parse_date(value):
    if not value:
        return ""

    value = str(value).strip()

    try:
        dt = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        )

        return dt.date().isoformat()

    except Exception:
        pass

    try:
        return datetime.strptime(
            value[:10],
            "%Y-%m-%d",
        ).date().isoformat()

    except Exception:
        pass

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


def request_json(
    url,
    timeout=30,
    params=None,
):
    """
    GET JSON with a small retry policy for temporary
    429/5xx responses.
    """

    for attempt in range(3):

        try:
            response = SESSION.get(
                url,
                params=params,
                timeout=timeout,
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code in (
                429,
                500,
                502,
                503,
                504,
            ):
                if attempt < 2:
                    time.sleep(
                        1.5 * (attempt + 1)
                    )
                    continue

            return None

        except Exception:

            if attempt < 2:
                time.sleep(
                    1.5 * (attempt + 1)
                )
                continue

            return None

    return None


# ============================================================
# JOB MATCHING
# ============================================================

def is_target_title(title):
    if not title:
        return False

    return bool(
        TITLE_REGEX.search(
            clean_text(title)
        )
    )


def has_relevant_metadata(metadata):
    if not metadata:
        return False

    return bool(
        METADATA_REGEX.search(
            clean_text(metadata)
        )
    )


def is_relevant_job(
    title,
    metadata="",
):
    """
    Primary signal is the title.

    Metadata is accepted only when the title is a short,
    role-like word such as Specialist/Associate/Representative
    and the ATS itself classifies it as Support/Success/etc.
    """

    title = clean_text(title)
    metadata = clean_text(metadata)

    if is_target_title(title):
        return True

    generic_role = re.search(
        r"\b("
        r"specialist|associate|representative|"
        r"advisor|agent|analyst|coordinator|"
        r"consultant|executive|engineer|"
        r"manager"
        r")\b",
        title,
        re.IGNORECASE,
    )

    if generic_role and has_relevant_metadata(
        metadata
    ):
        return True

    return False


# ============================================================
# LOCATION CLASSIFICATION
# ============================================================

def classify_location(
    location,
    remote=False,
    country_code="",
):
    location = clean_text(location)

    code = clean_text(
        country_code
    ).upper()

    if code == "IN":
        return "India"

    if code == "US":
        return "USA"

    if INDIA_REGEX.search(location):
        return "India"

    if USA_REGEX.search(location):
        return "USA"

    # Worldwide/global remote jobs are intentionally rejected
    # unless India or USA is explicitly stated.
    if remote or REMOTE_REGEX.search(location):
        return ""

    return ""


# ============================================================
# DATE FILTER
# ============================================================

def is_recent(posted_date):
    if not posted_date:
        return True

    try:
        dt = datetime.strptime(
            posted_date,
            "%Y-%m-%d",
        ).date()

        cutoff = (
            datetime.now(timezone.utc).date()
            - timedelta(
                days=MAX_JOB_AGE_DAYS
            )
        )

        return dt >= cutoff

    except Exception:
        return True


# ============================================================
# ATS BOARD DISCOVERY
# ============================================================

def extract_slug_from_url(
    url,
    domain,
):
    try:
        parsed = urlparse(url)

        hostname = (
            parsed.netloc
            .lower()
            .split(":")[0]
        )

        if hostname != domain.lower():
            return None

        path = parsed.path.strip("/")

        if not path:
            return None

        slug = unquote(
            path.split("/")[0]
        ).strip()

        if not slug:
            return None

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
            "api",
            "locations",
            "departments",
        }

        if slug.lower() in blocked:
            return None

        if len(slug) < 2 or len(slug) > 150:
            return None

        return slug

    except Exception:
        return None


def discover_from_wayback(
    domain,
):
    """
    Discover candidate board slugs from Internet Archive CDX.
    """

    candidates = set()

    url = (
        "https://web.archive.org/cdx/search/cdx"
        f"?url={quote(domain + '/*', safe=':/?*')}"
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
                f"Wayback returned "
                f"{response.status_code} "
                f"for {domain}"
            )
            return candidates

        data = response.json()

        for item in data:

            if isinstance(
                item,
                list,
            ):
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
            f"Wayback discovery failed "
            f"for {domain}: {exc}"
        )

    return candidates


def discover_boards_for_ats(
    ats,
):
    source = ATS_SOURCES[ats]

    all_candidates = set()

    print(
        f"\nDiscovering {ats} boards..."
    )

    for domain in source[
        "archive_domains"
    ]:

        candidates = (
            discover_from_wayback(
                domain
            )
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

def validate_generic_board(
    ats,
    slug,
):
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

            return (
                response.status_code
                == 200
            )

    except Exception:
        pass

    return False


def validate_workable_board(
    slug,
):
    url = (
        ATS_SOURCES["workable"]["api"]
        .format(slug=slug)
    )

    data = request_json(
        url,
        timeout=20,
        params={
            "details": "false"
        },
    )

    if not isinstance(
        data,
        dict,
    ):
        return False

    return isinstance(
        data.get("jobs"),
        list,
    )


def validate_smartrecruiters_board(
    slug,
):
    url = (
        ATS_SOURCES[
            "smartrecruiters"
        ]["api"]
        .format(slug=slug)
    )

    data = request_json(
        url,
        timeout=20,
        params={
            "limit": 1,
            "offset": 0,
            "destination": "PUBLIC",
        },
    )

    if not isinstance(
        data,
        dict,
    ):
        return False

    try:
        return int(
            data.get(
                "totalFound",
                0,
            )
        ) > 0

    except Exception:
        return False


def validate_board(
    ats,
    slug,
):
    if ats == "workable":
        return validate_workable_board(
            slug
        )

    if ats == "smartrecruiters":
        return validate_smartrecruiters_board(
            slug
        )

    return validate_generic_board(
        ats,
        slug,
    )


def validate_boards(
    ats,
    candidates,
):
    valid = []

    candidates = sorted(
        set(candidates),
        key=str.lower,
    )

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

            slug = futures[
                future
            ]

            try:
                if future.result():
                    valid.append(
                        slug
                    )

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

def fetch_greenhouse(
    slug,
):
    url = (
        ATS_SOURCES[
            "greenhouse"
        ]["api"]
        .format(slug=slug)
    )

    data = request_json(
        url,
        timeout=30,
    )

    if not isinstance(
        data,
        dict,
    ):
        return []

    jobs = data.get(
        "jobs",
        [],
    )

    results = []

    for job in jobs:

        title = clean_text(
            job.get("title")
        )

        if not is_relevant_job(
            title
        ):
            continue

        location_obj = (
            job.get("location")
            or {}
        )

        location = clean_text(
            location_obj.get("name")
        )

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
            job.get(
                "first_published"
            )
            or job.get(
                "updated_at"
            )
        )

        if not is_recent(
            posted
        ):
            continue

        job_id = str(
            job.get("id")
            or ""
        )

        link = (
            job.get("absolute_url")
            or ""
        )

        if not link:
            continue

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


def fetch_ashby(
    slug,
):
    url = (
        ATS_SOURCES[
            "ashby"
        ]["api"]
        .format(slug=slug)
    )

    data = request_json(
        url,
        timeout=30,
    )

    if not isinstance(
        data,
        dict,
    ):
        return []

    jobs = data.get(
        "jobs",
        [],
    )

    results = []

    for job in jobs:

        if job.get(
            "isListed",
            True,
        ) is False:
            continue

        title = clean_text(
            job.get("title")
        )

        metadata = " ".join([
            clean_text(
                job.get("department")
            ),
            clean_text(
                job.get("team")
            ),
        ])

        if not is_relevant_job(
            title,
            metadata,
        ):
            continue

        locations = []

        primary = job.get(
            "location"
        )

        if primary:
            locations.append(
                clean_text(
                    primary
                )
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

        if not is_recent(
            posted
        ):
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

        if not link:
            continue

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


def fetch_lever(
    slug,
):
    url = (
        ATS_SOURCES[
            "lever"
        ]["api"]
        .format(slug=slug)
    )

    data = request_json(
        url,
        timeout=30,
    )

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

        categories = (
            job.get("categories")
            or {}
        )

        metadata = " ".join([
            clean_text(
                categories.get(
                    "team"
                )
            ),
            clean_text(
                categories.get(
                    "department"
                )
            ),
        ])

        if not is_relevant_job(
            title,
            metadata,
        ):
            continue

        location = clean_text(
            categories.get(
                "location"
            )
        )

        country_code = clean_text(
            job.get("country")
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
            country_code,
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

        if not is_recent(
            posted
        ):
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

        if not link:
            continue

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


def fetch_workable(
    slug,
):
    url = (
        ATS_SOURCES[
            "workable"
        ]["api"]
        .format(slug=slug)
    )

    data = request_json(
        url,
        timeout=30,
        params={
            "details": "false"
        },
    )

    if not isinstance(
        data,
        dict,
    ):
        return []

    jobs = data.get(
        "jobs",
        [],
    )

    results = []

    for job in jobs:

        title = clean_text(
            job.get("title")
        )

        metadata = " ".join([
            clean_text(
                job.get("department")
            ),
            clean_text(
                job.get("function")
            ),
        ])

        if not is_relevant_job(
            title,
            metadata,
        ):
            continue

        country_code = clean_text(
            job.get("country")
        )

        state = clean_text(
            job.get("state")
        )

        city = clean_text(
            job.get("city")
        )

        location_parts = [
            city,
            state,
            country_code,
        ]

        location = ", ".join(
            dict.fromkeys(
                x for x in location_parts
                if x
            )
        )

        workplace_type = clean_text(
            job.get(
                "workplace_type"
            )
        )

        remote = (
            bool(
                job.get(
                    "telecommuting",
                    False,
                )
            )
            or workplace_type.lower()
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
            country_code,
        )

        if country not in (
            "India",
            "USA",
        ):
            continue

        posted = parse_date(
            job.get(
                "published_on"
            )
            or job.get(
                "created_at"
            )
        )

        if not is_recent(
            posted
        ):
            continue

        job_id = str(
            job.get("shortcode")
            or job.get("code")
            or ""
        )

        link = (
            job.get("application_url")
            or job.get("url")
            or job.get("shortlink")
            or ""
        )

        if not link:
            continue

        results.append({
            "ats": "workable",
            "company": slug,
            "job_id": job_id,
            "title": title,
            "location": location,
            "country": country,
            "posted_date": posted,
            "job_url": link,
        })

    return results


def fetch_smartrecruiters(
    slug,
):
    url = (
        ATS_SOURCES[
            "smartrecruiters"
        ]["api"]
        .format(slug=slug)
    )

    results = []

    offset = 0
    total_found = None

    while True:

        data = request_json(
            url,
            timeout=30,
            params={
                "limit": SMARTRECRUITERS_PAGE_SIZE,
                "offset": offset,
                "destination": "PUBLIC",
            },
        )

        if not isinstance(
            data,
            dict,
        ):
            break

        content = data.get(
            "content",
            [],
        )

        if not isinstance(
            content,
            list,
        ):
            break

        try:
            total_found = int(
                data.get(
                    "totalFound",
                    0,
                )
            )
        except Exception:
            total_found = None

        if not content:
            break

        for job in content:

            title = clean_text(
                job.get("name")
                or job.get("title")
            )

            location_obj = (
                job.get("location")
                or {}
            )

            city = clean_text(
                location_obj.get(
                    "city"
                )
            )

            region = clean_text(
                location_obj.get(
                    "region"
                )
            )

            country_code = clean_text(
                location_obj.get(
                    "country"
                )
            )

            remote_flag = bool(
                location_obj.get(
                    "remote",
                    False,
                )
            )

            location_parts = [
                city,
                region,
                country_code.upper()
                if country_code
                else "",
            ]

            if remote_flag:
                location_parts.insert(
                    0,
                    "Remote",
                )

            location = ", ".join(
                dict.fromkeys(
                    x for x in location_parts
                    if x
                )
            )

            department_obj = (
                job.get("department")
                or {}
            )

            function_obj = (
                job.get("function")
                or {}
            )

            metadata = " ".join([
                clean_text(
                    department_obj.get(
                        "label"
                    )
                ),
                clean_text(
                    function_obj.get(
                        "label"
                    )
                ),
            ])

            if not is_relevant_job(
                title,
                metadata,
            ):
                continue

            country = classify_location(
                location,
                remote_flag,
                country_code,
            )

            if country not in (
                "India",
                "USA",
            ):
                continue

            posted = ""

            job_id = str(
                job.get("id")
                or job.get("uuid")
                or ""
            )

            ref = clean_text(
                job.get("ref")
            )

            link = (
                f"https://careers.smartrecruiters.com/"
                f"{slug}/{job_id}"
                if job_id
                else ref
            )

            if not link:
                continue

            results.append({
                "ats": "smartrecruiters",
                "company": slug,
                "job_id": job_id,
                "title": title,
                "location": location,
                "country": country,
                "posted_date": posted,
                "job_url": link,
            })

        offset += len(content)

        if (
            total_found is not None
            and offset >= total_found
        ):
            break

        if len(content) < (
            SMARTRECRUITERS_PAGE_SIZE
        ):
            break

        if offset > 100000:
            break

    return results


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "ashby": fetch_ashby,
    "lever": fetch_lever,
    "workable": fetch_workable,
    "smartrecruiters": fetch_smartrecruiters,
}


# ============================================================
# DISCOVER + VALIDATE
# ============================================================

def discover_all_boards():
    boards = {}

    for ats in ATS_SOURCES:

        candidates = (
            discover_boards_for_ats(
                ats
            )
        )

        valid = validate_boards(
            ats,
            candidates,
        )

        boards[ats] = valid

    return boards


# ============================================================
# CRAWL ALL BOARDS
# ============================================================

def crawl_boards(
    boards,
):
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

                if jobs:
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

def deduplicate_jobs(
    jobs,
):
    unique = {}

    for job in jobs:

        job_key = make_job_key(
            job["ats"],
            job["job_id"],
            job["job_url"],
        )

        if job_key not in unique:
            unique[
                job_key
            ] = job

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
        rows = (
            worksheet.get_all_values()
        )

        if not rows:
            return existing

        headers = rows[0]

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

def save_jobs(
    jobs,
):
    worksheet = get_google_sheet()

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

    existing_values = (
        worksheet.get_all_values()
    )

    if not existing_values:

        worksheet.append_row(
            headers
        )

    elif existing_values[0] != headers:

        print(
            "Existing Jobs sheet has a "
            "different header structure. "
            "Existing data will not be "
            "deleted."
        )

    existing_keys = (
        load_existing_keys(
            worksheet
        )
    )

    new_rows = []

    for job in jobs:

        id_key = (
            f"{job['ats']}:"
            f"{job['job_id']}"
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
        f"Started: "
        f"{started.isoformat()}"
    )

    print(
        "\nATS:"
    )

    for ats in ATS_SOURCES:
        print(
            f"  {ats}"
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
        "\nSTEP 1 — "
        "Discovering ATS boards"
    )

    boards = (
        discover_all_boards()
    )

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
            f"  {ats}: "
            f"{len(slugs)}"
        )

    if total_boards == 0:

        print(
            "\nNo live ATS boards "
            "were discovered."
        )

        return

    # --------------------------------------------------------
    # STEP 2: CRAWL JOBS
    # --------------------------------------------------------

    print(
        "\nSTEP 2 — "
        "Crawling jobs"
    )

    jobs = crawl_boards(
        boards
    )

    print(
        f"\nMatching jobs before "
        f"deduplication: "
        f"{len(jobs)}"
    )

    # --------------------------------------------------------
    # STEP 3: DEDUPLICATE
    # --------------------------------------------------------

    print(
        "\nSTEP 3 — "
        "Deduplicating"
    )

    jobs = deduplicate_jobs(
        jobs
    )

    print(
        f"Unique jobs: "
        f"{len(jobs)}"
    )

    # --------------------------------------------------------
    # STEP 4: GOOGLE SHEETS
    # --------------------------------------------------------

    print(
        "\nSTEP 4 — "
        "Updating Google Sheets"
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
        f"New jobs added: "
        f"{added}"
    )

    print(
        f"Runtime: "
        f"{duration:.1f} seconds"
    )

    print(
        "========================================"
    )


if __name__ == "__main__":
    main()
