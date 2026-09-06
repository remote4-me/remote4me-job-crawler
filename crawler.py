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
#   Automatically discover public ATS boards and collect
#   relevant REMOTE jobs for Remote4.me.
#
# Focus:
#   Customer Support
#   Technical Support
#   Customer Success
#
# Countries:
#   India
#   USA
#
# ATS:
#   Greenhouse
#   Ashby
#   Lever
#   Workable
#
# Important:
#   - SmartRecruiters has been removed.
#   - Only remote jobs are accepted.
#   - Only India/USA jobs are accepted.
#   - Jobs older than MAX_JOB_AGE_DAYS are skipped.
#   - Jobs already present in Google Sheets are skipped.
#   - Added Date is written as the first column.
#
# ============================================================


# ============================================================
# CONFIGURATION
# ============================================================

SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID",
    "",
)

GOOGLE_CREDENTIALS = os.environ.get(
    "GOOGLE_CREDENTIALS",
    "",
)

# Moderate concurrency for public ATS endpoints.
CONCURRENCY = 8

# IMPORTANT:
# Your first crawl produced ~11,000 jobs because 30 days
# was being scanned across thousands of boards.
#
# With a daily crawler, 7 days is a much more practical window.
MAX_JOB_AGE_DAYS = 7

# 0 = crawl every discovered live board.
MAX_BOARDS_PER_RUN = 0

# Refresh board discovery every run.
REFRESH_BOARDS = True

# Google Sheet tab.
JOBS_SHEET_NAME = "Jobs"

# Date format used in the Added Date column.
DATE_FORMAT = "%Y-%m-%d"

# Public HTTP user agent.
USER_AGENT = "Remote4.me Job Crawler/4.0"


# ============================================================
# ATS SOURCES
# ============================================================

ATS_SOURCES = {
    "greenhouse": {
        "archive_domains": [
            "boards.greenhouse.io",
            "job-boards.greenhouse.io",
        ],
        "api": (
            "https://boards-api.greenhouse.io/"
            "v1/boards/{slug}/jobs"
        ),
    },

    "ashby": {
        "archive_domains": [
            "jobs.ashbyhq.com",
        ],
        "api": (
            "https://api.ashbyhq.com/"
            "posting-api/job-board/{slug}"
        ),
    },

    "lever": {
        "archive_domains": [
            "jobs.lever.co",
            "jobs.eu.lever.co",
        ],
        "api": (
            "https://api.lever.co/"
            "v0/postings/{slug}?mode=json"
        ),
    },

    "workable": {
        "archive_domains": [
            "apply.workable.com",
        ],
        "api": (
            "https://www.workable.com/"
            "api/accounts/{slug}"
        ),
    },
}


# ============================================================
# TARGET JOB TITLES
# ============================================================
#
# These are intentionally title-based.
#
# We do NOT use standalone words such as:
#   support
#   success
#   manager
#   specialist
#   operations
#
# because those create too much unrelated content.
#
# ============================================================

TARGET_TITLE_PATTERNS = [

    # --------------------------------------------------------
    # CUSTOMER SUPPORT / CUSTOMER SERVICE
    # --------------------------------------------------------
    r"\bcustomer support\b",
    r"\bcustomer service\b",
    r"\bcustomer care\b",
    r"\bcustomer assistance\b",
    r"\bcustomer advocate\b",
    r"\bcustomer advocacy\b",
    r"\bcustomer experience\b",
    r"\bcustomer experience specialist\b",
    r"\bcustomer experience associate\b",
    r"\bcustomer experience representative\b",
    r"\bcustomer experience advisor\b",
    r"\bcustomer experience agent\b",
    r"\bcustomer support specialist\b",
    r"\bcustomer support associate\b",
    r"\bcustomer support representative\b",
    r"\bcustomer support agent\b",
    r"\bcustomer support advisor\b",
    r"\bcustomer support consultant\b",
    r"\bcustomer support coordinator\b",
    r"\bcustomer support analyst\b",
    r"\bcustomer service specialist\b",
    r"\bcustomer service associate\b",
    r"\bcustomer service representative\b",
    r"\bcustomer service agent\b",
    r"\bcustomer service advisor\b",
    r"\bcustomer service coordinator\b",
    r"\bcustomer care specialist\b",
    r"\bcustomer care associate\b",
    r"\bcustomer care representative\b",
    r"\bcustomer care agent\b",
    r"\bclient support\b",
    r"\bclient support specialist\b",
    r"\bclient support associate\b",
    r"\bclient support representative\b",
    r"\bclient support agent\b",
    r"\bclient services\b",
    r"\bclient service specialist\b",
    r"\bclient service representative\b",
    r"\bmember support\b",
    r"\bmember support specialist\b",
    r"\bmember support associate\b",
    r"\bmember services\b",
    r"\buser support\b",
    r"\buser support specialist\b",
    r"\buser support associate\b",
    r"\buser support representative\b",
    r"\buser services\b",
    r"\buser experience support\b",
    r"\bcommunity support\b",
    r"\bcommunity support specialist\b",
    r"\bcommunity support associate\b",
    r"\bpartner support\b",
    r"\bpartner support specialist\b",
    r"\bpartner support associate\b",
    r"\bvendor support\b",
    r"\bmerchant support\b",
    r"\bmerchant support specialist\b",
    r"\bseller support\b",
    r"\bseller support specialist\b",
    r"\bprovider support\b",
    r"\bprovider support specialist\b",
    r"\bmember experience\b",
    r"\bmember experience specialist\b",
    r"\bmember experience associate\b",
    r"\bclient experience\b",
    r"\bclient experience specialist\b",
    r"\bclient experience associate\b",
    r"\bcustomer operations\b",
    r"\bcustomer operations specialist\b",
    r"\bcustomer operations associate\b",
    r"\bcustomer operations analyst\b",
    r"\bclient operations\b",
    r"\bclient operations specialist\b",
    r"\bclient operations associate\b",
    r"\bsupport operations\b",
    r"\bsupport operations specialist\b",
    r"\bsupport operations associate\b",
    r"\bsupport operations analyst\b",
    r"\bsupport coordinator\b",
    r"\bsupport specialist\b",
    r"\bsupport associate\b",
    r"\bsupport representative\b",
    r"\bsupport agent\b",
    r"\bsupport advisor\b",
    r"\bsupport consultant\b",
    r"\bsupport analyst\b",
    r"\bsupport executive\b",
    r"\bsupport administrator\b",
    r"\bsupport administrator\b",
    r"\bsupport lead\b",
    r"\bsupport supervisor\b",
    r"\bsupport manager\b",
    r"\bcustomer support manager\b",
    r"\bcustomer service manager\b",
    r"\bcustomer care manager\b",
    r"\bclient support manager\b",
    r"\bmember support manager\b",

    # --------------------------------------------------------
    # TECHNICAL / PRODUCT / APPLICATION SUPPORT
    # --------------------------------------------------------
    r"\btechnical support\b",
    r"\btechnical support specialist\b",
    r"\btechnical support associate\b",
    r"\btechnical support representative\b",
    r"\btechnical support agent\b",
    r"\btechnical support advisor\b",
    r"\btechnical support consultant\b",
    r"\btechnical support analyst\b",
    r"\btechnical support engineer\b",
    r"\btechnical support lead\b",
    r"\btechnical support manager\b",
    r"\btechnical customer support\b",
    r"\btechnical customer service\b",
    r"\btechnical assistance\b",
    r"\btechnical helpdesk\b",
    r"\btechnical help desk\b",
    r"\bproduct support\b",
    r"\bproduct support specialist\b",
    r"\bproduct support associate\b",
    r"\bproduct support representative\b",
    r"\bproduct support agent\b",
    r"\bproduct support engineer\b",
    r"\bproduct support analyst\b",
    r"\bproduct support consultant\b",
    r"\bproduct support manager\b",
    r"\bsoftware support\b",
    r"\bsoftware support specialist\b",
    r"\bsoftware support associate\b",
    r"\bsoftware support representative\b",
    r"\bsoftware support engineer\b",
    r"\bsoftware support analyst\b",
    r"\bapplication support\b",
    r"\bapplication support specialist\b",
    r"\bapplication support associate\b",
    r"\bapplication support analyst\b",
    r"\bapplication support engineer\b",
    r"\bapplication support consultant\b",
    r"\bplatform support\b",
    r"\bplatform support specialist\b",
    r"\bplatform support associate\b",
    r"\bplatform support engineer\b",
    r"\bplatform support analyst\b",
    r"\bplatform support manager\b",
    r"\bapi support\b",
    r"\bapi support specialist\b",
    r"\bapi support engineer\b",
    r"\bdeveloper support\b",
    r"\bdeveloper support specialist\b",
    r"\bdeveloper support engineer\b",
    r"\bdeveloper support representative\b",
    r"\btechnical account support\b",
    r"\btechnical account specialist\b",
    r"\btechnical account manager\b",
    r"\btechnical account support specialist\b",
    r"\btechnical success\b",
    r"\btechnical customer success\b",
    r"\btechnical services support\b",
    r"\btechnical solutions support\b",
    r"\btechnical solutions specialist\b",
    r"\btechnical services specialist\b",
    r"\bservice desk\b",
    r"\bservice desk analyst\b",
    r"\bservice desk specialist\b",
    r"\bservice desk engineer\b",
    r"\bservice desk representative\b",
    r"\bhelp desk\b",
    r"\bhelpdesk\b",
    r"\bhelp desk specialist\b",
    r"\bhelp desk analyst\b",
    r"\bhelp desk engineer\b",
    r"\bhelp desk technician\b",
    r"\bhelpdesk specialist\b",
    r"\bit support\b",
    r"\bit support specialist\b",
    r"\bit support associate\b",
    r"\bit support engineer\b",
    r"\bit support analyst\b",
    r"\bit support technician\b",
    r"\bit service desk\b",
    r"\btechnical support technician\b",
    r"\bsupport technician\b",
    r"\bsupport engineer\b",
    r"\bsupport engineering\b",
    r"\bescalation support\b",
    r"\bsupport escalation\b",
    r"\bsupport escalation specialist\b",
    r"\bescalation specialist\b",
    r"\bescalations specialist\b",
    r"\btechnical escalation\b",
    r"\btechnical escalation specialist\b",
    r"\btechnical escalation engineer\b",

    # --------------------------------------------------------
    # CUSTOMER SUCCESS / ONBOARDING / IMPLEMENTATION
    # --------------------------------------------------------
    r"\bcustomer success\b",
    r"\bcustomer success specialist\b",
    r"\bcustomer success associate\b",
    r"\bcustomer success representative\b",
    r"\bcustomer success agent\b",
    r"\bcustomer success advisor\b",
    r"\bcustomer success consultant\b",
    r"\bcustomer success analyst\b",
    r"\bcustomer success manager\b",
    r"\bcustomer success lead\b",
    r"\bcustomer success director\b",
    r"\bcustomer success executive\b",
    r"\bcustomer success operations\b",
    r"\bcustomer success operations specialist\b",
    r"\bcustomer success operations analyst\b",
    r"\bclient success\b",
    r"\bclient success specialist\b",
    r"\bclient success associate\b",
    r"\bclient success representative\b",
    r"\bclient success advisor\b",
    r"\bclient success consultant\b",
    r"\bclient success manager\b",
    r"\bclient success lead\b",
    r"\bcustomer onboarding\b",
    r"\bcustomer onboarding specialist\b",
    r"\bcustomer onboarding associate\b",
    r"\bcustomer onboarding representative\b",
    r"\bcustomer onboarding manager\b",
    r"\bcustomer onboarding consultant\b",
    r"\bclient onboarding\b",
    r"\bclient onboarding specialist\b",
    r"\bclient onboarding associate\b",
    r"\bclient onboarding manager\b",
    r"\bclient onboarding consultant\b",
    r"\bonboarding specialist\b",
    r"\bonboarding associate\b",
    r"\bonboarding representative\b",
    r"\bonboarding advisor\b",
    r"\bonboarding consultant\b",
    r"\bonboarding manager\b",
    r"\bonboarding lead\b",
    r"\bimplementation specialist\b",
    r"\bimplementation associate\b",
    r"\bimplementation representative\b",
    r"\bimplementation consultant\b",
    r"\bimplementation manager\b",
    r"\bimplementation lead\b",
    r"\bimplementation analyst\b",
    r"\bcustomer implementation\b",
    r"\bclient implementation\b",
    r"\bcustomer enablement\b",
    r"\bcustomer enablement specialist\b",
    r"\bcustomer enablement manager\b",
    r"\bcustomer education\b",
    r"\bcustomer education specialist\b",
    r"\bcustomer education manager\b",
    r"\bcustomer advocacy\b",
    r"\bcustomer advocacy specialist\b",
    r"\bcustomer advocacy manager\b",
    r"\bcustomer adoption\b",
    r"\bcustomer adoption specialist\b",
    r"\bcustomer adoption manager\b",
    r"\bcustomer retention specialist\b",
    r"\bcustomer retention manager\b",
    r"\bclient retention specialist\b",
    r"\bclient retention manager\b",
    r"\bcustomer lifecycle specialist\b",
    r"\bcustomer lifecycle manager\b",
    r"\bcustomer engagement specialist\b",
    r"\bcustomer engagement manager\b",
    r"\bcustomer experience manager\b",
    r"\bcustomer experience lead\b",
    r"\bclient experience manager\b",
    r"\bclient experience lead\b",
]



TITLE_REGEX = re.compile(
    "|".join(TARGET_TITLE_PATTERNS),
    re.IGNORECASE,
)


# ============================================================
# ATS METADATA MATCHING
# ============================================================

RELEVANT_METADATA_PATTERNS = [
    r"\bcustomer support\b",
    r"\bcustomer service\b",
    r"\bcustomer care\b",
    r"\bcustomer experience\b",
    r"\bclient support\b",
    r"\bclient service\b",
    r"\bmember support\b",
    r"\bmember services\b",
    r"\buser support\b",
    r"\bcommunity support\b",
    r"\bpartner support\b",
    r"\bmerchant support\b",
    r"\bseller support\b",
    r"\bprovider support\b",
    r"\bsupport operations\b",
    r"\btechnical support\b",
    r"\btechnical services\b",
    r"\bproduct support\b",
    r"\bapplication support\b",
    r"\bsoftware support\b",
    r"\bplatform support\b",
    r"\bdeveloper support\b",
    r"\bapi support\b",
    r"\bhelp desk\b",
    r"\bhelpdesk\b",
    r"\bservice desk\b",
    r"\bcustomer success\b",
    r"\bclient success\b",
    r"\bcustomer onboarding\b",
    r"\bclient onboarding\b",
    r"\bonboarding\b",
    r"\bimplementation\b",
    r"\bcustomer enablement\b",
    r"\bcustomer education\b",
    r"\bcustomer advocacy\b",
    r"\bcustomer adoption\b",
]



METADATA_REGEX = re.compile(
    "|".join(RELEVANT_METADATA_PATTERNS),
    re.IGNORECASE,
)


GENERIC_ROLE_REGEX = re.compile(
    r"\b("
    r"specialist|"
    r"associate|"
    r"representative|"
    r"advisor|"
    r"agent|"
    r"analyst|"
    r"coordinator|"
    r"consultant|"
    r"executive|"
    r"engineer|"
    r"manager"
    r")\b",
    re.IGNORECASE,
)


# ============================================================
# LOCATION PATTERNS
# ============================================================

INDIA_PATTERNS = [
    r"\bindia\b",
    r"\bindian\b",
    r"\bbharat\b",
    # Major cities and common spellings
    r"\bbengaluru\b", r"\bbangalore\b", r"\bhyderabad\b",
    r"\bpune\b", r"\bmumbai\b", r"\bbombay\b",
    r"\bdelhi\b", r"\bnew delhi\b", r"\bnoida\b",
    r"\bgreater noida\b", r"\bgurgaon\b", r"\bgurugram\b",
    r"\bfaridabad\b", r"\bghaziabad\b", r"\bchennai\b",
    r"\bkolkata\b", r"\bcalcutta\b", r"\bahmedabad\b",
    r"\bsurat\b", r"\bvadodara\b", r"\brajkot\b",
    r"\bkochi\b", r"\bcochin\b", r"\bthiruvananthapuram\b",
    r"\btrivandrum\b", r"\bthrissur\b", r"\bkozhikode\b",
    r"\bcalicut\b", r"\bjaipur\b", r"\budaipur\b",
    r"\bjodhpur\b", r"\bindore\b", r"\bbhopal\b",
    r"\bnagpur\b", r"\bnashik\b", r"\baurangabad\b",
    r"\bchhatrapati sambhajinagar\b", r"\bchandigarh\b",
    r"\blucknow\b", r"\bkanpur\b", r"\bagra\b",
    r"\bprayagraj\b", r"\ballahabad\b", r"\bvaranasi\b",
    r"\bpatna\b", r"\branchi\b", r"\bbhubaneswar\b",
    r"\bcuttack\b", r"\bguwahati\b", r"\bvisakhapatnam\b",
    r"\bvijayawada\b", r"\btirupati\b", r"\bcoimbatore\b",
    r"\bmadurai\b", r"\btrichy\b", r"\btiruchirappalli\b",
    r"\bsalem\b", r"\bmysore\b", r"\bmysuru\b",
    r"\bmangalore\b", r"\bmangaluru\b", r"\bhubli\b",
    r"\bhubballi\b", r"\bamritsar\b", r"\bludhiana\b",
    r"\bdehradun\b", r"\bsrinagar\b", r"\bjammu\b",
    # States / territories
    r"\bandhra pradesh\b", r"\barunachal pradesh\b", r"\bassam\b",
    r"\bbihar\b", r"\bchhattisgarh\b", r"\bgoa\b",
    r"\bgujarat\b", r"\bharyana\b", r"\bhimachal pradesh\b",
    r"\bjharkhand\b", r"\bkarnataka\b", r"\bkerala\b",
    r"\bmadhya pradesh\b", r"\bmaharashtra\b", r"\bmanipur\b",
    r"\bmeghalaya\b", r"\bmizoram\b", r"\bnagaland\b",
    r"\bodisha\b", r"\borissa\b", r"\bpunjab\b",
    r"\brajasthan\b", r"\bsikkim\b", r"\btamil nadu\b",
    r"\btelangana\b", r"\btripura\b", r"\buttar pradesh\b",
    r"\buttarakhand\b", r"\bwest bengal\b",
    r"\bdelhi nct\b", r"\bpondicherry\b", r"\bpuducherry\b",
]



USA_PATTERNS = [
    r"\bunited states\b",
    r"\bunited states of america\b",
    r"\busa\b",
    r"\bu\.s\.a?\b",
    r"\bus-based\b",
    # All 50 states
    r"\balabama\b", r"\balaska\b", r"\barizona\b", r"\barkansas\b",
    r"\bcalifornia\b", r"\bcolorado\b", r"\bconnecticut\b", r"\bdelaware\b",
    r"\bflorida\b", r"\bgeorgia\b", r"\bhawaii\b", r"\bidaho\b",
    r"\billinois\b", r"\bindiana\b", r"\biowa\b", r"\bkansas\b",
    r"\bkentucky\b", r"\blouisiana\b", r"\bmaine\b", r"\bmaryland\b",
    r"\bmassachusetts\b", r"\bmichigan\b", r"\bminnesota\b", r"\bmississippi\b",
    r"\bmissouri\b", r"\bmontana\b", r"\bnebraska\b", r"\bnevada\b",
    r"\bnew hampshire\b", r"\bnew jersey\b", r"\bnew mexico\b", r"\bnew york\b",
    r"\bnorth carolina\b", r"\bnorth dakota\b", r"\bohio\b", r"\boklahoma\b",
    r"\boregon\b", r"\bpennsylvania\b", r"\brhode island\b", r"\bsouth carolina\b",
    r"\bsouth dakota\b", r"\btennessee\b", r"\btexas\b", r"\butah\b",
    r"\bvermont\b", r"\bvirginia\b", r"\bwashington\b", r"\bwest virginia\b",
    r"\bwisconsin\b", r"\bwyoming\b",
    # Major cities / metros
    r"\bnew york city\b", r"\bnyc\b", r"\blos angeles\b", r"\bla\b",
    r"\bsan francisco\b", r"\bsan diego\b", r"\bsan jose\b",
    r"\bseattle\b", r"\bportland\b", r"\bchicago\b", r"\bboston\b",
    r"\baustin\b", r"\bdallas\b", r"\bfort worth\b", r"\bhouston\b",
    r"\bmiami\b", r"\borlando\b", r"\btampa\b", r"\batlanta\b",
    r"\bdenver\b", r"\bphoenix\b", r"\bphiladelphia\b", r"\bcharlotte\b",
    r"\braleigh\b", r"\bdetroit\b", r"\bminneapolis\b", r"\bst louis\b",
    r"\bkansas city\b", r"\bnashville\b", r"\bnew orleans\b", r"\bcleveland\b",
    r"\bpittsburgh\b", r"\bcolumbus\b", r"\bcincinnati\b", r"\bindianapolis\b",
    r"\bmilwaukee\b", r"\bsacramento\b", r"\bsalt lake city\b",
    r"\bdenver\b", r"\bwashington dc\b", r"\bwashington d\.c\.\b",
    r"\barlington\b", r"\balexandria\b", r"\bjersey city\b",
    r"\bnewark\b", r"\bbaltimore\b", r"\bvirginia beach\b",
    r"\bmemphis\b", r"\boklahoma city\b", r"\blas vegas\b",
]



REMOTE_PATTERNS = [
    r"\bremote\b",
    r"\bfully remote\b",
    r"\b100% remote\b",
    r"\bwork remotely\b",
    r"\bwork from home\b",
    r"\bwork-from-home\b",
    r"\bhome based\b",
    r"\bhome-based\b",
    r"\bremote first\b",
    r"\bremote-first\b",
    r"\bdistributed team\b",
    r"\bdistributed\b",
    r"\btelecommute\b",
    r"\btelecommuting\b",
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

    client = gspread.authorize(
        credentials
    )

    spreadsheet = client.open_by_key(
        SPREADSHEET_ID
    )

    try:
        worksheet = spreadsheet.worksheet(
            JOBS_SHEET_NAME
        )

    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=JOBS_SHEET_NAME,
            rows=1000,
            cols=9,
        )

    return worksheet


# ============================================================
# GENERAL HELPERS
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

        if not parsed.scheme or not parsed.netloc:
            return url.rstrip("/")

        clean = (
            f"{parsed.scheme.lower()}://"
            f"{parsed.netloc.lower()}"
            f"{parsed.path.rstrip('/')}"
        )

        return clean

    except Exception:
        return url.rstrip("/")


def normalize_for_duplicate(value):
    value = clean_text(value).lower()

    value = re.sub(
        r"[^a-z0-9]+",
        " ",
        value,
    )

    return re.sub(
        r"\s+",
        " ",
        value,
    ).strip()


def make_job_key(
    ats,
    job_id,
    url,
):
    if job_id:
        raw = (
            f"{ats.lower()}:"
            f"{str(job_id).strip()}"
        )

    else:
        raw = (
            f"url:"
            f"{normalize_url(url).lower()}"
        )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()


def make_fallback_job_key(
    job,
):
    """
    Secondary duplicate protection.

    This catches the same posting if the ATS returns it
    without a stable job ID but with the same company,
    title and location.
    """

    company = normalize_for_duplicate(
        job.get("company")
    )

    title = normalize_for_duplicate(
        job.get("title")
    )

    location = normalize_for_duplicate(
        job.get("location")
    )

    raw = (
        f"{company}|"
        f"{title}|"
        f"{location}"
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
    GET JSON with limited retries for temporary
    rate limits and server errors.
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
    title = clean_text(title)

    if not title:
        return False

    return bool(
        TITLE_REGEX.search(title)
    )


def has_relevant_metadata(
    metadata,
):
    metadata = clean_text(
        metadata
    )

    if not metadata:
        return False

    return bool(
        METADATA_REGEX.search(
            metadata
        )
    )


def is_relevant_job(
    title,
    metadata="",
):
    """
    Title is the primary signal.

    Metadata can rescue a generic role title only when
    the ATS department/team/function explicitly identifies
    it as Support/Success/Onboarding/Implementation/etc.
    """

    title = clean_text(title)

    if is_target_title(title):
        return True

    if (
        GENERIC_ROLE_REGEX.search(title)
        and has_relevant_metadata(
            metadata
        )
    ):
        return True

    return False


# ============================================================
# REMOTE + COUNTRY CLASSIFICATION
# ============================================================

def classify_country(
    location,
    country_code="",
):
    location = clean_text(
        location
    )

    code = clean_text(
        country_code
    ).upper()

    # Explicit country codes first.
    if code in (
        "IN",
        "IND",
        "INDIA",
    ):
        return "India"

    if code in (
        "US",
        "USA",
        "UNITED STATES",
    ):
        return "USA"

    if INDIA_REGEX.search(
        location
    ):
        return "India"

    if USA_REGEX.search(
        location
    ):
        return "USA"

    return ""


def is_remote_job(
    location,
    remote=False,
    workplace_type="",
):
    location = clean_text(
        location
    )

    workplace_type = clean_text(
        workplace_type
    ).lower()

    if remote is True:
        return True

    if workplace_type in (
        "remote",
        "fully remote",
        "work from home",
    ):
        return True

    if REMOTE_REGEX.search(
        location
    ):
        return True

    return False


def classify_job_location(
    location,
    remote=False,
    country_code="",
    workplace_type="",
):
    """
    Returns:
        (country, is_remote)

    Only India/USA + remote are accepted.
    """

    remote_ok = is_remote_job(
        location,
        remote,
        workplace_type,
    )

    if not remote_ok:
        return "", False

    country = classify_country(
        location,
        country_code,
    )

    if country not in (
        "India",
        "USA",
    ):
        return "", False

    return country, True


# ============================================================
# DATE FILTER
# ============================================================

def is_recent(
    posted_date,
):
    if not posted_date:
        return True

    try:
        dt = datetime.strptime(
            posted_date,
            "%Y-%m-%d",
        ).date()

        cutoff = (
            datetime.now(
                timezone.utc
            ).date()
            - timedelta(
                days=MAX_JOB_AGE_DAYS
            )
        )

        return dt >= cutoff

    except Exception:
        return True


# ============================================================
# WAYBACK BOARD DISCOVERY
# ============================================================

def extract_slug_from_url(
    url,
    domain,
):
    try:
        parsed = urlparse(
            url
        )

        hostname = (
            parsed.netloc
            .lower()
            .split(":")[0]
        )

        if hostname != (
            domain.lower()
        ):
            return None

        path = parsed.path.strip(
            "/"
        )

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

        if (
            len(slug) < 2
            or len(slug) > 150
        ):
            return None

        return slug

    except Exception:
        return None


def discover_from_wayback(
    domain,
):
    """
    Discover candidate company/board slugs from
    Internet Archive CDX.
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
                original = str(
                    item
                )

            slug = extract_slug_from_url(
                original,
                domain,
            )

            if slug:
                candidates.add(
                    slug
                )

    except Exception as exc:
        print(
            f"Wayback discovery failed "
            f"for {domain}: {exc}"
        )

    return candidates


def discover_boards_for_ats(
    ats,
):
    source = ATS_SOURCES[
        ats
    ]

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
    source = ATS_SOURCES[
        ats
    ]

    url = source[
        "api"
    ].format(
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
    url = ATS_SOURCES[
        "workable"
    ]["api"].format(
        slug=slug
    )

    data = request_json(
        url,
        timeout=20,
        params={
            "details": "false",
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


def validate_board(
    ats,
    slug,
):
    if ats == "workable":
        return validate_workable_board(
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
# GREENHOUSE
# ============================================================

def fetch_greenhouse(
    slug,
):
    url = ATS_SOURCES[
        "greenhouse"
    ]["api"].format(
        slug=slug
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
            location_obj.get(
                "name"
            )
        )

        remote = bool(
            REMOTE_REGEX.search(
                location
            )
        )

        country, remote_ok = (
            classify_job_location(
                location,
                remote=remote,
            )
        )

        if not remote_ok:
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
            job.get(
                "absolute_url"
            )
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


# ============================================================
# ASHBY
# ============================================================

def fetch_ashby(
    slug,
):
    url = ATS_SOURCES[
        "ashby"
    ]["api"].format(
        slug=slug
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
                job.get(
                    "department"
                )
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
                        clean_text(
                            loc
                        )
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

        workplace_type = clean_text(
            job.get(
                "workplaceType"
            )
        )

        country, remote_ok = (
            classify_job_location(
                location,
                remote=remote,
                workplace_type=workplace_type,
            )
        )

        if not remote_ok:
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


# ============================================================
# LEVER
# ============================================================

def fetch_lever(
    slug,
):
    url = ATS_SOURCES[
        "lever"
    ]["api"].format(
        slug=slug
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
            job.get(
                "categories"
            )
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
            job.get(
                "country"
            )
        )

        workplace = clean_text(
            job.get(
                "workplaceType"
            )
        )

        remote = (
            workplace.lower()
            in (
                "remote",
                "fully remote",
            )
            or bool(
                REMOTE_REGEX.search(
                    location
                )
            )
        )

        country, remote_ok = (
            classify_job_location(
                location,
                remote=remote,
                country_code=country_code,
                workplace_type=workplace,
            )
        )

        if not remote_ok:
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
            or job.get(
                "hostedUrl"
            )
            or job.get(
                "applyUrl"
            )
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


# ============================================================
# WORKABLE
# ============================================================

def fetch_workable(
    slug,
):
    url = ATS_SOURCES[
        "workable"
    ]["api"].format(
        slug=slug
    )

    data = request_json(
        url,
        timeout=30,
        params={
            "details": "false",
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
                job.get(
                    "department"
                )
            ),
            clean_text(
                job.get(
                    "function"
                )
            ),
        ])

        if not is_relevant_job(
            title,
            metadata,
        ):
            continue

        country_code = clean_text(
            job.get(
                "country"
            )
        )

        state = clean_text(
            job.get(
                "state"
            )
        )

        city = clean_text(
            job.get(
                "city"
            )
        )

        location_parts = [
            city,
            state,
            country_code,
        ]

        location = ", ".join(
            dict.fromkeys(
                x
                for x in location_parts
                if x
            )
        )

        workplace_type = clean_text(
            job.get(
                "workplace_type"
            )
        )

        remote = bool(
            job.get(
                "telecommuting",
                False,
            )
        )

        if (
            workplace_type.lower()
            == "remote"
        ):
            remote = True

        country, remote_ok = (
            classify_job_location(
                location,
                remote=remote,
                country_code=country_code,
                workplace_type=workplace_type,
            )
        )

        if not remote_ok:
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
            job.get(
                "application_url"
            )
            or job.get(
                "url"
            )
            or job.get(
                "shortlink"
            )
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


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "ashby": fetch_ashby,
    "lever": fetch_lever,
    "workable": fetch_workable,
}


# ============================================================
# DISCOVER + VALIDATE ALL BOARDS
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
# CURRENT-RUN DEDUPLICATION
# ============================================================

def deduplicate_jobs(
    jobs,
):
    """
    Remove duplicates inside the current crawl.

    Primary key:
        ATS + Job ID

    Secondary:
        normalized URL

    Fallback:
        company + title + location
    """

    unique = {}

    url_keys = set()
    fallback_keys = set()

    for job in jobs:

        job_key = make_job_key(
            job.get("ats", ""),
            job.get("job_id", ""),
            job.get("job_url", ""),
        )

        url_key = normalize_url(
            job.get("job_url", "")
        ).lower()

        fallback_key = (
            make_fallback_job_key(
                job
            )
        )

        if job_key in unique:
            continue

        if (
            url_key
            and url_key in url_keys
        ):
            continue

        if (
            fallback_key
            and fallback_key in fallback_keys
        ):
            continue

        unique[job_key] = job

        if url_key:
            url_keys.add(
                url_key
            )

        if fallback_key:
            fallback_keys.add(
                fallback_key
            )

    return list(
        unique.values()
    )


# ============================================================
# EXISTING GOOGLE SHEET JOBS
# ============================================================

def load_existing_keys(
    worksheet,
):
    """
    Read the existing Jobs sheet and build duplicate keys.

    This is what prevents a job already added to the sheet
    from being added again on the next daily run.
    """

    existing = set()
    existing_urls = set()
    existing_fallbacks = set()

    try:
        rows = (
            worksheet.get_all_values()
        )

        if not rows:
            return (
                existing,
                existing_urls,
                existing_fallbacks,
            )

        headers = rows[0]

        def find_header(
            *names
        ):
            for name in names:
                try:
                    return headers.index(
                        name
                    )
                except ValueError:
                    continue

            return -1

        ats_index = find_header(
            "ATS"
        )

        id_index = find_header(
            "Job ID"
        )

        url_index = find_header(
            "Job Link",
            "Job URL",
        )

        company_index = find_header(
            "Company"
        )

        title_index = find_header(
            "Job Title",
            "Title",
        )

        location_index = find_header(
            "Location"
        )

        for row in rows[1:]:

            ats = (
                row[ats_index]
                if (
                    ats_index >= 0
                    and len(row)
                    > ats_index
                )
                else ""
            )

            job_id = (
                row[id_index]
                if (
                    id_index >= 0
                    and len(row)
                    > id_index
                )
                else ""
            )

            url = (
                row[url_index]
                if (
                    url_index >= 0
                    and len(row)
                    > url_index
                )
                else ""
            )

            company = (
                row[company_index]
                if (
                    company_index >= 0
                    and len(row)
                    > company_index
                )
                else ""
            )

            title = (
                row[title_index]
                if (
                    title_index >= 0
                    and len(row)
                    > title_index
                )
                else ""
            )

            location = (
                row[location_index]
                if (
                    location_index >= 0
                    and len(row)
                    > location_index
                )
                else ""
            )

            if ats and job_id:
                existing.add(
                    f"{ats.lower()}:{job_id}"
                )

            normalized_url = (
                normalize_url(
                    url
                ).lower()
            )

            if normalized_url:
                existing_urls.add(
                    normalized_url
                )

            if (
                company
                or title
                or location
            ):
                raw = (
                    f"{normalize_for_duplicate(company)}|"
                    f"{normalize_for_duplicate(title)}|"
                    f"{normalize_for_duplicate(location)}"
                )

                existing_fallbacks.add(
                    hashlib.sha256(
                        raw.encode(
                            "utf-8"
                        )
                    ).hexdigest()
                )

    except Exception as exc:
        print(
            f"Could not read existing "
            f"Jobs sheet: {exc}"
        )

    return (
        existing,
        existing_urls,
        existing_fallbacks,
    )


# ============================================================
# SHEET HEADER
# ============================================================

NEW_HEADERS = [
    "Added Date",
    "Company",
    "Job Title",
    "Location",
    "Country",
    "Posted Date",
    "Job Link",
    "ATS",
    "Job ID",
]


def ensure_sheet_headers(
    worksheet,
):
    """
    Ensure the Jobs sheet uses the new 9-column structure.

    If the existing sheet still has the old 8-column header,
    the new Added Date column is inserted at column A while
    preserving the existing job data.
    """

    values = (
        worksheet.get_all_values()
    )

    if not values:
        worksheet.append_row(
            NEW_HEADERS,
            value_input_option="RAW",
        )
        return

    current_headers = values[0]

    if current_headers == NEW_HEADERS:
        return

    old_headers = [
        "Company",
        "Job Title",
        "Location",
        "Country",
        "Posted Date",
        "Job Link",
        "ATS",
        "Job ID",
    ]

    if current_headers == old_headers:

        # Insert a physical first column.
        try:
            worksheet.insert_cols(
                ["" for _ in range(
                    len(values)
                )],
                1,
            )

        except Exception:
            # If insert_cols behaves differently with a
            # particular gspread version, rebuild the rows.
            rebuilt = []

            rebuilt.append(
                NEW_HEADERS
            )

            for row in values[1:]:
                padded = (
                    row
                    + [""] * (
                        8 - len(row)
                    )
                )

                rebuilt.append([
                    "",
                    padded[0],
                    padded[1],
                    padded[2],
                    padded[3],
                    padded[4],
                    padded[5],
                    padded[6],
                    padded[7],
                ])

            worksheet.clear()

            worksheet.update(
                "A1",
                rebuilt,
                value_input_option="RAW",
            )

            return

        # Write the new header.
        worksheet.update(
            "A1:I1",
            [NEW_HEADERS],
            value_input_option="RAW",
        )

        # Fill Added Date for old rows if empty.
        # We use today's date because those rows were
        # already imported before Added Date existed.
        if len(values) > 1:
            today = datetime.now(
                timezone.utc
            ).date().strftime(
                DATE_FORMAT
            )

            added_dates = [
                [today]
                for _ in values[1:]
            ]

            worksheet.update(
                f"A2:A{len(values)}",
                added_dates,
                value_input_option="RAW",
            )

        return

    print(
        "\nWARNING: Jobs sheet has an "
        "unexpected header structure."
    )

    print(
        "The crawler will preserve existing "
        "data and will not delete it."
    )


# ============================================================
# SAVE NEW JOBS
# ============================================================

def save_jobs(
    jobs,
):
    worksheet = get_google_sheet()

    # Ensure Added Date is the first column.
    ensure_sheet_headers(
        worksheet
    )

    (
        existing_keys,
        existing_urls,
        existing_fallbacks,
    ) = load_existing_keys(
        worksheet
    )

    new_rows = []

    added_date = datetime.now(
        timezone.utc
    ).date().strftime(
        DATE_FORMAT
    )

    for job in jobs:

        ats = str(
            job.get("ats", "")
        ).strip()

        job_id = str(
            job.get("job_id", "")
        ).strip()

        job_url = str(
            job.get("job_url", "")
        ).strip()

        id_key = (
            f"{ats.lower()}:"
            f"{job_id}"
            if (
                ats
                and job_id
            )
            else ""
        )

        url_key = (
            normalize_url(
                job_url
            ).lower()
        )

        fallback_key = (
            make_fallback_job_key(
                job
            )
        )

        # ----------------------------------------------------
        # DUPLICATE CHECK #1
        # Existing ATS + Job ID
        # ----------------------------------------------------

        if (
            id_key
            and id_key in existing_keys
        ):
            continue

        # ----------------------------------------------------
        # DUPLICATE CHECK #2
        # Existing normalized URL
        # ----------------------------------------------------

        if (
            url_key
            and url_key in existing_urls
        ):
            continue

        # ----------------------------------------------------
        # DUPLICATE CHECK #3
        # Existing company + title + location
        # ----------------------------------------------------

        if (
            fallback_key
            and fallback_key
            in existing_fallbacks
        ):
            continue

        new_rows.append([
            added_date,
            job.get("company", ""),
            job.get("title", ""),
            job.get("location", ""),
            job.get("country", ""),
            job.get("posted_date", ""),
            job.get("job_url", ""),
            job.get("ats", ""),
            job.get("job_id", ""),
        ])

        # Add immediately so another matching job
        # in this same run cannot be inserted again.
        if id_key:
            existing_keys.add(
                id_key
            )

        if url_key:
            existing_urls.add(
                url_key
            )

        if fallback_key:
            existing_fallbacks.add(
                fallback_key
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
        "\nSmartRecruiters: REMOVED"
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

    print(
        "\nRemote only: YES"
    )

    print(
        f"Job age window: "
        f"{MAX_JOB_AGE_DAYS} days"
    )

    # --------------------------------------------------------
    # STEP 1
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
    # STEP 2
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
    # STEP 3
    # --------------------------------------------------------

    print(
        "\nSTEP 3 — "
        "Removing duplicates"
    )

    jobs = deduplicate_jobs(
        jobs
    )

    print(
        f"Unique jobs in this run: "
        f"{len(jobs)}"
    )

    # --------------------------------------------------------
    # STEP 4
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
