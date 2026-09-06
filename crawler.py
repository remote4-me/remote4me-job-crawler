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
# REMOTE4.ME JOB CRAWLER — QUALITY / DAILY VERSION
# ============================================================
#
# Scope:
#   - Customer Support
#   - Technical Support
#   - Customer Success
#   - Customer Experience
#   - Customer Service
#   - Onboarding / Implementation / Enablement
#   - Closely related support roles
#
# Locations:
#   - USA: REMOTE only
#   - India: REMOTE + HYBRID
#
# ATS:
#   - Greenhouse
#   - Ashby
#   - Lever
#   - Workable
#
# Design goals:
#   - High relevance, low junk
#   - Direct job URL required
#   - Strong duplicate protection
#   - Persistent ATS board cache in Google Sheets
#   - Internet Archive/Common Crawl discovery runs every day
#   - Newly discovered boards are validated directly against
#     the ATS before being added to Boards
#   - Every active board is scanned directly through its ATS API
#     every day for fresh jobs
#   - Daily jobs are limited to a recent publication window
#
# The GitHub Actions workflow should run this file at:
#   19:00 IST = 13:30 UTC
# ============================================================


# ============================================================
# CONFIGURATION
# ============================================================

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")

# Higher concurrency reduces the ~20 minute runtime seen with
# thousands of boards. 16 is a compromise between speed and
# public-API friendliness.
CONCURRENCY = 16

# Daily crawl window.
# Because the crawler runs every day and deduplicates against the
# Sheet, 7 days gives a useful safety buffer without repeatedly
# importing months of old jobs.
MAX_JOB_AGE_DAYS = 7

# Board discovery refresh interval for re-validating existing boards.
# Internet Archive discovery itself runs EVERY day.
# The Boards sheet persists between GitHub Actions runs.
BOARD_CACHE_DAYS = 7

# 0 = crawl every cached live board.
MAX_BOARDS_PER_RUN = 0

# If a cached board has failed repeatedly, it can be removed on
# the next discovery refresh. This avoids carrying dead boards.
MAX_BOARD_FAILURES = 3

USER_AGENT = "Remote4.me Job Crawler/3.1"

SMART_RETRY_STATUS_CODES = {
    408,
    425,
    429,
    500,
    502,
    503,
    504,
}


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
        "eu_api": (
            "https://api.eu.lever.co/"
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


# Runtime metadata discovered while validating/scanning boards.
# These dictionaries let the Boards sheet preserve the exact ATS-hosted
# domain when a company uses a regional ATS endpoint (for example Lever EU).
DISCOVERED_BOARD_URLS = {}
DISCOVERED_COMPANY_NAMES = {}


# ============================================================
# HIGH-COVERAGE, TITLE-FOCUSED ROLE KEYWORDS
# ============================================================
#
# Do NOT add standalone words such as:
#   support
#   success
#   service
#   specialist
#   manager
#   operations
#
# by themselves. They create large amounts of junk.
#
# The patterns below deliberately use meaningful combinations.
# ============================================================

TARGET_TITLE_PATTERNS = [

    # --------------------------------------------------------
    # CUSTOMER SUPPORT
    # --------------------------------------------------------
    r"\bcustomer support\b",
    r"\bcustomer support specialist\b",
    r"\bcustomer support associate\b",
    r"\bcustomer support representative\b",
    r"\bcustomer support agent\b",
    r"\bcustomer support advisor\b",
    r"\bcustomer support consultant\b",
    r"\bcustomer support coordinator\b",
    r"\bcustomer support analyst\b",
    r"\bcustomer support executive\b",
    r"\bcustomer support manager\b",
    r"\bcustomer support lead\b",
    r"\bcustomer support supervisor\b",
    r"\bcustomer support director\b",
    r"\bhead of customer support\b",
    r"\bglobal customer support\b",
    r"\binternational customer support\b",
    r"\bcustomer technical support\b",

    # --------------------------------------------------------
    # CUSTOMER SERVICE
    # --------------------------------------------------------
    r"\bcustomer service\b",
    r"\bcustomer service specialist\b",
    r"\bcustomer service associate\b",
    r"\bcustomer service representative\b",
    r"\bcustomer service agent\b",
    r"\bcustomer service advisor\b",
    r"\bcustomer service consultant\b",
    r"\bcustomer service coordinator\b",
    r"\bcustomer service analyst\b",
    r"\bcustomer service executive\b",
    r"\bcustomer service manager\b",
    r"\bcustomer service lead\b",
    r"\bcustomer service supervisor\b",
    r"\bcustomer service director\b",
    r"\bhead of customer service\b",

    # --------------------------------------------------------
    # CUSTOMER CARE
    # --------------------------------------------------------
    r"\bcustomer care\b",
    r"\bcustomer care specialist\b",
    r"\bcustomer care associate\b",
    r"\bcustomer care representative\b",
    r"\bcustomer care agent\b",
    r"\bcustomer care advisor\b",
    r"\bcustomer care manager\b",
    r"\bcustomer care lead\b",

    # --------------------------------------------------------
    # CUSTOMER EXPERIENCE
    # --------------------------------------------------------
    r"\bcustomer experience\b",
    r"\bcustomer experience specialist\b",
    r"\bcustomer experience associate\b",
    r"\bcustomer experience representative\b",
    r"\bcustomer experience advisor\b",
    r"\bcustomer experience manager\b",
    r"\bcustomer experience lead\b",
    r"\bcustomer experience analyst\b",
    r"\bcustomer experience operations\b",
    r"\bhead of customer experience\b",
    r"\bcustomer experience director\b",

    # --------------------------------------------------------
    # CLIENT / USER / MEMBER SUPPORT
    # --------------------------------------------------------
    r"\bclient support\b",
    r"\bclient support specialist\b",
    r"\bclient support associate\b",
    r"\bclient support representative\b",
    r"\bclient support agent\b",
    r"\bclient support manager\b",
    r"\bclient service\b",
    r"\bclient services\b",
    r"\bclient services specialist\b",
    r"\bclient services associate\b",
    r"\bclient services representative\b",
    r"\bclient services manager\b",
    r"\buser support\b",
    r"\buser support specialist\b",
    r"\buser support associate\b",
    r"\buser support representative\b",
    r"\buser support agent\b",
    r"\buser support manager\b",
    r"\buser services\b",
    r"\buser services specialist\b",
    r"\bmember support\b",
    r"\bmember support specialist\b",
    r"\bmember support associate\b",
    r"\bmember services\b",
    r"\bmember services specialist\b",

    # --------------------------------------------------------
    # COMMUNITY / PARTNER / MERCHANT SUPPORT
    # --------------------------------------------------------
    r"\bcommunity support\b",
    r"\bcommunity support specialist\b",
    r"\bcommunity support manager\b",
    r"\bcommunity operations\b",
    r"\bpartner support\b",
    r"\bpartner support specialist\b",
    r"\bpartner support manager\b",
    r"\bpartner services\b",
    r"\bmerchant support\b",
    r"\bmerchant support specialist\b",
    r"\bmerchant support representative\b",
    r"\bmerchant success\b",
    r"\bseller support\b",
    r"\bseller support specialist\b",
    r"\bvendor support\b",
    r"\bvendor support specialist\b",
    r"\bprovider support\b",
    r"\bprovider support specialist\b",

    # --------------------------------------------------------
    # GENERAL SUPPORT
    # --------------------------------------------------------
    r"\bsupport specialist\b",
    r"\bsupport associate\b",
    r"\bsupport representative\b",
    r"\bsupport agent\b",
    r"\bsupport advisor\b",
    r"\bsupport consultant\b",
    r"\bsupport coordinator\b",
    r"\bsupport analyst\b",
    r"\bsupport executive\b",
    r"\bsupport administrator\b",
    r"\bsupport lead\b",
    r"\bsupport manager\b",
    r"\bsupport supervisor\b",
    r"\bsupport operations\b",
    r"\bsupport operations specialist\b",
    r"\bsupport operations associate\b",
    r"\bsupport operations analyst\b",
    r"\bsupport operations manager\b",
    r"\bsupport program manager\b",
    r"\bsupport enablement\b",
    r"\bsupport quality\b",
    r"\bsupport quality analyst\b",
    r"\bsupport quality specialist\b",
    r"\bsupport trainer\b",
    r"\bsupport training specialist\b",
    r"\bsupport workforce\b",
    r"\bsupport workforce analyst\b",
    r"\bsupport workforce manager\b",
    r"\bescalation support\b",
    r"\bescalations specialist\b",
    r"\bescalations manager\b",
    r"\bescalation specialist\b",
    r"\bescalation manager\b",
    r"\btechnical escalation\b",
    r"\bincident support\b",

    # --------------------------------------------------------
    # TECHNICAL SUPPORT
    # --------------------------------------------------------
    r"\btechnical support\b",
    r"\btechnical support specialist\b",
    r"\btechnical support associate\b",
    r"\btechnical support representative\b",
    r"\btechnical support agent\b",
    r"\btechnical support advisor\b",
    r"\btechnical support consultant\b",
    r"\btechnical support coordinator\b",
    r"\btechnical support analyst\b",
    r"\btechnical support engineer\b",
    r"\btechnical support lead\b",
    r"\btechnical support manager\b",
    r"\btechnical support supervisor\b",
    r"\btechnical support director\b",
    r"\bhead of technical support\b",
    r"\btechnical customer support\b",
    r"\btechnical customer service\b",
    r"\btechnical assistance specialist\b",
    r"\btechnical assistance\b",
    r"\btechnical helpdesk\b",
    r"\btechnical help desk\b",
    r"\btechnical service desk\b",

    # --------------------------------------------------------
    # PRODUCT SUPPORT
    # --------------------------------------------------------
    r"\bproduct support\b",
    r"\bproduct support specialist\b",
    r"\bproduct support associate\b",
    r"\bproduct support representative\b",
    r"\bproduct support agent\b",
    r"\bproduct support advisor\b",
    r"\bproduct support analyst\b",
    r"\bproduct support engineer\b",
    r"\bproduct support consultant\b",
    r"\bproduct support manager\b",
    r"\bproduct support lead\b",
    r"\bproduct support operations\b",

    # --------------------------------------------------------
    # SOFTWARE / APPLICATION / PLATFORM SUPPORT
    # --------------------------------------------------------
    r"\bsoftware support\b",
    r"\bsoftware support specialist\b",
    r"\bsoftware support associate\b",
    r"\bsoftware support representative\b",
    r"\bsoftware support engineer\b",
    r"\bsoftware support analyst\b",
    r"\bsoftware support manager\b",
    r"\bapplication support\b",
    r"\bapplication support specialist\b",
    r"\bapplication support associate\b",
    r"\bapplication support analyst\b",
    r"\bapplication support engineer\b",
    r"\bapplication support manager\b",
    r"\bplatform support\b",
    r"\bplatform support specialist\b",
    r"\bplatform support associate\b",
    r"\bplatform support engineer\b",
    r"\bplatform support analyst\b",
    r"\bplatform support manager\b",
    r"\bapi support\b",
    r"\bapi support specialist\b",
    r"\bapi support engineer\b",
    r"\bintegration support\b",
    r"\bintegration support specialist\b",
    r"\bintegration support engineer\b",
    r"\bdeveloper support\b",
    r"\bdeveloper support specialist\b",
    r"\bdeveloper support engineer\b",
    r"\bdeveloper experience support\b",

    # --------------------------------------------------------
    # IT / HELP DESK / SERVICE DESK
    # --------------------------------------------------------
    r"\bit support\b",
    r"\bit support specialist\b",
    r"\bit support associate\b",
    r"\bit support representative\b",
    r"\bit support engineer\b",
    r"\bit support analyst\b",
    r"\bit support technician\b",
    r"\bit support manager\b",
    r"\bit help desk\b",
    r"\bhelp desk\b",
    r"\bhelpdesk\b",
    r"\bhelp desk specialist\b",
    r"\bhelp desk analyst\b",
    r"\bhelp desk technician\b",
    r"\bhelp desk engineer\b",
    r"\bhelp desk manager\b",
    r"\bservice desk\b",
    r"\bservice desk specialist\b",
    r"\bservice desk analyst\b",
    r"\bservice desk technician\b",
    r"\bservice desk engineer\b",
    r"\bservice desk manager\b",
    r"\bit service desk\b",

    # --------------------------------------------------------
    # TECHNICAL ACCOUNT / CUSTOMER TECHNICAL SUCCESS
    # --------------------------------------------------------
    r"\btechnical account support\b",
    r"\btechnical account specialist\b",
    r"\btechnical account associate\b",
    r"\btechnical account manager\b",
    r"\btechnical account executive\b",
    r"\btechnical success\b",
    r"\btechnical success specialist\b",
    r"\btechnical success manager\b",
    r"\btechnical customer success\b",
    r"\bcustomer technical success\b",
    r"\btechnical client success\b",

    # --------------------------------------------------------
    # CUSTOMER SUCCESS
    # --------------------------------------------------------
    r"\bcustomer success\b",
    r"\bcustomer success specialist\b",
    r"\bcustomer success associate\b",
    r"\bcustomer success representative\b",
    r"\bcustomer success advisor\b",
    r"\bcustomer success consultant\b",
    r"\bcustomer success analyst\b",
    r"\bcustomer success manager\b",
    r"\bcustomer success lead\b",
    r"\bcustomer success director\b",
    r"\bhead of customer success\b",
    r"\bvp customer success\b",
    r"\bvice president customer success\b",
    r"\bcustomer success operations\b",
    r"\bcustomer success operations specialist\b",
    r"\bcustomer success operations manager\b",
    r"\bcustomer success enablement\b",
    r"\bcustomer success strategy\b",
    r"\bclient success\b",
    r"\bclient success specialist\b",
    r"\bclient success associate\b",
    r"\bclient success representative\b",
    r"\bclient success advisor\b",
    r"\bclient success consultant\b",
    r"\bclient success analyst\b",
    r"\bclient success manager\b",
    r"\bclient success lead\b",
    r"\bclient success director\b",
    r"\bhead of client success\b",

    # --------------------------------------------------------
    # ONBOARDING
    # --------------------------------------------------------
    r"\bcustomer onboarding\b",
    r"\bcustomer onboarding specialist\b",
    r"\bcustomer onboarding associate\b",
    r"\bcustomer onboarding representative\b",
    r"\bcustomer onboarding manager\b",
    r"\bcustomer onboarding lead\b",
    r"\bclient onboarding\b",
    r"\bclient onboarding specialist\b",
    r"\bclient onboarding associate\b",
    r"\bclient onboarding manager\b",
    r"\bonboarding specialist\b",
    r"\bonboarding associate\b",
    r"\bonboarding representative\b",
    r"\bonboarding advisor\b",
    r"\bonboarding consultant\b",
    r"\bonboarding manager\b",
    r"\bonboarding lead\b",
    r"\bonboarding operations\b",

    # --------------------------------------------------------
    # IMPLEMENTATION
    # --------------------------------------------------------
    r"\bcustomer implementation\b",
    r"\bcustomer implementation specialist\b",
    r"\bcustomer implementation manager\b",
    r"\bclient implementation\b",
    r"\bclient implementation specialist\b",
    r"\bclient implementation manager\b",
    r"\bimplementation specialist\b",
    r"\bimplementation associate\b",
    r"\bimplementation consultant\b",
    r"\bimplementation analyst\b",
    r"\bimplementation manager\b",
    r"\bimplementation lead\b",
    r"\bimplementation director\b",
    r"\bimplementation operations\b",
    r"\bsolutions implementation\b",
    r"\bsolution implementation specialist\b",

    # --------------------------------------------------------
    # ENABLEMENT / EDUCATION / ADOPTION / RETENTION
    # --------------------------------------------------------
    r"\bcustomer enablement\b",
    r"\bcustomer enablement specialist\b",
    r"\bcustomer enablement manager\b",
    r"\bclient enablement\b",
    r"\bclient enablement specialist\b",
    r"\bcustomer education\b",
    r"\bcustomer education specialist\b",
    r"\bcustomer education manager\b",
    r"\bclient education\b",
    r"\bclient education specialist\b",
    r"\bcustomer adoption\b",
    r"\bcustomer adoption specialist\b",
    r"\bcustomer adoption manager\b",
    r"\bcustomer retention\b",
    r"\bcustomer retention specialist\b",
    r"\bcustomer retention manager\b",
    r"\bcustomer lifecycle\b",
    r"\bcustomer lifecycle specialist\b",
    r"\bcustomer lifecycle manager\b",
    r"\bcustomer engagement\b",
    r"\bcustomer engagement specialist\b",
    r"\bcustomer engagement manager\b",
    r"\bcustomer advocacy\b",
    r"\bcustomer advocacy specialist\b",
    r"\bcustomer advocacy manager\b",

    # --------------------------------------------------------
    # CUSTOMER / CLIENT OPERATIONS & CX SHORT-FORM TITLES
    # --------------------------------------------------------
    r"\bcustomer operations\b",
    r"\bcustomer operations specialist\b",
    r"\bcustomer operations associate\b",
    r"\bcustomer operations representative\b",
    r"\bcustomer operations analyst\b",
    r"\bcustomer operations manager\b",
    r"\bclient operations\b",
    r"\bclient operations specialist\b",
    r"\bclient operations associate\b",
    r"\bclient operations analyst\b",
    r"\bclient operations manager\b",
    r"\buser operations\b",
    r"\buser operations specialist\b",
    r"\buser operations associate\b",
    r"\buser operations manager\b",
    r"\bcx specialist\b",
    r"\bcx associate\b",
    r"\bcx representative\b",
    r"\bcx advisor\b",
    r"\bcx manager\b",
    r"\bcx lead\b",
    r"\bcustomer relations specialist\b",
    r"\bcustomer relations representative\b",
    r"\bclient relations specialist\b",
    r"\bclient relations representative\b",
    r"\btrust and safety specialist\b",
    r"\btrust & safety specialist\b",
    r"\btrust and safety operations\b",
    r"\btrust & safety operations\b",
]


TITLE_REGEX = re.compile(
    "|".join(TARGET_TITLE_PATTERNS),
    re.IGNORECASE,
)


# ============================================================
# JUNK / FALSE-POSITIVE TITLE FILTER
# ============================================================

EXCLUDED_TITLE_PATTERNS = [
    r"\bsales support\b",
    r"\bsales development\b",
    r"\bsales operations\b",
    r"\bmarketing support\b",
    r"\bmarketing operations\b",
    r"\brecruiting support\b",
    r"\brecruitment support\b",
    r"\bexecutive assistant\b",
    r"\badministrative assistant\b",
    r"\bpersonal assistant\b",
    r"\btechnical recruiter\b",
    r"\bsupporting actor\b",
]

EXCLUDED_TITLE_REGEX = re.compile(
    "|".join(EXCLUDED_TITLE_PATTERNS),
    re.IGNORECASE,
)


# ============================================================
# LOCATION DATA
# ============================================================

INDIA_STATES = [
    "andhra pradesh",
    "arunachal pradesh",
    "assam",
    "bihar",
    "chhattisgarh",
    "goa",
    "gujarat",
    "haryana",
    "himachal pradesh",
    "jharkhand",
    "karnataka",
    "kerala",
    "madhya pradesh",
    "maharashtra",
    "manipur",
    "meghalaya",
    "mizoram",
    "nagaland",
    "odisha",
    "orissa",
    "punjab",
    "rajasthan",
    "sikkim",
    "tamil nadu",
    "telangana",
    "tripura",
    "uttar pradesh",
    "uttarakhand",
    "west bengal",
    "andaman and nicobar",
    "chandigarh",
    "dadra and nagar haveli",
    "daman and diu",
    "delhi",
    "jammu and kashmir",
    "ladakh",
    "lakshadweep",
    "puducherry",
]

INDIA_CITIES = [
    "ahmedabad",
    "amritsar",
    "aurangabad",
    "bengaluru",
    "bangalore",
    "bhilai",
    "bhubaneswar",
    "bikaner",
    "bokaro",
    "chandigarh",
    "chennai",
    "coimbatore",
    "cochin",
    "kochi",
    "cuttack",
    "dehradun",
    "delhi",
    "new delhi",
    "faridabad",
    "gandhinagar",
    "ghaziabad",
    "goa",
    "gurgaon",
    "gurugram",
    "guwahati",
    "gwalior",
    "howrah",
    "hubli",
    "hyderabad",
    "indore",
    "jaipur",
    "jalandhar",
    "jammu",
    "jamshedpur",
    "kanpur",
    "kochi",
    "kolkata",
    "kota",
    "lucknow",
    "ludhiana",
    "madurai",
    "mangalore",
    "mangaluru",
    "meerut",
    "mohali",
    "mumbai",
    "bombay",
    "mysore",
    "mysuru",
    "nagpur",
    "nashik",
    "navi mumbai",
    "noida",
    "patna",
    "pondicherry",
    "puducherry",
    "prayagraj",
    "allahabad",
    "pune",
    "raipur",
    "rajkot",
    "ranchi",
    "salem",
    "surat",
    "thane",
    "thiruvananthapuram",
    "trivandrum",
    "udaipur",
    "vadodara",
    "baroda",
    "varanasi",
    "vijayawada",
    "visakhapatnam",
    "vizag",
    "warangal",
]

US_STATES = [
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "district of columbia",
]

US_CITIES = [
    "new york",
    "new york city",
    "nyc",
    "los angeles",
    "san francisco",
    "san diego",
    "san jose",
    "sacramento",
    "oakland",
    "seattle",
    "portland",
    "las vegas",
    "phoenix",
    "tucson",
    "denver",
    "austin",
    "dallas",
    "fort worth",
    "houston",
    "san antonio",
    "chicago",
    "boston",
    "cambridge",
    "atlanta",
    "miami",
    "orlando",
    "tampa",
    "charlotte",
    "raleigh",
    "durham",
    "washington dc",
    "washington d.c.",
    "washington",
    "philadelphia",
    "pittsburgh",
    "detroit",
    "minneapolis",
    "st paul",
    "st. paul",
    "nashville",
    "memphis",
    "salt lake city",
    "boise",
    "columbus",
    "cleveland",
    "cincinnati",
    "indianapolis",
    "kansas city",
    "st louis",
    "st. louis",
    "omaha",
    "oklahoma city",
    "new orleans",
    "baltimore",
    "richmond",
    "norfolk",
    "virginia beach",
    "hartford",
    "providence",
    "buffalo",
    "rochester",
    "albany",
    "jersey city",
    "newark",
    "princeton",
    "honolulu",
    "anchorage",
    "albuquerque",
    "el paso",
    "bozeman",
    "madison",
    "milwaukee",
    "boulder",
    "colorado springs",
    "spokane",
    "tacoma",
    "irvine",
    "anaheim",
    "pasadena",
    "fremont",
    "berkeley",
    "santa clara",
    "mountain view",
    "palo alto",
    "redwood city",
    "menlo park",
    "cupertino",
]

INDIA_COUNTRY_PATTERNS = [
    r"\bindia\b",
    r"\bind\b",
    r"\bindian\b",
]

USA_COUNTRY_PATTERNS = [
    r"\bunited states\b",
    r"\bunited states of america\b",
    r"\busa\b",
    r"\bu\.s\.a\.\b",
    r"\bu\.s\.\b",
    r"\bunited states remote\b",
]

REMOTE_PATTERNS = [
    r"\bremote\b",
    r"\bfully remote\b",
    r"\b100% remote\b",
    r"\bwork from home\b",
    r"\bwork-from-home\b",
    r"\bhome based\b",
    r"\bhome-based\b",
    r"\bdistributed\b",
]

HYBRID_PATTERNS = [
    r"\bhybrid\b",
    r"\bpartially remote\b",
    r"\bremote hybrid\b",
]

INDIA_REGEX = re.compile(
    "|".join(
        [r"\b" + re.escape(x) + r"\b"
         for x in INDIA_STATES + INDIA_CITIES]
        + INDIA_COUNTRY_PATTERNS
    ),
    re.IGNORECASE,
)

USA_REGEX = re.compile(
    "|".join(
        [r"\b" + re.escape(x) + r"\b"
         for x in US_STATES + US_CITIES]
        + USA_COUNTRY_PATTERNS
    ),
    re.IGNORECASE,
)

REMOTE_REGEX = re.compile(
    "|".join(REMOTE_PATTERNS),
    re.IGNORECASE,
)

HYBRID_REGEX = re.compile(
    "|".join(HYBRID_PATTERNS),
    re.IGNORECASE,
)


# ============================================================
# HTTP
# ============================================================

SESSION = requests.Session()

SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/json,text/plain,*/*",
})


def request_json(
    url,
    timeout=30,
    params=None,
    max_attempts=3,
):
    for attempt in range(max_attempts):

        try:
            response = SESSION.get(
                url,
                params=params,
                timeout=timeout,
            )

            if response.status_code == 200:
                return response.json()

            if (
                response.status_code
                in SMART_RETRY_STATUS_CODES
                and attempt < max_attempts - 1
            ):
                time.sleep(
                    min(
                        4,
                        1.0 * (attempt + 1),
                    )
                )
                continue

            return None

        except (
            requests.RequestException,
            ValueError,
        ):

            if attempt < max_attempts - 1:
                time.sleep(
                    1.0 * (attempt + 1)
                )
                continue

            return None

    return None


# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_spreadsheet():
    if not SPREADSHEET_ID:
        raise RuntimeError(
            "Missing SPREADSHEET_ID GitHub secret."
        )

    if not GOOGLE_CREDENTIALS:
        raise RuntimeError(
            "Missing GOOGLE_CREDENTIALS GitHub secret."
        )

    info = json.loads(
        GOOGLE_CREDENTIALS
    )

    credentials = (
        Credentials.from_service_account_info(
            info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
    )

    client = gspread.authorize(
        credentials
    )

    return client.open_by_key(
        SPREADSHEET_ID
    )


def get_or_create_worksheet(
    spreadsheet,
    title,
    rows=1000,
    cols=12,
):
    try:
        return spreadsheet.worksheet(
            title
        )
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=title,
            rows=rows,
            cols=cols,
        )


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

    try:
        parsed = urlparse(
            str(url).strip()
        )

        return (
            f"{parsed.scheme.lower()}://"
            f"{parsed.netloc.lower()}"
            f"{parsed.path}"
        ).rstrip("/")

    except Exception:
        return str(url).strip().rstrip("/")


def parse_date(value):
    if not value:
        return ""

    value = str(value).strip()

    try:
        return datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        ).date().isoformat()

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
            return datetime.fromtimestamp(
                number / 1000,
                tz=timezone.utc,
            ).date().isoformat()

    except Exception:
        pass

    return ""


def make_job_key(
    ats,
    job_id,
    url,
):
    if job_id:
        raw = (
            f"{ats.lower()}:"
            f"{str(job_id).strip().lower()}"
        )
    else:
        raw = (
            f"{ats.lower()}:"
            f"{normalize_url(url)}"
        )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()


def title_is_relevant(
    title,
):
    title = clean_text(title)

    if not title:
        return False

    if EXCLUDED_TITLE_REGEX.search(
        title
    ):
        return False

    return bool(
        TITLE_REGEX.search(
            title
        )
    )


def has_remote_word(
    text,
):
    return bool(
        REMOTE_REGEX.search(
            clean_text(text)
        )
    )


def has_hybrid_word(
    text,
):
    return bool(
        HYBRID_REGEX.search(
            clean_text(text)
        )
    )


def classify_country(
    location="",
    country_code="",
    address_country="",
    description="",
):
    """
    Country classification is deliberately conservative.

    Priority:
      1. Explicit country code
      2. Explicit address country
      3. Location/city/state
      4. Description only when needed
    """

    code = clean_text(
        country_code
    ).upper()

    addr = clean_text(
        address_country
    ).lower()

    location = clean_text(
        location
    )

    description = clean_text(
        description
    )

    if code in {
        "IN",
        "IND",
        "INDIA",
    }:
        return "India"

    if code in {
        "US",
        "USA",
        "UNITED STATES",
    }:
        return "USA"

    if addr in {
        "in",
        "ind",
        "india",
    }:
        return "India"

    if addr in {
        "us",
        "usa",
        "united states",
        "united states of america",
    }:
        return "USA"

    if INDIA_REGEX.search(location):
        return "India"

    if USA_REGEX.search(location):
        return "USA"

    # Only use description for ambiguous remote/hybrid jobs.
    # This helps ATS entries such as "Remote" where the actual
    # country eligibility is mentioned in the text.
    if description:

        first_part = (
            description[:12000]
        )

        if INDIA_REGEX.search(
            first_part
        ):
            return "India"

        if USA_REGEX.search(
            first_part
        ):
            return "USA"

    return ""


def classify_workplace(
    location="",
    workplace_type="",
    remote_flag=False,
    description="",
):
    """
    Returns:
      remote
      hybrid
      onsite
      unknown
    """

    workplace = clean_text(
        workplace_type
    ).lower()

    if workplace in {
        "remote",
        "fully remote",
    }:
        return "remote"

    if workplace in {
        "hybrid",
    }:
        return "hybrid"

    if remote_flag:
        return "remote"

    if has_remote_word(location):
        return "remote"

    if has_hybrid_word(location):
        return "hybrid"

    if description:

        # Prefer an explicit workplace phrase near the start
        # of the description, while avoiding broad text matches.
        head = description[:5000]

        if has_hybrid_word(head):
            return "hybrid"

        if has_remote_word(head):
            return "remote"

    return "unknown"


def location_is_allowed(
    country,
    workplace,
):
    # USA: remote only.
    if country == "USA":
        return workplace == "remote"

    # India: remote OR hybrid.
    if country == "India":
        return workplace in {
            "remote",
            "hybrid",
        }

    return False


def is_recent(
    posted_date,
):
    if not posted_date:
        return True

    try:
        posted = datetime.strptime(
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

        return posted >= cutoff

    except Exception:
        return True


# ============================================================
# BOARD DISCOVERY
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

        if hostname != domain.lower():
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
            "companies",
            "account",
        }

        if slug.lower() in blocked:
            return None

        if len(slug) < 2:
            return None

        if len(slug) > 150:
            return None

        return slug

    except Exception:
        return None


def discover_from_wayback(
    domain,
):
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
                candidates.add(
                    slug
                )

    except Exception as exc:
        print(
            f"Wayback discovery failed "
            f"for {domain}: {exc}"
        )

    return candidates


def discover_from_commoncrawl(domain):
    """Fallback board discovery when Wayback is unavailable."""
    candidates = set()

    try:
        info = SESSION.get(
            "https://index.commoncrawl.org/collinfo.json",
            timeout=30,
        )

        if info.status_code != 200:
            return candidates

        collections = info.json()

        if not collections:
            return candidates

        index_url = collections[0].get("cdx-api")

        if not index_url:
            return candidates

        query = (
            f"{index_url}?url={quote(domain + '/*', safe=':/?*')}"
            "&output=json&filter=status:200&collapse=urlkey"
            "&limit=100000"
        )

        response = SESSION.get(
            query,
            timeout=90,
        )

        if response.status_code != 200:
            print(
                f"Common Crawl returned {response.status_code} "
                f"for {domain}"
            )
            return candidates

        for line in response.text.splitlines():
            if not line.strip():
                continue

            try:
                item = json.loads(line)
                original = item.get("url", "")
            except Exception:
                continue

            slug = extract_slug_from_url(
                original,
                domain,
            )

            if slug:
                candidates.add(slug)

    except Exception as exc:
        print(
            f"Common Crawl discovery failed for {domain}: {exc}"
        )

    return candidates


def discover_boards_for_ats(
    ats,
):
    candidates = set()

    print(
        f"\nDiscovering {ats} boards..."
    )

    for domain in ATS_SOURCES[
        ats
    ].get(
        "archive_domains",
        [],
    ):

        found = discover_from_wayback(
            domain
        )

        # Internet Archive can occasionally return 5xx/503 for
        # high-volume ATS domains. Use Common Crawl as a fallback
        # instead of silently losing that ATS from the crawler.
        if not found:
            fallback = discover_from_commoncrawl(
                domain
            )
            if fallback:
                print(
                    f"  Common Crawl fallback: "
                    f"{len(fallback)} candidates"
                )
            found.update(fallback)

        print(
            f"  {domain}: "
            f"{len(found)} candidates"
        )

        for slug in found:
            key = (ats, slug)
            # Prefer the first discovered domain. This preserves an EU Lever
            # board URL when that is the domain where the board was found.
            DISCOVERED_BOARD_URLS.setdefault(
                key,
                f"https://{domain}/{quote(str(slug), safe='')}"
            )

        candidates.update(
            found
        )

    print(
        f"  Total {ats} candidates: "
        f"{len(candidates)}"
    )

    return candidates


# ============================================================
# BOARD VALIDATION
# ============================================================

def validate_greenhouse(
    slug,
):
    url = ATS_SOURCES[
        "greenhouse"
    ]["api"].format(
        slug=slug
    )

    data = request_json(
        url,
        timeout=20,
    )

    return isinstance(
        data,
        dict,
    ) and isinstance(
        data.get("jobs"),
        list,
    )


def validate_ashby(
    slug,
):
    url = ATS_SOURCES[
        "ashby"
    ]["api"].format(
        slug=slug
    )

    data = request_json(
        url,
        timeout=20,
    )

    return isinstance(
        data,
        dict,
    ) and isinstance(
        data.get("jobs"),
        list,
    )


def validate_lever(
    slug,
    eu=False,
):
    if eu:
        url = ATS_SOURCES[
            "lever"
        ]["eu_api"].format(
            slug=slug
        )
    else:
        url = ATS_SOURCES[
            "lever"
        ]["api"].format(
            slug=slug
        )

    data = request_json(
        url,
        timeout=20,
    )

    return isinstance(
        data,
        list,
    )


def validate_workable(
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

    if not (
        isinstance(data, dict)
        and isinstance(data.get("jobs"), list)
    ):
        return False

    account_name = clean_text(data.get("name"))
    if account_name:
        DISCOVERED_COMPANY_NAMES[("workable", slug)] = account_name

    return True


def validate_board(
    ats,
    slug,
):
    if ats == "greenhouse":
        return validate_greenhouse(
            slug
        )

    if ats == "ashby":
        return validate_ashby(
            slug
        )

    if ats == "lever":
        return (
            validate_lever(
                slug,
                eu=False,
            )
            or validate_lever(
                slug,
                eu=True,
            )
        )

    if ats == "workable":
        return validate_workable(
            slug
        )

    return False


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

    valid = sorted(
        set(valid),
        key=str.lower,
    )

    print(
        f"  LIVE {ats} boards: "
        f"{len(valid)}"
    )

    return valid


# ============================================================
# PERSISTENT BOARD CACHE
# ============================================================

# The Boards sheet is the permanent ATS/company registry.
# Company is stored as a human-readable name and is hyperlinked to
# the company's ATS-hosted board. Board Slug remains the stable key.
BOARD_HEADERS = [
    "Company",
    "ATS",
    "Board Slug",
    "ATS Link",
    "Last Validated",
    "Failures",
    "Active",
    "Discovery Source",
]


def humanize_company_slug(slug):
    """Best-effort company display name when the ATS does not expose one."""
    value = unquote(clean_text(slug))
    value = re.sub(r"[_-]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()

    if not value:
        return ""

    # Keep obvious domain-style/company-brand punctuation readable.
    if "." in value and " " not in value:
        parts = value.split(".")
        value = ".".join(
            part[:1].upper() + part[1:]
            if part else part
            for part in parts
        )
        return value

    # Preserve all-uppercase acronyms while title-casing ordinary slugs.
    return " ".join(
        part if part.isupper() else part[:1].upper() + part[1:]
        for part in value.split()
    )


def ats_board_url(ats, slug):
    """Return the stable public ATS-hosted company/board URL."""
    encoded = quote(str(slug), safe="")

    if ats == "greenhouse":
        return f"https://job-boards.greenhouse.io/{encoded}"

    if ats == "ashby":
        return f"https://jobs.ashbyhq.com/{encoded}"

    if ats == "lever":
        return f"https://jobs.lever.co/{encoded}"

    if ats == "workable":
        return f"https://apply.workable.com/{encoded}/"

    return ""


def board_display_name(ats, slug, existing_name=""):
    """Prefer an existing verified name; otherwise humanize the ATS slug."""
    existing_name = clean_text(existing_name)
    if existing_name:
        return existing_name

    return humanize_company_slug(slug)


def load_board_cache(worksheet):
    cache = {}

    try:
        rows = worksheet.get_all_values()

        if not rows:
            return cache

        headers = rows[0]

        def idx(name):
            try:
                return headers.index(name)
            except ValueError:
                return -1

        # Support both the new registry format and the previous format.
        ats_i = idx("ATS")
        slug_i = idx("Board Slug")
        company_i = idx("Company")
        validated_i = idx("Last Validated")
        failures_i = idx("Failures")
        active_i = idx("Active")
        source_i = idx("Discovery Source")
        link_i = idx("ATS Link")

        if ats_i < 0 or slug_i < 0:
            return cache

        for row in rows[1:]:
            if len(row) <= max(ats_i, slug_i):
                continue

            ats = clean_text(row[ats_i]).lower()
            slug = clean_text(row[slug_i])

            if not ats or not slug:
                continue

            active = True
            if active_i >= 0 and len(row) > active_i:
                active = clean_text(row[active_i]).lower() in {
                    "yes",
                    "true",
                    "1",
                }

            # Inactive boards stay in the sheet for history, but are not
            # returned to the daily crawl set.
            company = (
                clean_text(row[company_i])
                if company_i >= 0 and len(row) > company_i
                else ""
            )

            validated = (
                clean_text(row[validated_i])
                if validated_i >= 0 and len(row) > validated_i
                else ""
            )

            try:
                failures = int(
                    row[failures_i]
                ) if (
                    failures_i >= 0
                    and len(row) > failures_i
                    and clean_text(row[failures_i])
                ) else 0
            except Exception:
                failures = 0

            source = (
                clean_text(row[source_i])
                if source_i >= 0 and len(row) > source_i
                else ""
            )

            link = (
                clean_text(row[link_i])
                if link_i >= 0 and len(row) > link_i
                else ""
            )

            cache[(ats, slug)] = {
                "ats": ats,
                "slug": slug,
                "company": company,
                "ats_link": link or ats_board_url(ats, slug),
                "last_validated": validated,
                "failures": failures,
                "active": active,
                "discovery_source": source or "Existing",
            }

    except Exception as exc:
        print(f"Could not load board cache: {exc}")

    return cache


def parse_datetime_utc(value):
    """Parse an ISO timestamp and always return a UTC-aware datetime.

    Google Sheets may return older timestamps without a timezone while new
    crawler timestamps are timezone-aware. Normalizing both prevents Python's
    offset-naive vs offset-aware comparison error.
    """
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            str(value).strip().replace("Z", "+00:00")
        )
    except Exception:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    return parsed


def board_cache_is_fresh(cache):
    if not cache:
        return False

    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(days=BOARD_CACHE_DAYS)
    )

    dates = []

    for item in cache.values():
        if not item.get("active", True):
            continue

        validated = parse_datetime_utc(
            item.get("last_validated", "")
        )

        if validated is None:
            return False

        dates.append(validated)

    if not dates:
        return False

    return min(dates) >= cutoff


def save_board_cache(
    worksheet,
    boards,
    existing_cache=None,
    discovery_sources=None,
    company_names=None,
    ats_links=None,
):
    """Merge live ATS boards into the permanent Boards registry.

    Key:
        (ATS, Board Slug)

    The company name and ATS link are stored with every board. The company
    cell is written as a Google Sheets HYPERLINK formula so clicking the
    company opens the ATS-hosted company board directly.
    """
    existing_cache = existing_cache or {}
    discovery_sources = discovery_sources or {}
    company_names = company_names or {}
    ats_links = ats_links or {}

    today = datetime.now(timezone.utc).date().isoformat()
    merged = {}

    # Preserve all existing entries, including inactive historical entries.
    for key, item in existing_cache.items():
        ats, slug = key
        merged[key] = {
            "ats": ats,
            "slug": slug,
            "company": board_display_name(
                ats,
                slug,
                item.get("company", ""),
            ),
            "ats_link": (
                item.get("ats_link")
                or ats_board_url(ats, slug)
            ),
            "last_validated": item.get("last_validated", ""),
            "failures": item.get("failures", 0),
            "active": item.get("active", True),
            "discovery_source": item.get(
                "discovery_source",
                "Existing",
            ),
        }

    # Confirmed boards become active and reset their failure counter.
    for ats, slugs in boards.items():
        for slug in slugs:
            key = (ats, slug)
            old = merged.get(key, {})

            company = (
                company_names.get(key)
                or old.get("company")
                or humanize_company_slug(slug)
            )

            link = (
                ats_links.get(key)
                or old.get("ats_link")
                or ats_board_url(ats, slug)
            )

            merged[key] = {
                "ats": ats,
                "slug": slug,
                "company": board_display_name(
                    ats,
                    slug,
                    company,
                ),
                "ats_link": link,
                "last_validated": today,
                "failures": 0,
                "active": True,
                "discovery_source": (
                    discovery_sources.get(key)
                    or old.get("discovery_source")
                    or "ATS"
                ),
            }

    rows = [BOARD_HEADERS]

    for key in sorted(
        merged,
        key=lambda x: (x[0].lower(), x[1].lower()),
    ):
        item = merged[key]
        company = item["company"] or humanize_company_slug(item["slug"])
        link = item["ats_link"] or ats_board_url(
            item["ats"],
            item["slug"],
        )

        # Formula is intentionally used only for the display-name cell.
        # The raw ATS Link remains visible in its own column for easy copying.
        company_formula = (
            f'=HYPERLINK("{link.replace(chr(34), chr(34) * 2)}",'
            f'"{company.replace(chr(34), chr(34) * 2)}")'
            if link
            else company
        )

        rows.append([
            company_formula,
            item["ats"],
            item["slug"],
            link,
            item["last_validated"],
            item["failures"],
            "Yes" if item["active"] else "No",
            item["discovery_source"],
        ])

    try:
        worksheet.clear()
        worksheet.update(
            "A1",
            rows,
            value_input_option="USER_ENTERED",
        )
        print(
            f"Saved {len(rows) - 1} unique boards to Boards sheet "
            "with company names and ATS links."
        )
    except Exception as exc:
        print(f"Could not save board cache: {exc}")


def board_needs_revalidation(item):
    validated = parse_datetime_utc(
        item.get("last_validated", "")
    )

    if validated is None:
        return True

    return validated < (
        datetime.now(timezone.utc)
        - timedelta(days=BOARD_CACHE_DAYS)
    )


def discover_or_load_boards(spreadsheet):
    """Daily discovery + direct ATS validation + persistent board registry.

    Flow:
      1. Load the permanent Boards registry.
      2. Scan Internet Archive/Common Crawl every day.
      3. Skip duplicate (ATS, slug) candidates already known.
      4. Validate new/stale candidates directly against the ATS.
      5. Keep all previously active boards even if an archive misses them.
      6. Save company name + ATS board URL in Boards.
      7. Return all active boards for the direct daily ATS job crawl.
    """
    boards_sheet = get_or_create_worksheet(
        spreadsheet,
        "Boards",
        rows=10000,
        cols=len(BOARD_HEADERS),
    )

    existing_cache = load_board_cache(boards_sheet)
    print(f"\nExisting unique Boards entries: {len(existing_cache)}")

    discovered = {ats: set() for ats in ATS_SOURCES}
    discovery_sources = {}
    company_names = {}
    ats_links = {}

    print("\nDaily board discovery — Internet Archive first")

    for ats in ATS_SOURCES:
        candidates = discover_boards_for_ats(ats)
        discovered[ats].update(candidates)

        for slug in candidates:
            key = (ats, slug)
            discovery_sources[key] = "Internet Archive/Common Crawl"
            ats_links[key] = (
                DISCOVERED_BOARD_URLS.get(key)
                or ats_board_url(ats, slug)
            )
            company_names[key] = (
                DISCOVERED_COMPANY_NAMES.get(key)
                or humanize_company_slug(slug)
            )

    boards = {ats: set() for ats in ATS_SOURCES}
    candidates_to_validate = {ats: set() for ats in ATS_SOURCES}

    for ats in ATS_SOURCES:
        for slug in discovered[ats]:
            key = (ats, slug)

            if key not in existing_cache:
                candidates_to_validate[ats].add(slug)
                continue

            # Existing boards are skipped as duplicates unless their ATS
            # validation is stale. A stale board is rechecked, not duplicated.
            if board_needs_revalidation(existing_cache[key]):
                candidates_to_validate[ats].add(slug)
            elif existing_cache[key].get("active", True):
                boards[ats].add(slug)

    # Existing active boards always remain in the daily crawl set. An archive
    # miss must never make a known live company disappear.
    for (ats, slug), item in existing_cache.items():
        if ats in boards and item.get("active", True):
            boards[ats].add(slug)

    total_candidates = sum(
        len(values)
        for values in candidates_to_validate.values()
    )

    print(
        "\nNew/stale board candidates requiring direct ATS validation: "
        f"{total_candidates}"
    )

    for ats in ATS_SOURCES:
        candidates = sorted(
            candidates_to_validate[ats],
            key=str.lower,
        )

        if not candidates:
            continue

        valid = validate_boards(ats, candidates)

        for slug in valid:
            key = (ats, slug)
            boards[ats].add(slug)
            discovery_sources[key] = (
                "Internet Archive/Common Crawl"
                if slug in discovered[ats]
                else "ATS"
            )
            ats_links[key] = (
                DISCOVERED_BOARD_URLS.get(key)
                or ats_board_url(ats, slug)
            )
            company_names[key] = (
                DISCOVERED_COMPANY_NAMES.get(key)
                or humanize_company_slug(slug)
            )

    save_board_cache(
        boards_sheet,
        boards,
        existing_cache=existing_cache,
        discovery_sources=discovery_sources,
        company_names=company_names,
        ats_links=ats_links,
    )

    print("\nDirect ATS crawl will now scan every active board daily.")

    for ats in ATS_SOURCES:
        print(f"  {ats}: {len(boards.get(ats, set()))}")

    return {
        ats: sorted(values, key=str.lower)
        for ats, values in boards.items()
    }


# ============================================================
# JOB FETCHERS
# ============================================================

def fetch_greenhouse_detail(slug, job_id):
    """Fetch one Greenhouse job only when the list result is ambiguous."""
    if not job_id:
        return None

    base = ATS_SOURCES["greenhouse"]["api"].format(
        slug=slug
    )

    return request_json(
        f"{base}/{job_id}",
        timeout=20,
    )


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

    results = []

    for job in data.get(
        "jobs",
        [],
    ):

        title = clean_text(
            job.get("title")
        )

        if not title_is_relevant(
            title
        ):
            continue

        location = clean_text(
            (
                job.get("location")
                or {}
            ).get("name")
        )

        job_id = str(
            job.get("id")
            or ""
        )

        workplace = classify_workplace(
            location=location,
        )

        country = classify_country(
            location=location,
        )

        # Only relevant-but-ambiguous Greenhouse postings get a
        # second request. This recovers country/workplace data from
        # the full posting without downloading every description.
        if not location_is_allowed(
            country,
            workplace,
        ) and job_id:
            detail = fetch_greenhouse_detail(
                slug,
                job_id,
            )

            if isinstance(detail, dict):
                detail_location = clean_text(
                    (
                        detail.get("location")
                        or {}
                    ).get("name")
                )

                if detail_location:
                    location = detail_location

                office_text = " ".join(
                    (
                        clean_text(office.get("name"))
                        + " "
                        + clean_text(office.get("location"))
                    )
                    for office in (detail.get("offices") or [])
                    if isinstance(office, dict)
                )

                department_text = " ".join(
                    clean_text(dept.get("name"))
                    for dept in (detail.get("departments") or [])
                    if isinstance(dept, dict)
                )

                detail_text = " ".join([
                    clean_text(detail.get("content")),
                    office_text,
                    department_text,
                ])

                workplace = classify_workplace(
                    location=location,
                    description=detail_text,
                )

                country = classify_country(
                    location=location,
                    description=detail_text,
                )

        if not location_is_allowed(
            country,
            workplace,
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

        link = clean_text(
            job.get(
                "absolute_url"
            )
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
            "workplace": workplace,
            "posted_date": posted,
            "job_url": link,
        })

    return results


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

    results = []

    for job in data.get(
        "jobs",
        [],
    ):

        if job.get(
            "isListed",
            True,
        ) is False:
            continue

        title = clean_text(
            job.get("title")
        )

        if not title_is_relevant(
            title
        ):
            continue

        locations = []

        primary = clean_text(
            job.get("location")
        )

        if primary:
            locations.append(
                primary
            )

        address = (
            job.get("address")
            or {}
        ).get(
            "postalAddress"
        ) or {}

        address_country = clean_text(
            address.get(
                "addressCountry"
            )
        )

        address_city = clean_text(
            address.get(
                "addressLocality"
            )
        )

        address_region = clean_text(
            address.get(
                "addressRegion"
            )
        )

        if address_city:
            locations.append(
                address_city
            )

        if address_region:
            locations.append(
                address_region
            )

        if address_country:
            locations.append(
                address_country
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
                loc = clean_text(
                    secondary.get(
                        "location"
                    )
                )

                if loc:
                    locations.append(
                        loc
                    )

                sec_address = (
                    secondary.get(
                        "address"
                    )
                    or {}
                )

                sec_country = clean_text(
                    sec_address.get(
                        "addressCountry"
                    )
                )

                if sec_country:
                    locations.append(
                        sec_country
                    )

        location = ", ".join(
            dict.fromkeys(
                x for x in locations
                if x
            )
        )

        description = clean_text(
            job.get(
                "descriptionPlain"
            )
        )

        workplace = classify_workplace(
            location=location,
            workplace_type=job.get(
                "workplaceType"
            ),
            remote_flag=bool(
                job.get(
                    "isRemote",
                    False,
                )
            ),
            description=description,
        )

        country = classify_country(
            location=location,
            address_country=address_country,
            description=description,
        )

        if not location_is_allowed(
            country,
            workplace,
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

        link = clean_text(
            job.get("jobUrl")
            or job.get("applyUrl")
        )

        if not link:
            continue

        job_id = str(
            job.get("id")
            or job.get("jobId")
            or ""
        )

        results.append({
            "ats": "ashby",
            "company": slug,
            "job_id": job_id,
            "title": title,
            "location": location,
            "country": country,
            "workplace": workplace,
            "posted_date": posted,
            "job_url": link,
        })

    return results


def fetch_lever(
    slug,
    eu=False,
):
    if eu:
        url = ATS_SOURCES[
            "lever"
        ]["eu_api"].format(
            slug=slug
        )
    else:
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

        if not title_is_relevant(
            title
        ):
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

        description = clean_text(
            job.get(
                "descriptionPlain"
            )
            or job.get(
                "description"
            )
        )

        country_code = clean_text(
            job.get(
                "country"
            )
        )

        workplace = classify_workplace(
            location=location,
            workplace_type=job.get(
                "workplaceType"
            ),
            description=description,
        )

        country = classify_country(
            location=location,
            country_code=country_code,
            description=description,
        )

        if not location_is_allowed(
            country,
            workplace,
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

        urls = (
            job.get("urls")
            or {}
        )

        link = clean_text(
            urls.get("show")
            or job.get("hostedUrl")
            or job.get("applyUrl")
        )

        if not link:
            continue

        job_id = str(
            job.get("id")
            or ""
        )

        results.append({
            "ats": "lever",
            "company": slug,
            "job_id": job_id,
            "title": title,
            "location": location,
            "country": country,
            "workplace": workplace,
            "posted_date": posted,
            "job_url": link,
        })

    return results


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

    results = []

    for job in data.get(
        "jobs",
        [],
    ):

        title = clean_text(
            job.get("title")
        )

        if not title_is_relevant(
            title
        ):
            continue

        country_code = clean_text(
            job.get("country")
        )

        city = clean_text(
            job.get("city")
        )

        state = clean_text(
            job.get("state")
        )

        location = ", ".join(
            x for x in [
                city,
                state,
                country_code,
            ]
            if x
        )

        workplace = classify_workplace(
            location=location,
            workplace_type=job.get(
                "workplace_type"
            ),
            remote_flag=bool(
                job.get(
                    "telecommuting",
                    False,
                )
            ),
        )

        country = classify_country(
            location=location,
            country_code=country_code,
        )

        if not location_is_allowed(
            country,
            workplace,
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

        link = clean_text(
            job.get(
                "application_url"
            )
            or job.get("url")
            or job.get("shortlink")
        )

        if not link:
            continue

        job_id = str(
            job.get("shortcode")
            or job.get("code")
            or ""
        )

        results.append({
            "ats": "workable",
            "company": slug,
            "job_id": job_id,
            "title": title,
            "location": location,
            "country": country,
            "workplace": workplace,
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
# CRAWL
# ============================================================

def crawl_boards(
    boards,
):
    tasks = []

    for ats, slugs in boards.items():

        if MAX_BOARDS_PER_RUN:
            slugs = slugs[
                :MAX_BOARDS_PER_RUN
            ]

        for slug in slugs:

            # Lever may exist on either global or EU instance.
            # The fetcher first tries the normal endpoint.
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

    all_jobs = []

    with ThreadPoolExecutor(
        max_workers=CONCURRENCY
    ) as executor:

        futures = {}

        for ats, slug in tasks:

            future = executor.submit(
                crawl_one_board,
                ats,
                slug,
            )

            futures[future] = (
                ats,
                slug,
            )

        completed = 0

        for future in as_completed(
            futures
        ):

            ats, slug = futures[
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


def crawl_one_board(
    ats,
    slug,
):
    if ats == "lever":

        jobs = fetch_lever(
            slug,
            eu=False,
        )

        if jobs:
            return jobs

        # If global endpoint returned nothing, try EU.
        return fetch_lever(
            slug,
            eu=True,
        )

    return FETCHERS[
        ats
    ](slug)


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate_jobs(
    jobs,
):
    unique = {}

    for job in jobs:

        key = make_job_key(
            job["ats"],
            job["job_id"],
            job["job_url"],
        )

        if key not in unique:
            unique[key] = job

    return list(
        unique.values()
    )


# ============================================================
# EXISTING JOB KEYS
# ============================================================

JOB_HEADERS = [
    "Date",
    "Company",
    "Job Title",
    "Location",
    "Country",
    "Posted Date",
    "Job Link",
    "Job ID",
    "ATS",
]


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

        def idx(name):
            try:
                return headers.index(
                    name
                )
            except ValueError:
                return -1

        ats_i = idx("ATS")
        id_i = idx("Job ID")
        url_i = idx("Job Link")
        company_i = idx("Company")
        title_i = idx("Job Title")
        location_i = idx("Location")

        for row in rows[1:]:

            ats = (
                row[ats_i]
                if ats_i >= 0
                and len(row) > ats_i
                else ""
            )

            job_id = (
                row[id_i]
                if id_i >= 0
                and len(row) > id_i
                else ""
            )

            url = (
                row[url_i]
                if url_i >= 0
                and len(row) > url_i
                else ""
            )

            company = (
                row[company_i]
                if company_i >= 0
                and len(row) > company_i
                else ""
            )

            title = (
                row[title_i]
                if title_i >= 0
                and len(row) > title_i
                else ""
            )

            location = (
                row[location_i]
                if location_i >= 0
                and len(row) > location_i
                else ""
            )

            if ats and job_id:
                existing.add(
                    (
                        "id",
                        ats.lower().strip(),
                        str(job_id).lower().strip(),
                    )
                )

            normalized = (
                normalize_url(url)
            )

            if normalized:
                existing.add(
                    (
                        "url",
                        normalized,
                    )
                )

            if (
                company
                and title
                and location
            ):
                existing.add(
                    (
                        "fallback",
                        clean_text(
                            company
                        ).lower(),
                        clean_text(
                            title
                        ).lower(),
                        clean_text(
                            location
                        ).lower(),
                    )
                )

    except Exception as exc:
        print(
            f"Could not read existing "
            f"Jobs sheet: {exc}"
        )

    return existing


def job_already_exists(
    job,
    existing_keys,
):
    ats = clean_text(
        job["ats"]
    ).lower()

    job_id = clean_text(
        job["job_id"]
    ).lower()

    url = normalize_url(
        job["job_url"]
    )

    company = clean_text(
        job["company"]
    ).lower()

    title = clean_text(
        job["title"]
    ).lower()

    location = clean_text(
        job["location"]
    ).lower()

    if job_id and (
        "id",
        ats,
        job_id,
    ) in existing_keys:
        return True

    if url and (
        "url",
        url,
    ) in existing_keys:
        return True

    if (
        company
        and title
        and location
        and (
            "fallback",
            company,
            title,
            location,
        ) in existing_keys
    ):
        return True

    return False


# ============================================================
# SAVE JOBS
# ============================================================

def ensure_job_headers(worksheet):
    values = worksheet.get_all_values()

    if not values:
        worksheet.update(
            "A1",
            [JOB_HEADERS],
            value_input_option="RAW",
        )
        return

    headers = [clean_text(x) for x in values[0]]

    if headers == JOB_HEADERS:
        return

    # Older 8-column Date-first layout used by the sheet screenshot:
    # Date | Company | Job Title | Location | Posted Date | Job Link | Job ID | ATS
    # Insert Country without deleting any existing job data.
    old_date_first = [
        "Date",
        "Company",
        "Job Title",
        "Location",
        "Posted Date",
        "Job Link",
        "Job ID",
        "ATS",
    ]

    if headers == old_date_first:
        converted = [JOB_HEADERS]

        for row in values[1:]:
            padded = list(row)
            while len(padded) < 8:
                padded.append("")

            converted.append([
                padded[0],  # Date
                padded[1],  # Company
                padded[2],  # Job Title
                padded[3],  # Location
                "",        # Country was not present
                padded[4],  # Posted Date
                padded[5],  # Job Link
                padded[6],  # Job ID
                padded[7],  # ATS
            ])

        worksheet.clear()
        worksheet.update(
            "A1",
            converted,
            value_input_option="RAW",
        )
        print(
            "Upgraded Jobs sheet: inserted missing Country column "
            "while preserving existing rows."
        )
        return

    # Previous 8-column layout from an older crawler version:
    old_company_first = [
        "Company",
        "Job Title",
        "Location",
        "Country",
        "Posted Date",
        "Job Link",
        "ATS",
        "Job ID",
    ]

    if headers == old_company_first:
        today = datetime.now(timezone.utc).date().isoformat()
        converted = [JOB_HEADERS]

        for row in values[1:]:
            padded = list(row)
            while len(padded) < 8:
                padded.append("")

            converted.append([
                today,
                padded[0],
                padded[1],
                padded[2],
                padded[3],
                padded[4],
                padded[5],
                padded[7],
                padded[6],
            ])

        worksheet.clear()
        worksheet.update(
            "A1",
            converted,
            value_input_option="RAW",
        )
        print(
            "Converted old Jobs sheet to the Date-first format."
        )
        return

    # If the user has a custom header, do not destroy their data.
    # save_jobs will still use named-column lookup where possible.
    print(
        "Existing Jobs header is custom; preserving it. "
        "No destructive header rewrite was performed."
    )


def save_jobs(
    spreadsheet,
    jobs,
):
    worksheet = get_or_create_worksheet(
        spreadsheet,
        "Jobs",
        rows=5000,
        cols=9,
    )

    ensure_job_headers(
        worksheet
    )

    existing_keys = (
        load_existing_keys(
            worksheet
        )
    )

    added_date = (
        datetime.now(
            timezone.utc
        ).date().isoformat()
    )

    new_rows = []

    for job in jobs:

        if job_already_exists(
            job,
            existing_keys,
        ):
            continue

        row = [
            added_date,
            job["company"],
            job["title"],
            job["location"],
            job["country"],
            job["posted_date"],
            job["job_url"],
            job["job_id"],
            job["ats"],
        ]

        new_rows.append(
            row
        )

        ats = clean_text(
            job["ats"]
        ).lower()

        job_id = clean_text(
            job["job_id"]
        ).lower()

        url = normalize_url(
            job["job_url"]
        )

        company = clean_text(
            job["company"]
        ).lower()

        title = clean_text(
            job["title"]
        ).lower()

        location = clean_text(
            job["location"]
        ).lower()

        if job_id:
            existing_keys.add(
                (
                    "id",
                    ats,
                    job_id,
                )
            )

        if url:
            existing_keys.add(
                (
                    "url",
                    url,
                )
            )

        if (
            company
            and title
            and location
        ):
            existing_keys.add(
                (
                    "fallback",
                    company,
                    title,
                    location,
                )
            )

    if not new_rows:
        print(
            "\nNo new matching jobs."
        )
        return 0

    # Append in one API call rather than one row at a time.
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
        "REMOTE4.ME QUALITY JOB CRAWLER"
    )
    print(
        "========================================"
    )

    print(
        f"Started UTC: "
        f"{started.isoformat()}"
    )

    print(
        "\nScope:"
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
        "  Customer Experience"
    )
    print(
        "  Customer Service"
    )
    print(
        "  Onboarding / Implementation"
    )
    print(
        "  Enablement / Adoption / Retention"
    )

    print(
        "\nLocation rules:"
    )
    print(
        "  USA = Remote only"
    )
    print(
        "  India = Remote + Hybrid"
    )

    print(
        f"\nJob age window: "
        f"{MAX_JOB_AGE_DAYS} days"
    )

    print(
        f"Board cache: "
        f"{BOARD_CACHE_DAYS} days"
    )

    # --------------------------------------------------------
    # GOOGLE SHEETS
    # --------------------------------------------------------

    print(
        "\nSTEP 1 — Connecting to Google Sheets"
    )

    spreadsheet = get_spreadsheet()

    # --------------------------------------------------------
    # BOARD DISCOVERY / CACHE
    # --------------------------------------------------------

    print(
        "\nSTEP 2 — Loading/discovering ATS boards"
    )

    boards = discover_or_load_boards(
        spreadsheet
    )

    total_boards = sum(
        len(items)
        for items in boards.values()
    )

    print(
        f"\nLive boards available: "
        f"{total_boards}"
    )

    for ats in ATS_SOURCES:
        print(
            f"  {ats}: "
            f"{len(boards.get(ats, []))}"
        )

    if total_boards == 0:
        print(
            "\nNo live boards available."
        )
        return

    # --------------------------------------------------------
    # CRAWL
    # --------------------------------------------------------

    print(
        "\nSTEP 3 — Crawling jobs"
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
    # DEDUPLICATE
    # --------------------------------------------------------

    print(
        "\nSTEP 4 — Deduplicating"
    )

    jobs = deduplicate_jobs(
        jobs
    )

    print(
        f"Unique jobs this run: "
        f"{len(jobs)}"
    )

    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------

    print(
        "\nSTEP 5 — Updating Google Sheets"
    )

    added = save_jobs(
        spreadsheet,
        jobs,
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
        "CRAWLER FINISHED SUCCESSFULLY"
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
