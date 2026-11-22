# Ashby-only Remote4Me runner.
# Imports the existing crawler module to reuse its tested Ashby adapter,
# India/USA + Remote/Hybrid eligibility rules and Google Sheets helpers.
# It intentionally reads the existing Boards registry but never modifies it.

import os
from datetime import datetime, timedelta, timezone

import crawler


def main():
    spreadsheet = crawler.get_spreadsheet()
    boards_sheet = crawler.get_or_create_worksheet(
        spreadsheet, "Boards", rows=10000, cols=len(crawler.BOARD_HEADERS)
    )
    registry = crawler.load_board_cache(boards_sheet)
    slugs = sorted(
        slug for (ats, slug), item in registry.items()
        if ats == "ashby" and item.get("active", True)
    )

    # Optional seed list allows known Ashby company slugs to be tested
    # without requiring a full Common Crawl / Wayback discovery run.
    seed = os.environ.get("ASHBY_SEED_SLUGS", "")
    slugs = sorted(set(slugs) | {
        value.strip() for value in seed.split(",") if value.strip()
    })

    print(f"Active Ashby boards loaded: {len(slugs)}")
    if not slugs:
        print("No Ashby boards in Boards registry and no seed slugs supplied.")
        print("Add Ashby board slugs to ASHBY_SEED_SLUGS or the Boards tab.")
        return

    # Use the same bounded thread pool as the main crawler rather than
    # issuing hundreds of board requests sequentially.
    company_by_slug = {
        slug: (item.get("company") or crawler.humanize_company_slug(slug))
        for (ats, slug), item in registry.items()
        if ats == "ashby"
    }
    jobs = crawler.crawl_boards({"ashby": slugs})
    for job in jobs:
        slug = job.get("company", "")
        job["company"] = company_by_slug.get(
            slug, crawler.humanize_company_slug(slug)
        )

    # Remote4Me's preferred freshness window is 7 days. The shared adapter
    # already removes older postings at its configured limit; this stricter
    # Ashby-only pass keeps the separate tabs aligned to the 7-day window.
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=7)
    recent_jobs = []
    for job in jobs:
        posted = crawler.parse_date(job.get("posted_date", ""))
        if not posted:
            recent_jobs.append(job)  # Keep undated public listings for manual review.
            continue
        try:
            posted_day = datetime.strptime(posted, "%Y-%m-%d").date()
        except ValueError:
            recent_jobs.append(job)
            continue
        if posted_day >= cutoff:
            recent_jobs.append(job)

    jobs = crawler.deduplicate_jobs(recent_jobs)
    print(f"Unique Ashby jobs within the 7-day window: {len(jobs)}")

    # Keep output isolated from the existing Remote4Me master and country tabs.
    # append_new_jobs_to_worksheet uses job ID, direct URL and fallback keys.
    headers = ["Date", "Company", "Job Title", "Location", "Country",
               "Posted Date", "Job Link", "Job ID", "ATS"]
    total_added = 0
    for country, workplace, tab in [
        ("India", "remote", "Ashby Remote India"),
        ("USA", "remote", "Ashby Remote USA"),
        ("India", "hybrid", "Ashby Hybrid India"),
        ("USA", "hybrid", "Ashby Hybrid USA"),
    ]:
        ws = crawler.get_or_create_worksheet(spreadsheet, tab, rows=5000, cols=len(headers))
        values = ws.get_all_values()
        if not values:
            ws.update("A1", [headers], value_input_option="RAW")
        elif values[0] != headers:
            raise RuntimeError(
                f"Refusing to write to {tab}: unexpected headers. "
                "No sheet data was changed."
            )

        subset = [
            job for job in jobs
            if job.get("country") == country
            and job.get("workplace") == workplace
        ]
        total_added += crawler.append_new_jobs_to_worksheet(ws, subset, tab)

    print(f"Finished. Newly appended Ashby jobs: {total_added}")
    print("Existing Jobs, Remote India/USA, Hybrid India/USA and Boards tabs were not edited.")
    print("No WordPress API or automatic publishing is used.")


if __name__ == "__main__":
    main()
