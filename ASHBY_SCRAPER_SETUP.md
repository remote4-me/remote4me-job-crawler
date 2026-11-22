# Ashby scraper for Remote4Me

This is an isolated Ashby-only pass that reuses the existing `crawler.py` implementation.

## How it runs

- The existing **Remote4.me Job Crawler** workflow runs on its normal daily schedule (7:00 PM IST).
- After that workflow completes successfully, **Remote4Me Ashby Support Jobs** runs automatically.
- You can also start the Ashby workflow manually from **Actions → Remote4Me Ashby Support Jobs → Run workflow**.
- Both workflows use GitHub-hosted runners. No VPS, Docker host, PostgreSQL server, or Intel NUC is required for this Ashby pass.

## Data scope

The runner reuses the existing Ashby adapter and the existing support-role, country and workplace classifiers in `crawler.py`.

Only jobs classified as:
- India + Remote
- India + Hybrid
- USA + Remote
- USA + Hybrid

are written. The existing crawler's job-age window and title relevance rules also apply. Each listing must have a direct job URL.

## Google Sheets

The runner uses the same existing repository Actions secrets as the main crawler:

- `GOOGLE_CREDENTIALS`
- `SPREADSHEET_ID`

No new secret or paid API key is required. The Google service-account email in `GOOGLE_CREDENTIALS` must already have Editor access to the spreadsheet.

The runner reads the existing `Boards` tab but does not modify it. It appends new, deduplicated jobs into these separate tabs:

- `Ashby Remote India`
- `Ashby Remote USA`
- `Ashby Hybrid India`
- `Ashby Hybrid USA`

It does **not** write to the existing `Jobs`, `Remote India`, `Remote USA`, `Hybrid India`, `Hybrid USA`, or `India Discovery` tabs.

## Optional seed boards

The main crawler normally discovers and verifies Ashby boards into the `Boards` registry. To test or add known boards manually, create an Actions **repository variable** named `ASHBY_SEED_SLUGS` containing comma-separated Ashby board slugs, for example:

`exampleco,another-company`

Do not paste full job URLs; use the board slug from `https://jobs.ashbyhq.com/{slug}`. This variable is optional.

## Manual test

1. Open the repository's **Actions** tab.
2. Select **Remote4Me Ashby Support Jobs**.
3. Choose **Run workflow** on the `main` branch.
4. Open the run and review the logs.
5. Check the four Ashby tabs in Google Sheets.

An empty result can be legitimate: no active boards, no matching jobs, no jobs inside the existing age window, or no explicit India/USA + Remote/Hybrid eligibility. Check the run log before assuming a failure.

## Publishing

This workflow only collects job data into Google Sheets. It has no WordPress credentials, no WordPress API calls, and no automatic publishing step. Review each listing and publish it manually using the existing Remote4Me process.
