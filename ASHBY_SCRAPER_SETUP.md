# Remote4Me Multi-ATS Discovery

Remote4Me uses one daily crawler for public ATS job boards. Ashby is one source among the supported ATS platforms; there is no separate Ashby output workflow or Ashby-specific Google Sheets destination in the intended setup.

## Supported ATS platforms

- Ashby
- Greenhouse
- Lever
- Workable
- SmartRecruiters
- Recruitee
- Teamtailor
- Pinpoint
- Workday
- Keka

## Discovery and collection

The crawler discovers company boards using the Internet Archive and Common Crawl, then validates candidates against each provider's public jobs endpoint before saving them to the persistent `Boards` registry in Google Sheets.

For Recruitee, Teamtailor, Pinpoint, Workday and Keka, discovery uses company-hosted subdomains. Workday boards use a tenant / wd-host / site identifier. Existing board entries remain available for daily crawling even if a board is absent from a particular archive snapshot.

Each ATS has its own public response format. The crawler normalizes matching jobs into the existing Remote4Me fields and applies the same customer-support role relevance, India/USA country, Remote/Hybrid workplace, publication-age, direct-job-link and deduplication rules.

## Google Sheets destinations

All ATS sources write into the existing four destination tabs:

- `Remote India`
- `Remote USA`
- `Hybrid India`
- `Hybrid USA`

The existing `Boards` registry remains the shared list of validated company career boards. No ATS-specific tabs should be created. Existing master `Jobs` and `India Discovery` behaviour is retained by the main crawler.

## GitHub Actions and credentials

Use the existing scheduled Remote4Me crawler workflow and existing repository secrets:

- `GOOGLE_CREDENTIALS`
- `SPREADSHEET_ID`

Optional search-provider keys remain optional and are not required for the ATS board discovery adapters. The crawler runs on GitHub-hosted runners; it does not require a VPS, Docker host, PostgreSQL server or Intel NUC.

## Publishing

This is a discovery and collection system only. It writes job leads to Google Sheets for manual review. It does not create or publish WordPress job posts. Continue to review each job and publish it manually through the existing Remote4Me process.

## Operational notes

- Public ATS APIs can change their response formats or apply rate limits. A failed board validation should not remove other working sources.
- Workday boards are paginated and capped per company per run to keep the daily job bounded.
- Only jobs with a supported India/USA location and an explicit Remote/Hybrid signal are eligible for the four destination tabs.
- A board can be discovered successfully but have no relevant support jobs in a given run; that is not itself a failure.
