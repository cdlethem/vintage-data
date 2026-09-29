# Add one new public data source

This project collects public data sources that change over time and need no key or login. Add one new, useful source end to end, as a single pull request.

1. **Pick one candidate.** First look for a staged fetcher in `discovery/staged_scripts/` whose script no `extract/sources/*.yml` references yet (compare their `script:` values). Otherwise find a public API or feed that publishes regularly changing data. Skip anything in `extract/retired_sources.yml`, anything already collected, and anything an open issue above already covers.
2. **Check it for real.** Fetch a small sample, confirm the terms or licence allow collection, and note how often the data actually changes.
3. **Implement it** following the "Extract and source changes" rules in `agents.md`: the fetcher in `extract/scripts/` (copy it from `discovery/staged_scripts/` when one exists, keeping the staged original), one `extract/sources/<name>.yml` with a staggered UTC cron no faster than the data changes plus rate, licence and attribution metadata, and a focused test.
4. **Verify.** Run the fetcher once against the live source with temporary state, and run its tests.

Answer `fix` with the change, `none` if no candidate qualifies today, or `ask` if a promising source needs a human decision (unclear terms, registration, cost). In the summary, say what data the source provides and why it is worth collecting.
