# The trend standard for mart dashboards

This project exists to show how published data changes over time. A dashboard
whose only chart ranks categories over the whole collection window fails that
purpose: it answers "what is there" and never "what is moving". Every mart
dashboard must therefore open onto time series that a reader can scan for
trends, at every dimension that plausibly carries one.

A mart's presentation lives in one place, `config.meta.vintage.visualization`
in the family's dbt YAML. `visualization/bin/viz content` renders it into
`transform/lightdash/`. Never hand-edit generated chart or dashboard YAML;
`viz content` overwrites it and `viz content --check` fails the diff.

## What a finished dashboard contains

Per mart, in dashboard order:

1. a context tile — model description, grain, the `analysis.headline`, window
   and provenance;
2. `analysis.trends` — the time series, two half-width tiles per row or one
   full-width tile;
3. the ranked comparison over the recent window, and the bounded record table.

Plus dashboard-level controls: `analysis.controls` become filter chips a reader
can set without editing a chart, and the date-zoom control re-grains every
time-series tile at once.

## The specification

```yaml
vintage:
  visualization:
    dimension: category          # ranked comparison, recent window
    metric: distinct_entities
    time: observed_at            # collection time: when the extractor fetched
    window_days: 30
    title: ...
    description: ...
    detail_fields: [...]
    sources: [...]
    analysis:
      headline: what a reader learns by watching this mart move.
      event_time: crime_month    # default time axis for trends
      grain: MONTH               # default grain for trends
      controls: [category, location_type]
      trends:
      - slug: monthly-volume     # chart slug suffix, unique per mart
        kind: total              # total | breakdown | composition
        metric: distinct_entities
        time: crime_month
        grain: MONTH             # HOUR DAY WEEK MONTH QUARTER YEAR
        title: Crimes recorded per month
        description: what the line measures and how to misread it.
        width: half              # half | full
        window_days: 1095        # omit for full published history
      - slug: by-category
        kind: breakdown
        breakdown: category
        top_n: 6
        ...
```

`viz validate` rejects an unusable specification and reports a mart with no
trends, no dimensional trend, or a collection-time-only axis as an analysis
**gap**. Gaps are scheduled work; they do not block deployment. Contract
breaches — unknown fields, undeclared grains, a trend that is not on a
dashboard — are **issues** and do block it.

## Chart kinds

Only shapes verified to render in the pinned Lightdash are available.

| kind | shape | use for |
| --- | --- | --- |
| `total` | line, no pivot | one series: the overall level or flow |
| `breakdown` | line pivoted on `breakdown`, top `top_n` series | comparing the largest categories' trajectories |
| `composition` | bar pivoted on `breakdown`, stacked | how the mix shifts, and sparse or bursty series |

`breakdown` sorts by the metric descending, so `columnLimit` keeps the largest
series rather than an alphabetical prefix. Unstacked `area` and stacked
`line`/`area` drop Lightdash's auto-expanded pivot series and render an empty
plot; they are rejected by the generator.

Choose `composition` when the data arrives in bursts. A line across months
where most months are empty reads as a flat zero with a spike; bars read
correctly.

## Choosing the time axis

Most marts carry two kinds of timestamp and they mean different things.

* **Collection time** (`observed_at`, `fetched_at`) is when this project
  fetched the row. History starts when collection started — days, not years.
  It is the correct axis for a *level*: how large the published catalogue is,
  how many stations are reporting, what the current queue looks like. It is
  also the only axis for gauges the publisher does not timestamp.
* **Event time** (`crime_month`, `published_at`, `posted_at`, `rating_date`,
  `week_start_date`) is when the thing happened. It usually reaches back years
  and is the correct axis for a *flow*: publications per month, postings per
  week, cases per epidemiological week.

Prefer event time for flows. `viz validate` raises a gap when a mart has a
non-lineage date dimension and no trend uses it. Never build a trend on
`source_loaded_at`, `extract_started_at`, `_dt` or `source_date`: those measure
the collector, not the source.

Pick the grain from the observed span and density, not by habit: enough points
to see a shape, few enough that each point is populated. Roughly 20–200 points.
Check it — `distinct_months` and `distinct_days` from `eda profile` say whether
a MONTH grain has 3 points or 300.

## Readability with many categories

A pivoted chart with forty series is unreadable. In order of preference:

1. `top_n` between 4 and 8 for `breakdown`, up to 12 for `composition`, and say
   in the description that the remainder is excluded rather than aggregated;
2. a coarser breakdown dimension if one exists (region rather than station);
3. a `composition` bar, where many thin segments still show the mix;
4. `analysis.controls`, so a reader narrows to the categories they care about;
5. separate trends per major grouping when the dimension has a handful of
   genuinely different populations.

Do not solve high cardinality by widening `top_n` past 20 or by charting a
dimension whose values are identifiers.

## Metrics

Two metrics per mart is the floor: a count of things and at least one measure
that can move independently of it. Levels and flows both matter — a registry
whose feature count is flat while its distinct-artist count climbs is a real
finding.

Use `count_distinct` on the entity key for a count that repeated snapshots
cannot inflate. Use `average`, `min`, `max` or `percentile` for reported
measures. Never `sum` a snapshot level, a balance, a rate, or a mixed-currency
amount; never sum a value the publisher republishes each poll.

Observation-weighted averages are biased by polling frequency. Say so in the
metric description when the source is polled irregularly.

## Writing descriptions

Every trend needs a description that states what is counted, over what axis and
window, and the specific way a reader could misread it. "Distinct `feature_id`
values present in the feed on each observation day. This is a level rather than
a flow — the feed republishes its whole catalogue, so the line moves only when
the publisher adds or withdraws a record." Not "features over time".

Absent data is absent, not zero. Say which.

Keep `: ` out of unquoted YAML prose; it is a mapping delimiter. Use an em dash.

## Doing the work

```bash
visualization/bin/eda profile fct_<mart>          # rows, nulls, cardinality, date spans
visualization/bin/eda query "select ..."          # read-only, one bounded SELECT
visualization/bin/viz check-analysis \
  transform/models/marts/<family>/_<family>_models.yml   # offline; the inner loop
$EDITOR transform/models/marts/<family>/_<family>_models.yml
transform/bin/dbt parse --target dev              # metrics and analysis reach the manifest
visualization/bin/viz content                     # regenerate charts and dashboards
visualization/bin/viz validate --json             # issues must be empty; gaps must shrink
```

An operator then runs `viz deploy` and `viz query-check`, and reviews the
dashboard in Lightdash. A trend that renders empty in the browser is not done,
whatever validation says.

`eda profile` runs one pass over the table. Above 200,000 rows it aggregates a
head sample and says so in `sampled_rows` and `note`; a sampled span is a lower
bound on the published history, so confirm the axis span and any breakdown
cardinality that decides a `top_n` with an explicit `eda query`.

## Acceptance

A mart is finished when all of the following hold.

- `viz validate --json` reports no `issues` and no `gaps` for it.
- Every declared trend renders a visible series in Lightdash, or its
  description states why the source is empty for the charted window.
- At least one trend uses source event time when the mart has one.
- Every trend's grain yields a shape, not two points or three hundred spikes.
- Every chart with a breakdown is legible: bounded series count, legend beside
  the plot, largest series kept.
- Descriptions name the misreading. Levels are not described as flows.
- `analysis.controls` lists the dimensions a reader would slice by.
- Metric semantics are honest: nothing summed that must not be summed.
