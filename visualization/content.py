"""Render native Lightdash YAML from each mart's reviewed presentation metadata.

Two presentation layers exist per mart:

* the census layer (`dimension`/`metric`/`detail_fields`) — a ranked comparison
  over a bounded collection window plus a bounded record table;
* the trend layer (`analysis.trends`) — time series over the mart's analytic
  time axis, which is normally the source's own event time rather than the
  extractor's observation time.

The trend layer is the point of the project, so `visualization/project.py`
reports its absence as an analysis gap. Only chart shapes verified to render in
the pinned Lightdash are emitted: `line` and `bar` series, `bar` with layout
stacking, and pivoted breakdowns bounded by `columnLimit`. Unstacked `area` and
stacked `line`/`area` do not render auto-expanded pivot series and are rejected.
"""
from __future__ import annotations

import json
import pathlib
import uuid

import yaml

from visualization.project import (CONTENT, ROOT, GRAIN_INTERVALS, TREND_KINDS, dimension_id,
                                   mart_nodes, metadata, time_field_id, trend_specs)

GRID = 36
HALF = GRID // 2
TILE_HEIGHT = 10
CONTEXT_HEIGHT = 6
HEADING_HEIGHT = 1


def stable_id(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "vintage-data/lightdash/" + name))


def window_filter(node: dict, slug: str, field: str, days: int | None, extra: list[dict],
                  *, snapshot: tuple[str, int] | None = None) -> dict:
    """Bounded UTC window plus reviewed constant filters, or no window when days is None.

    `snapshot` additionally bounds the extractor's collection time. A mart that
    republishes its whole catalogue on every poll accumulates one copy of each
    record per poll, so a trend over source event time must restrict which
    snapshots it counts or it scans and de-duplicates the entire history.
    """
    rules = []
    if days is not None:
        rules.append({"id": stable_id(slug + "/recent"), "target": {"fieldId": dimension_id(node, field)},
                      "operator": "inThePast", "values": [days],
                      "settings": {"unitOfTime": "days", "completed": False}})
    if snapshot is not None:
        column, snapshot_days = snapshot
        rules.append({"id": stable_id(slug + "/snapshot"), "target": {"fieldId": dimension_id(node, column)},
                      "operator": "inThePast", "values": [snapshot_days],
                      "settings": {"unitOfTime": "days", "completed": False}})
    for index, rule in enumerate(extra):
        rules.append({"id": stable_id(f"{slug}/filter/{index}"), "operator": "equals",
                      "target": {"fieldId": dimension_id(node, rule["field"])}, "values": rule["values"]})
    if not rules:
        return {}
    return {"dimensions": {"id": stable_id(slug + "/filters"), "and": rules}}


def series(kind: str, x: str, y: str) -> dict:
    # Lightdash expands one prototype series across every pivot value, so a
    # single entry is enough and no pivot value is hard-coded into content.
    return {"type": kind, "yAxisIndex": 0, "encode": {"xRef": {"field": x}, "yRef": {"field": y}}}


def trend_chart(node: dict, slug: str, spec: dict, extra_filters: list[dict]) -> dict:
    name = node["name"]
    kind = spec["kind"]
    metric = f"{name}_{spec['metric']}"
    breakdown = dimension_id(node, spec["breakdown"]) if spec.get("breakdown") else None
    # A trend may carry its own constant filters, replacing the shared ones,
    # so one trajectory can keep a predicate the other charts must not have.
    trend_filters = spec.get("filters", extra_filters)
    x = time_field_id(node, spec["time"], spec["grain"], spec.get("time_dimension"))
    dimensions = [x] + ([breakdown] if breakdown else [])
    sorts = [{"fieldId": x, "descending": False}]
    if breakdown:
        # Rank pivot columns by size first so columnLimit keeps the largest
        # series; the x axis is ordered by Lightdash regardless of row order.
        sorts = [{"fieldId": metric, "descending": True}, {"fieldId": x, "descending": False}]
    layout = {"xField": x, "yField": [metric]}
    config = {"layout": layout, "eChartsConfig": {
        "series": [series("bar" if kind == "composition" else "line", x, metric)],
        "xAxis": [{"name": spec["time_label"]}], "yAxis": [{"name": spec["metric_label"]}]}}
    if kind == "composition":
        layout["stack"] = True
    if breakdown:
        config["columnLimit"] = spec["top_n"]
        config["eChartsConfig"]["legend"] = {"placement": "outsideRight", "orient": "vertical"}
    chart = {"name": spec["title"], "description": spec["description"], "tableName": name,
             "slug": slug, "spaceSlug": "vintage-marts", "version": 1,
             "metricQuery": {"exploreName": name, "dimensions": dimensions, "metrics": [metric],
                             "filters": window_filter(node, slug, spec["time"], spec["window_days"], trend_filters,
                                                      snapshot=spec.get("snapshot")),
                             "sorts": sorts, "limit": spec["limit"], "tableCalculations": []},
             "chartConfig": {"type": "cartesian", "config": config},
             "tableConfig": {"columnOrder": dimensions + [metric]}}
    if breakdown:
        chart["pivotConfig"] = {"columns": [breakdown]}
    return chart


def render(manifest: dict) -> dict[str, dict]:
    outputs, dashboards = {}, {}
    for node in mart_nodes(manifest).values():
        name = node["name"]
        meta = metadata(node)
        spec = meta.get("vintage", {}).get("visualization")
        if not spec:
            raise ValueError(f"{name}: add reviewed config.meta.vintage.visualization before rendering")
        family = pathlib.PurePosixPath(node["original_file_path"]).parts[2]
        family_slug = family.replace("_", "-")
        slug = name.replace("_", "-")
        space = "vintage-marts"
        dimension = dimension_id(node, spec["dimension"])
        metric = f"{name}_{spec['metric']}"
        window_days = spec.get("window_days", 30)
        extra_filters = spec.get("filters", [])
        filters = window_filter(node, slug, spec["time"], window_days, extra_filters)
        description = spec["description"]
        query = {"exploreName": name, "dimensions": [dimension], "metrics": [metric], "filters": filters,
                 "sorts": [{"fieldId": metric, "descending": True}], "limit": 20, "tableCalculations": []}
        outputs[f"charts/{slug}.yml"] = {
            "name": spec["title"], "description": description, "tableName": name,
            "slug": slug, "spaceSlug": space, "version": 1, "metricQuery": query,
            "chartConfig": {"type": "cartesian", "config": {
                "layout": {"xField": dimension, "yField": [metric], "flipAxes": True},
                "eChartsConfig": {"series": [series("bar", dimension, metric)]}}},
            "tableConfig": {"columnOrder": [dimension, metric]}}
        trends = trend_specs(node)
        for trend in trends:
            outputs[f"charts/{slug}-{trend['slug']}.yml"] = trend_chart(node, f"{slug}-{trend['slug']}", trend, extra_filters)
        detail_fields = [dimension_id(node, column) for column in spec["detail_fields"]]
        outputs[f"charts/{slug}-details.yml"] = {
            "name": meta["label"] + " — observations", "description": node["description"],
            "tableName": name, "slug": slug + "-details", "spaceSlug": space, "version": 1,
            "metricQuery": {"exploreName": name, "dimensions": detail_fields, "metrics": [], "filters": filters,
                            "sorts": [{"fieldId": dimension_id(node, spec["time"]), "descending": True}],
                            "limit": 100, "tableCalculations": []},
            "chartConfig": {"type": "table", "config": {}}, "tableConfig": {"columnOrder": detail_fields}}
        dashboard = dashboards.setdefault(family_slug, {
            "name": family.replace("_", " ").title(), "slug": family_slug, "spaceSlug": space, "version": 1,
            "tabs": [], "tiles": [], "config": {"isDateZoomDisabled": True},
            "description": "Source-family trends over time. Observation windows and aggregation notes are shown with each chart.",
            "filters": {"dimensions": [], "metrics": [], "tableCalculations": []}})
        y = max((tile["y"] + tile["h"] for tile in dashboard["tiles"]), default=0)
        provenance = "\n".join(f"- {source}" for source in spec.get("sources", []))
        analysis = meta.get("vintage", {}).get("visualization", {}).get("analysis") or {}
        headline = f"**What changes here:** {analysis['headline']}\n\n" if analysis.get("headline") else ""
        dashboard["tiles"].append({"type": "markdown", "x": 0, "y": y, "w": GRID, "h": CONTEXT_HEIGHT,
            "properties": {"title": meta["label"], "hideFrame": False, "content":
                f"{node['description']}\n\n**Grain:** {meta['grain']}\n\n{headline}"
                f"**Window:** ranked and record tiles cover the past {window_days} days of `{spec['time']}` (UTC);"
                f" trend tiles state their own axis and window. Empty charts mean no matching published observations;"
                f" they do not establish that no events occurred.\n\n**Provenance**\n{provenance}"}})
        y += CONTEXT_HEIGHT
        if trends:
            dashboard["tiles"].append({"type": "heading", "x": 0, "y": y, "w": GRID, "h": HEADING_HEIGHT,
                "properties": {"text": f"{meta['label']} — change over time", "showDivider": True}})
            y += HEADING_HEIGHT
            column = 0
            for trend in trends:
                width = GRID if trend["width"] == "full" else HALF
                if column and column + width > GRID:
                    column, y = 0, y + TILE_HEIGHT
                chart = outputs[f"charts/{slug}-{trend['slug']}.yml"]
                dashboard["tiles"].append({"type": "saved_chart", "x": column, "y": y, "w": width, "h": TILE_HEIGHT,
                    "properties": {"title": chart["name"], "hideTitle": False,
                                   "chartName": chart["name"], "chartSlug": chart["slug"]}})
                column += width
                if column >= GRID:
                    column, y = 0, y + TILE_HEIGHT
            if column:
                y += TILE_HEIGHT
            # One date-zoom control re-grains every tile, so offer only grains
            # that every trend time column on this dashboard declares.
            offered = set.intersection(*(set(trend["intervals"]) for trend in trends)) & set(GRAIN_INTERVALS)
            offered &= set(dashboard.setdefault("_grains", offered))
            dashboard["_grains"] = offered
            default = trends[0]["grain"] if trends[0]["grain"] in offered else next(
                (grain for grain in GRAIN_INTERVALS if grain in offered), None)
            dashboard["config"] = {"isDateZoomDisabled": not offered,
                                   "dateZoomGranularities": [g.title() for g in GRAIN_INTERVALS if g in offered]}
            if default:
                dashboard["config"]["defaultDateZoomGranularity"] = default.title()
            dashboard["tiles"].append({"type": "heading", "x": 0, "y": y, "w": GRID, "h": HEADING_HEIGHT,
                "properties": {"text": f"{meta['label']} — latest window", "showDivider": True}})
            y += HEADING_HEIGHT
        for chart_slug, width, column in ((slug, HALF, 0), (slug + "-details", HALF, HALF)):
            chart = outputs[f"charts/{chart_slug}.yml"]
            dashboard["tiles"].append({"type": "saved_chart", "x": column, "y": y, "w": width, "h": TILE_HEIGHT,
                "properties": {"title": chart["name"], "hideTitle": False,
                               "chartName": chart["name"], "chartSlug": chart_slug}})
        for index, field in enumerate(analysis.get("controls", [])):
            field_id = dimension_id(node, field)
            label = metadata(node["columns"][field]).get("dimension", {}).get("label", field)
            dashboard["filters"]["dimensions"].append({
                "target": {"fieldId": field_id, "tableName": name}, "operator": "equals", "values": [],
                "label": f"{meta['label']}: {label}", "disabled": True, "required": False,
                "id": stable_id(f"{slug}/control/{index}")})
    for dashboard in dashboards.values():
        dashboard.pop("_grains", None)
    outputs.update({f"dashboards/{slug}.yml": dashboard for slug, dashboard in dashboards.items()})
    return outputs


def write_content(manifest: dict, *, check=False, destination: pathlib.Path = CONTENT) -> list[str]:
    changed = []
    expected = render(manifest)
    for relative, value in expected.items():
        path = destination / relative
        content = yaml.safe_dump(value, sort_keys=True, allow_unicode=True, width=100)
        if not path.exists() or path.read_text() != content:
            changed.append(relative)
            if not check:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
    for kind in ("charts", "dashboards"):
        directory = destination / kind
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.yml")):
            if f"{kind}/{path.name}" not in expected:
                changed.append(f"{kind}/{path.name} (removed)")
                if not check:
                    path.unlink()
    return changed


def validate_schemas(content: pathlib.Path = CONTENT) -> list[str]:
    import jsonschema
    errors = []
    for kind in ("chart", "dashboard"):
        schema = json.loads((ROOT / "visualization" / "schemas" / f"{kind}-as-code-1.0.json").read_text())
        validator = jsonschema.Draft7Validator(schema)
        for path in sorted((content / f"{kind}s").glob("*.yml")):
            for error in validator.iter_errors(yaml.safe_load(path.read_text())):
                errors.append(f"{path.name}: {list(error.path)}: {error.message}")
    return errors


__all__ = ["GRAIN_INTERVALS", "TREND_KINDS", "render", "stable_id", "validate_schemas", "write_content"]
