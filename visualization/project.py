"""Read the single dbt project; produce portable BI artifacts, never SQL rewrites."""
from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import re

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTENT = ROOT / "transform" / "lightdash"

# Grains Lightdash exposes as date-truncated dimension variants, coarsest last.
# Ordering is the date-zoom control order and the trend-grain sort order.
GRAIN_INTERVALS = ("HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR")
# Verified to render in the pinned Lightdash: `total` is an unpivoted line,
# `breakdown` a pivoted line bounded by columnLimit, `composition` a stacked
# bar. Unstacked `area` and stacked `line`/`area` drop auto-expanded pivot
# series and are therefore not offered.
TREND_KINDS = ("total", "breakdown", "composition")
TOP_N_DEFAULTS = {"breakdown": 6, "composition": 8}
# Extractor and loader bookkeeping timestamps. They describe when this project
# fetched a row, never when the underlying event happened, so a trend built on
# them measures the collector rather than the source.
LINEAGE_TIMES = ("source_loaded_at", "extract_started_at", "_extract_started_at", "_dt", "source_date")


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def mart_nodes(manifest: dict) -> dict:
    if not isinstance(manifest.get("nodes"), dict):
        raise ValueError("dbt manifest is missing nodes")
    return {
        uid: node for uid, node in sorted(manifest["nodes"].items())
        if node.get("resource_type") == "model"
        and node.get("package_name") == "vintage_data"
        and node.get("original_file_path", "").startswith("models/marts/")
        and node.get("config", {}).get("enabled", True)
    }


def node_from_yaml(model: dict, path: str = "models/marts/family/model.sql") -> dict:
    """Shape a dbt schema-YAML model like a manifest node, for offline checks."""
    node = {"name": model.get("name"), "description": model.get("description"),
            "original_file_path": path, "config": model.get("config", {}), "columns": {}}
    for column in model.get("columns") or []:
        node["columns"][column.get("name")] = column
    return node




def check_family(path: pathlib.Path) -> dict:
    """Validate the reviewed analysis specification in one family YAML.

    Offline: no dbt manifest, no warehouse, no credentials. Catches an unusable
    trend specification in a second instead of after a parse and a render.
    """
    document = yaml.safe_load(path.read_text()) or {}
    results = []
    for model in document.get("models") or []:
        node = node_from_yaml(model, f"models/marts/{path.parent.name}/{model.get('name')}.sql")
        entry = {"model": node["name"], "errors": [], "gaps": [], "trends": []}
        try:
            trends = trend_specs(node)
            entry["trends"] = [{"slug": trend["slug"], "kind": trend["kind"], "metric": trend["metric"],
                                "time": trend["time"], "grain": trend["grain"], "top_n": trend["top_n"],
                                "width": trend["width"], "window_days": trend["window_days"]} for trend in trends]
            entry["gaps"] = analysis_gaps(node, trends)
        except ValueError as error:
            entry["errors"].append(str(error))
        for key in metric_collisions(node):
            entry["errors"].append(f"metric {key!r} collides with a dimension of the same name;"
                                   " Lightdash keeps the dimension and drops the metric")
        results.append(entry)
    return {"path": str(path), "models": results,
            "ok": bool(results) and not any(item["errors"] or item["gaps"] for item in results)}


def source_ancestors(manifest: dict, uid: str) -> set[str]:
    sources = manifest.get("sources", {})
    found, seen, pending = set(), set(), [uid]
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        if current in sources:
            if sources[current].get("source_name") == "raw":
                found.add(sources[current]["name"])
        else:
            pending.extend(manifest["nodes"].get(current, {}).get("depends_on", {}).get("nodes", []))
    return found


def metadata(node: dict) -> dict:
    return {**node.get("meta", {}), **node.get("config", {}).get("meta", {})}


def pg_type(value: str) -> str:
    value = value.lower().strip()
    types = {"varchar": "text", "text": "text", "bigint": "bigint", "integer": "integer",
             # DuckDB permits non-standard numeric values such as NaN inside
             # JSON. Preserve its serialized form instead of rejecting a mart.
             "smallint": "smallint", "boolean": "boolean", "date": "date", "json": "text",
             "double": "double precision", "double precision": "double precision",
             "float": "real", "real": "real", "timestamp": "timestamp without time zone",
             "timestamp without time zone": "timestamp without time zone",
             "timestamp with time zone": "timestamp with time zone"}
    if value in types:
        return types[value]
    if re.fullmatch(r"(?:decimal|numeric)\(\d+,\s*\d+\)", value):
        return value.replace("decimal", "numeric").replace(" ", "")
    raise ValueError(f"unsupported serving type {value!r}")


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", value) or len(value.encode()) > 63:
        raise ValueError(f"invalid or overlong database identifier {value!r}")
    return '"' + value + '"'


def columns(node: dict) -> list[dict]:
    return [{"name": name, "type": pg_type(col["data_type"])} for name, col in node["columns"].items()]


def interval_dimensions(col: dict) -> dict[str, list[str]]:
    """Short-key additional dimensions the column offers, with their grains.

    Lightdash applies a semantic dimension alias to the base field only: the
    interval variants it derives from a column keep the physical column name,
    so a long column on a long mart produces a field id past PostgreSQL's
    63-byte identifier limit and the query is refused outright. An
    `additional_dimensions` entry is named from its own key, which is the only
    supported way to shorten an interval field id without renaming the model.
    """
    offered = {}
    for key, extra in (metadata(col).get("additional_dimensions") or {}).items():
        if str(extra.get("type")) in ("date", "timestamp"):
            offered[key] = [str(value).upper() for value in extra.get("time_intervals") or []]
    return offered


def field_ids(node: dict) -> set[str]:
    name = node["name"]
    ids = {f"{name}_{key}" for key in metadata(node).get("metrics", {})}
    for key, col in node.get("columns", {}).items():
        meta = metadata(col)
        ids.update(f"{name}_{metric}" for metric in meta.get("metrics", {}))
        dimension = meta.get("dimension", {})
        ids.add(f"{name}_{dimension.get('name', key)}")
        variants = {key: [str(value).upper() for value in dimension.get("time_intervals") or []]} \
            if dimension.get("type") in ("date", "timestamp") else {}
        variants.update(interval_dimensions(col))
        for base, declared in variants.items():
            ids.add(f"{name}_{base}")
            # Lightdash derives only the intervals the dimension declares.
            ids.update(f"{name}_{base}_{interval.lower()}"
                       for interval in GRAIN_INTERVALS if interval in declared)
    return ids


def dimension_id(node: dict, column: str) -> str:
    alias = metadata(node["columns"][column]).get("dimension", {}).get("name", column)
    return f"{node['name']}_{alias}"


def time_field_id(node: dict, column: str, grain: str, dimension: str | None = None) -> str:
    """Interval variants are named from the dimension key, never from an alias."""
    return f"{node['name']}_{dimension or column}_{grain.lower()}"


def metric_names(node: dict) -> dict[str, dict]:
    names = dict(metadata(node).get("metrics") or {})
    for col in node.get("columns", {}).values():
        names.update(metadata(col).get("metrics") or {})
    return names


def metric_collisions(node: dict) -> list[str]:
    """Metric keys that a dimension on the same mart already occupies.

    Lightdash derives both field ids as `<table>_<name>`, and on a collision it
    keeps the dimension and silently drops the metric, so every chart that
    charts it fails at query time with a missing field.
    """
    names = set(node.get("columns", {}))
    names.update(metadata(col).get("dimension", {}).get("name", key)
                 for key, col in node.get("columns", {}).items())
    return sorted(key for key in metric_names(node) if key in names)


def analytic_times(node: dict) -> list[str]:
    """Declared date dimensions that carry source event semantics."""
    collection = (metadata(node).get("vintage", {}).get("visualization") or {}).get("time")
    return [name for name, col in node.get("columns", {}).items()
            if metadata(col).get("dimension", {}).get("type") in ("date", "timestamp")
            and not metadata(col).get("dimension", {}).get("hidden")
            and name not in LINEAGE_TIMES and name != collection]


def trend_specs(node: dict) -> list[dict]:
    """Normalise and validate `analysis.trends`; raise on an unusable specification."""
    name = node["name"]
    spec = (metadata(node).get("vintage", {}).get("visualization") or {})
    analysis = spec.get("analysis") or {}
    if not analysis:
        return []
    metrics, columns_meta, slugs, results = metric_names(node), node.get("columns", {}), set(), []

    def fail(message: str):
        raise ValueError(f"{name}: {message}")

    default_time, default_grain = analysis.get("event_time", spec.get("time")), analysis.get("grain", "MONTH")
    if not analysis.get("headline"):
        fail("analysis.headline must state what changes over time")
    for entry in analysis.get("trends") or []:
        trend = dict(entry)
        slug = str(trend.get("slug", ""))
        if not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", slug) or slug in slugs or slug == "details":
            fail(f"trend slug {slug!r} must be a unique lowercase-hyphen token and not 'details'")
        slugs.add(slug)
        kind = trend.setdefault("kind", "total")
        if kind not in TREND_KINDS:
            fail(f"{slug}: kind must be one of {TREND_KINDS}")
        metric = trend.setdefault("metric", spec.get("metric"))
        if metric not in metrics:
            fail(f"{slug}: metric {metric!r} is not defined on the mart")
        time = trend.setdefault("time", default_time)
        column = columns_meta.get(time) or fail(f"{slug}: time column {time!r} is not a mart column")
        dimension = metadata(column).get("dimension", {})
        if dimension.get("type") not in ("date", "timestamp"):
            fail(f"{slug}: time column {time!r} is not a date or timestamp dimension")
        grain = str(trend.setdefault("grain", default_grain)).upper()
        trend["grain"] = grain
        if grain not in GRAIN_INTERVALS:
            fail(f"{slug}: grain must be one of {GRAIN_INTERVALS}")
        variants = interval_dimensions(column)
        source = trend.get("time_dimension")
        if source is None:
            declared = [str(value).upper() for value in dimension.get("time_intervals") or []]
        elif source in variants:
            declared = variants[source]
        else:
            fail(f"{slug}: time_dimension {source!r} is not an additional_dimensions key on {time!r}")
        trend["time_dimension"] = source
        if grain not in declared:
            fail(f"{slug}: {source or time!r} does not declare time_intervals entry {grain}")
        if dimension.get("type") == "date" and "HOUR" in declared + [grain]:
            fail(f"{slug}: date column {time!r} cannot be grained by HOUR")
        trend["intervals"] = [value for value in GRAIN_INTERVALS if value in declared]
        breakdown = trend.get("breakdown")
        if kind == "total" and breakdown:
            fail(f"{slug}: kind 'total' does not take a breakdown")
        if kind != "total":
            if not breakdown:
                fail(f"{slug}: kind {kind!r} requires a breakdown dimension")
            if breakdown not in columns_meta:
                fail(f"{slug}: breakdown {breakdown!r} is not a mart column")
            if metadata(columns_meta[breakdown]).get("dimension", {}).get("type") in ("date", "timestamp"):
                fail(f"{slug}: breakdown {breakdown!r} is a date dimension; break down by a category")
        top_n = int(trend.setdefault("top_n", TOP_N_DEFAULTS.get(kind, 0)) or 0)
        if kind != "total" and not 2 <= top_n <= 20:
            fail(f"{slug}: top_n must be between 2 and 20 to stay readable")
        trend["top_n"] = top_n
        width = trend.setdefault("width", "full" if kind != "total" else "half")
        if width not in ("half", "full"):
            fail(f"{slug}: width must be 'half' or 'full'")
        window = trend.get("window_days", analysis.get("window_days"))
        trend["window_days"] = None if window is None else int(window)
        if trend["window_days"] is not None and trend["window_days"] < 1:
            fail(f"{slug}: window_days must be a positive number of days or omitted for full history")
        snapshot = trend.get("snapshot_days", analysis.get("snapshot_days"))
        if snapshot is None:
            trend["snapshot"] = None
        elif not spec.get("time"):
            fail(f"{slug}: snapshot_days needs a collection time field on the visualization spec")
        elif int(snapshot) < 1:
            fail(f"{slug}: snapshot_days must be a positive number of days")
        else:
            # Bounds which republished snapshots the trend counts, so a
            # catalogue mart can trend over event time without scanning and
            # de-duplicating every poll it has ever recorded.
            trend["snapshot"] = (spec["time"], int(snapshot))
        for key in ("title", "description"):
            if not str(trend.get(key, "")).strip():
                fail(f"{slug}: {key} is required so the chart states its own reading")
        trend["time_label"] = trend.get("time_label") or f"{dimension.get('label', time)} ({grain.title()})"
        trend["metric_label"] = trend.get("metric_label") or metrics[metric].get("label", metric)
        # Pivoted rows multiply by breakdown cardinality, so keep a headroom
        # limit rather than truncating the series that carry the trend.
        trend["limit"] = int(trend.get("limit", 5000))
        results.append(trend)
    for field in analysis.get("controls") or []:
        if field not in columns_meta:
            fail(f"control {field!r} is not a mart column")
        if metadata(columns_meta[field]).get("dimension", {}).get("hidden"):
            fail(f"control {field!r} is a hidden dimension")
    return results


def analysis_gaps(node: dict, trends: list[dict]) -> list[str]:
    """Analysis-completeness deficits. Work to schedule, not a contract breach."""
    spec = (metadata(node).get("vintage", {}).get("visualization") or {})
    analysis = spec.get("analysis") or {}
    gaps = []
    if not trends:
        return ["no time series: add config.meta.vintage.visualization.analysis.trends"]
    kinds = {trend["kind"] for trend in trends}
    if "total" not in kinds:
        gaps.append("no overall trend: add a kind 'total' time series")
    if not kinds & {"breakdown", "composition"}:
        gaps.append("no dimensional trend: add a kind 'breakdown' or 'composition' time series")
    available = analytic_times(node)
    if available and not ({trend["time"] for trend in trends} & set(available)):
        gaps.append(f"trends use collection time only; source event time available: {', '.join(sorted(available)[:6])}")
    analytic = [key for key in metric_names(node) if key != "latest_load"]
    if len(analytic) < 2:
        gaps.append("one analytic metric only: add a second measure so trends compare levels and mix")
    if len({trend["metric"] for trend in trends}) < min(2, len(analytic)):
        gaps.append("trends chart a single metric; chart each analytic metric that changes over time")
    if not analysis.get("controls"):
        gaps.append("no dashboard controls: list analysis.controls dimensions viewers should slice by")
    return gaps


def coverage(manifest: dict, content: pathlib.Path = CONTENT) -> dict:
    charts, dashboards, errors = {}, {}, []
    for kind, target in (("charts", charts), ("dashboards", dashboards)):
        for path in sorted((content / kind).glob("*.yml")):
            value = yaml.safe_load(path.read_text())
            slug = value.get("slug")
            if not slug or slug in target:
                errors.append(f"{path.name}: missing or duplicate slug")
            target[slug] = value
    memberships = {}
    for slug, dashboard in dashboards.items():
        for tile in dashboard.get("tiles", []):
            if tile.get("type") == "saved_chart":
                chart = tile["properties"].get("chartSlug")
                memberships.setdefault(chart, []).append(slug)
                if chart not in charts:
                    errors.append(f"{slug}: missing chart {chart}")
    items = []
    marts = mart_nodes(manifest)
    names = {node["name"] for node in marts.values()}
    for slug, chart in charts.items():
        if chart.get("tableName") not in names:
            errors.append(f"{slug}: unknown mart {chart.get('tableName')}")
    for uid, node in marts.items():
        issues, gaps = [], []
        meta = metadata(node)
        if not meta.get("label") or not meta.get("grain") or not node.get("description"):
            issues.append("missing model metadata")
        if not meta.get("metrics") and not any(metadata(c).get("metrics") for c in node.get("columns", {}).values()):
            issues.append("missing metrics")
        for name, col in node.get("columns", {}).items():
            if not col.get("description") or not metadata(col).get("dimension", {}).get("type"):
                issues.append(f"missing field metadata: {name}")
        try:
            trends = trend_specs(node)
            gaps = analysis_gaps(node, trends)
        except ValueError as error:
            issues.append(f"invalid analysis specification: {error}")
            trends = []
        for key in metric_collisions(node):
            issues.append(f"metric {key!r} collides with a dimension of the same name;"
                          " Lightdash keeps the dimension and drops the metric")
        linked = {slug: chart for slug, chart in charts.items() if chart.get("tableName") == node["name"]}
        meaningful = [slug for slug, chart in linked.items() if chart.get("chartConfig", {}).get("type") != "table" and memberships.get(slug)]
        if not meaningful:
            issues.append("missing dashboard visualization")
        for trend in trends:
            slug = f"{node['name'].replace('_', '-')}-{trend['slug']}"
            if slug not in memberships:
                issues.append(f"{slug}: declared trend is not on a dashboard")
        valid = field_ids(node)
        for slug, chart in linked.items():
            query = chart.get("metricQuery", {})
            refs = query.get("dimensions", []) + query.get("metrics", [])
            refs += [sort["fieldId"] for sort in query.get("sorts", [])]
            def filter_fields(value):
                if isinstance(value, dict):
                    if isinstance(value.get("target"), dict) and value["target"].get("fieldId"):
                        yield value["target"]["fieldId"]
                    for child in value.values():
                        yield from filter_fields(child)
                elif isinstance(value, list):
                    for child in value:
                        yield from filter_fields(child)
            refs += list(filter_fields(query.get("filters", {})))
            if query.get("exploreName") != node["name"]:
                issues.append(f"{slug}: wrong explore")
            for field in refs:
                if field not in valid:
                    issues.append(f"{slug}: unknown field {field}")
        items.append({"unique_id": uid, "name": node["name"], "family": pathlib.PurePosixPath(node["original_file_path"]).parts[2],
                      "sources": sorted(source_ancestors(manifest, uid)), "issues": issues, "gaps": gaps,
                      "trend_count": len(trends),
                      "charts": sorted(linked), "dashboards": sorted({d for c in linked for d in memberships.get(c, [])})})
    return {"schema_version": 2, "mart_count": len(items), "covered_count": sum(not i["issues"] for i in items),
            "trend_covered_count": sum(not i["gaps"] and not i["issues"] for i in items),
            "trend_chart_count": sum(i["trend_count"] for i in items),
            "gap_count": sum(len(i["gaps"]) for i in items),
            "chart_count": len(charts), "dashboard_count": len(dashboards), "errors": errors,
            "ok": bool(items) and not errors and all(not i["issues"] for i in items), "models": items}


def bundle(manifest: dict, destination: pathlib.Path, database: str, schema: str = "transform_marts") -> dict:
    identifier(database); identifier(schema)
    result = copy.deepcopy(manifest)
    result["metadata"]["adapter_type"] = "postgres"
    marts = mart_nodes(result)
    # Keep the complete source manifest separately for provenance. Only marts
    # are models in the serving artifact, so Lightdash cannot expose raw views.
    result["nodes"] = marts
    for node in marts.values():
        alias = node.get("alias") or node["name"]
        node.update(database=database, schema=schema, relation_name=f"{identifier(database)}.{identifier(schema)}.{identifier(alias)}", compiled=True)
        node.setdefault("config", {}).setdefault("meta", {}).update(metadata(node))
        node["config"]["meta"]["sql_from"] = f"{identifier(schema)}.{identifier(alias)}"
        node["meta"] = node["config"]["meta"]
        for col in node["columns"].values():
            col["data_type"] = pg_type(col["data_type"])
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "target").mkdir(exist_ok=True)
    (destination / "source-manifest.json").write_text(json.dumps(manifest))
    (destination / "target" / "manifest.json").write_text(json.dumps(result))
    (destination / "dbt_project.yml").write_text(yaml.safe_dump({"name": "vintage_data", "version": "1.0.0", "config-version": 2, "profile": "vintage_serving"}))
    profile = {"type": "postgres", "host": "{{ env_var('LIGHTDASH_PG_HOST', '127.0.0.1') }}",
               "port": "{{ env_var('LIGHTDASH_PG_PORT', '5433') | int }}", "user": "mart_reader",
               "password": "{{ env_var('LIGHTDASH_READER_PASSWORD') }}", "dbname": database, "schema": schema, "threads": 1, "sslmode": "disable"}
    # Lightdash renders templates before parsing YAML: preserve literal single
    # quotes inside env_var expressions by using JSON-compatible double quotes.
    (destination / "profiles.yml").write_text(json.dumps({"vintage_serving": {"target": "serving", "outputs": {"serving": profile}}}, indent=2))
    info = {"schema_version": 1, "source_manifest_sha256": digest(manifest), "mart_count": len(marts), "database": database, "schema": schema}
    (destination / "bundle.json").write_text(json.dumps(info, indent=2) + "\n")
    return info
