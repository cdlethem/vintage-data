"""Strict recipe contracts for the bounded validation lane.

Recipe arguments describe assertions only.  They never contain a command, a SQL
statement, a filesystem path, or a credential reference.  The validation worker
selects a fixed, operator-installed command from its own catalog.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit



def _configured_json(name: str) -> object:
    """Read deployment-owned Airflow configuration without importing worker code."""
    from airflow.configuration import conf
    import json
    raw = conf.get("bot_dashboard", name, fallback="")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise ValidationRecipeError(f"configured {name} is invalid") from exc


def available_capabilities() -> set[str]:
    """Capabilities actually installed for server-side admission, never worker input."""
    value = _configured_json("validation_capabilities")
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValidationRecipeError("configured validation capabilities are invalid")
    allowed = set(RECIPE_CAPABILITIES.values())
    if len(value) != len(set(value)) or not set(value) <= allowed:
        raise ValidationRecipeError("configured validation capabilities are invalid")
    return set(value)


def installed_recipe_catalog() -> dict[str, dict[str, Any]]:
    """Return safe catalog metadata for admission and executive status surfaces.

    Worker argv lives only in the trusted worker runtime.  The provider stores
    this minimal deployment-owned projection so it can reject a gate that no
    installed fixed command could execute.
    """
    value = _configured_json("validation_recipe_catalog") or DEFAULT_RECIPE_CATALOG
    if not isinstance(value, dict) or len(value) > 100:
        raise ValidationRecipeError("configured validation recipe catalog is invalid")
    result: dict[str, dict[str, Any]] = {}
    for command_id, item in value.items():
        if not isinstance(command_id, str) or not _TOKEN.fullmatch(command_id) or not isinstance(item, dict):
            raise ValidationRecipeError("configured validation recipe catalog is invalid")
        if set(item) - {"recipe", "capability", "source_url", "expected_status"}:
            raise ValidationRecipeError("configured validation recipe catalog is invalid")
        recipe, capability = item.get("recipe"), item.get("capability")
        if recipe not in AUTOMATED_RECIPES or capability != RECIPE_CAPABILITIES[recipe]:
            raise ValidationRecipeError("configured validation recipe catalog is invalid")
        if recipe == "public_source_smoke":
            _https(item.get("source_url"), "source_url")
            status = item.get("expected_status")
            if type(status) is not int or not 200 <= status <= 599:
                raise ValidationRecipeError("configured validation recipe catalog is invalid")
        elif "source_url" in item or "expected_status" in item:
            raise ValidationRecipeError("configured validation recipe catalog is invalid")
        result[command_id] = dict(item)
    return result

_HEAD = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_-]{0,99}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)

RECIPE_CAPABILITIES = {
    "public_source_smoke": "public-network-readonly",
    "warehouse_check": "warehouse-readonly",
    "disposable_schema_migration": "disposable-schema-write",
    "dag_inspection": "candidate-tree-readonly",
    "lightdash_preview": "lightdash-readonly",
}

DEFAULT_RECIPE_CATALOG = {
    "smoke-arxiv-new": {"recipe": "public_source_smoke", "capability": "public-network-readonly",
                         "source_url": "https://export.arxiv.org/api/query", "expected_status": 200},
    "smoke-musicbrainz": {"recipe": "public_source_smoke", "capability": "public-network-readonly",
                          "source_url": "https://musicbrainz.org/ws/2/release/", "expected_status": 200},
    "smoke-digitraffic-rail": {"recipe": "public_source_smoke", "capability": "public-network-readonly",
                                "source_url": "https://rata.digitraffic.fi/api/v1/live-trains/station/HKI?minutes_before_departure=0&minutes_after_departure=60&minutes_before_arrival=0&minutes_after_arrival=60", "expected_status": 200},
    "smoke-open-library": {"recipe": "public_source_smoke", "capability": "public-network-readonly",
                           "source_url": "https://openlibrary.org/recentchanges/add-book.json?limit=20", "expected_status": 200},
    "smoke-workday": {"recipe": "public_source_smoke", "capability": "public-network-readonly",
                      "source_url": "https://2020companies.wd1.myworkdayjobs.com/wday/cxs/2020companies/external_careers/jobs", "expected_status": 200},
    "smoke-sensor-community": {"recipe": "public_source_smoke", "capability": "public-network-readonly",
                               "source_url": "https://data.sensor.community/airrohr/v1/filter/country=DE", "expected_status": 200},
}
AUTOMATED_RECIPES = frozenset(RECIPE_CAPABILITIES)


class ValidationRecipeError(ValueError):
    """A gate cannot safely be executed by the validation lane."""


def _token(value: object, name: str) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise ValidationRecipeError(f"{name} must be a lowercase bounded token")
    return value


def _tokens(value: object, name: str, *, minimum: int = 0, maximum: int = 50) -> tuple[str, ...]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise ValidationRecipeError(f"{name} has an invalid number of values")
    result = tuple(_token(item, name) for item in value)
    if len(set(result)) != len(result):
        raise ValidationRecipeError(f"{name} contains duplicate values")
    return result


def _https(value: object, name: str) -> str:
    if not isinstance(value, str) or len(value) > 2048:
        raise ValidationRecipeError(f"{name} must be a bounded HTTPS URL")
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
        raise ValidationRecipeError(f"{name} must be a canonical HTTPS URL")
    return value


def _exact_keys(value: dict[str, Any], required: set[str], optional: set[str] = set()) -> None:
    keys = set(value)
    if keys - required - optional or required - keys:
        raise ValidationRecipeError("recipe arguments do not match the supported recipe contract")


@dataclass(frozen=True)
class RecipeRequest:
    recipe: str
    command_id: str
    required_capability: str
    subject: str
    assertions: dict[str, Any]


def parse_recipe(*, recipe: object, recipe_args: object, required_capability: object, subject: object) -> RecipeRequest:
    """Validate the persisted recipe before it can enter either a queue or worker."""
    if recipe not in AUTOMATED_RECIPES:
        raise ValidationRecipeError("recipe is not automatically executable")
    if not isinstance(subject, str) or not _HEAD.fullmatch(subject):
        raise ValidationRecipeError("validation subject must be an exact Git object ID")
    capability = RECIPE_CAPABILITIES[recipe]
    if required_capability != capability:
        raise ValidationRecipeError("recipe requires a different capability")
    if not isinstance(recipe_args, dict):
        raise ValidationRecipeError("recipe arguments must be an object")

    if recipe == "public_source_smoke":
        _exact_keys(recipe_args, {"command_id", "source_url", "expected_status", "required_record_types"})
        status = recipe_args["expected_status"]
        if type(status) is not int or not 200 <= status <= 599:
            raise ValidationRecipeError("expected_status is outside HTTP bounds")
        assertions = {
            "source_url": _https(recipe_args["source_url"], "source_url"),
            "expected_status": status,
            "required_record_types": _tokens(recipe_args["required_record_types"], "required_record_types", minimum=1, maximum=20),
        }
    elif recipe == "warehouse_check":
        _exact_keys(recipe_args, {"command_id", "expected_relation", "expected_columns"})
        assertions = {
            "expected_relation": _token(recipe_args["expected_relation"], "expected_relation"),
            "expected_columns": _tokens(recipe_args["expected_columns"], "expected_columns", minimum=1),
        }
    elif recipe == "disposable_schema_migration":
        _exact_keys(recipe_args, {"command_id", "expected_revision"})
        assertions = {"expected_revision": _token(recipe_args["expected_revision"], "expected_revision")}
    elif recipe == "dag_inspection":
        _exact_keys(recipe_args, {"command_id", "dag_id", "expected_tasks"})
        assertions = {
            "dag_id": _token(recipe_args["dag_id"], "dag_id"),
            "expected_tasks": _tokens(recipe_args["expected_tasks"], "expected_tasks", minimum=1, maximum=100),
        }
    else:
        _exact_keys(recipe_args, {"command_id", "project_uuid", "explore", "fields"})
        project_uuid = recipe_args["project_uuid"]
        if not isinstance(project_uuid, str) or not _UUID.fullmatch(project_uuid):
            raise ValidationRecipeError("project_uuid must be a UUID")
        assertions = {
            "project_uuid": project_uuid.lower(),
            "explore": _token(recipe_args["explore"], "explore"),
            "fields": _tokens(recipe_args["fields"], "fields", minimum=1),
        }
    return RecipeRequest(recipe, _token(recipe_args["command_id"], "command_id"), capability, subject, assertions)

def validate_recipe_args(*, recipe: object, recipe_args: object, required_capability: object) -> dict[str, Any]:
    """Validate the typed request payload before a subject is available.

    API request models use this conditional validator.  Service admission must
    still call :func:`validate_admission`, which additionally proves the exact
    subject and the installed capability owner.
    """
    parse_recipe(
        recipe=recipe, recipe_args=recipe_args,
        required_capability=required_capability, subject="0" * 40,
    )
    # Pydantic has already copied the request body; the service stores no
    # normalized command or executable supplied by the request.
    return recipe_args


def validate_admission(gate: dict[str, Any], capabilities: set[str] | None = None) -> RecipeRequest:
    """Fail closed before an automatic plan creates an unexecutable required gate."""
    request = parse_recipe(
        recipe=gate.get("recipe"), recipe_args=gate.get("recipe_args"),
        required_capability=gate.get("required_capability"), subject=gate.get("subject"),
    )
    if request.required_capability not in (available_capabilities() if capabilities is None else capabilities):
        raise ValidationRecipeError(f"validation capability is not owned: {request.required_capability}")
    catalog = installed_recipe_catalog()
    item = catalog.get(request.command_id)
    if item is None or item["recipe"] != request.recipe or item["capability"] != request.required_capability:
        raise ValidationRecipeError("validation command is not installed")
    if request.recipe == "public_source_smoke" and (
            item["source_url"] != request.assertions["source_url"]
            or item["expected_status"] != request.assertions["expected_status"]):
        raise ValidationRecipeError("validation command does not match public source assertions")
    return request
