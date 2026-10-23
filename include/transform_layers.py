"""Registry of the staging -> current transform layers.

Single source of truth for which SQL files each transform DAG runs, and in what
order. The DAGs (dags/transform_*.py) build one task per file from this list,
and tests/transform runs the same files in the same order against a scratch
Postgres, so the tested pipeline is the deployed pipeline.

Paths are relative to include/sql/. Every layer starts by setting the active
namespace and rebuilding the etl.src_* source views (see source_views.sql), so
any layer can be re-run on its own.
"""

from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"

# Rendered by Airflow (Jinja) at run time; tests call the function directly.
# Values are quote-escaped so a name like "O'Fallon" stays one SQL literal.
SET_NAMESPACE_SQL = (
    "SELECT crosswalk.set_active_namespace("
    "'{{ var.value.get('CDW-COUNTRY-ID', '840') | replace(\"'\", \"''\") }}', "
    "'{{ var.value.get('CDW-REGION-ID', '29510') | replace(\"'\", \"''\") }}', "
    "'{{ var.value.get('CDW-REGION-NAME', 'St. Louis City') | replace(\"'\", \"''\") }}')"
)

SOURCE_VIEWS = "current/source_views.sql"
ASSERT_BALANCED = "current/assert_balanced.sql"

# Layer order follows the FK dependencies in create_current.sql:
# reference -> entities -> spine -> events -> history -> qa.
LAYERS: dict[str, list[str]] = {
    "reference": [
        "current/reference/municipality.sql",
        "current/reference/division.sql",
        "current/reference/zoning_class.sql",
        "current/reference/use_type.sql",
        "current/reference/permit_type.sql",
        "current/reference/inspection_type.sql",
        "current/reference/service_type.sql",
    ],
    "entities": [
        "current/entities/account.sql",
        "current/entities/address.sql",
        "current/entities/legal_entity.sql",
    ],
    "spine": [
        "current/spine/account_class.sql",
        "current/spine/parcel.sql",
        "current/spine/building.sql",
        "current/spine/unit.sql",
        "current/spine/secondary_owner.sql",
    ],
    "events": [
        "current/events/assessment.sql",
        "current/events/tax_delinquency.sql",
        "current/events/permit.sql",
        "current/events/inspection.sql",
        "current/events/condemnation.sql",
        "current/events/service.sql",
    ],
    "history": [
        "current/history/snapshot.sql",
    ],
    "qa": [
        "current/qa/checks.sql",
        "current/qa/assert.sql",
    ],
}


def layer_files(layer: str) -> list[str]:
    """Return the SQL files a layer runs: source views, the layer, then the
    capture-all assertion."""
    return [SOURCE_VIEWS, *LAYERS[layer], ASSERT_BALANCED]
