# Ensure catchup=False to avoid unintended backfills
# This test ensures: DAGs with catchup=True can create massive backfills on first deployment.
# Guidance: https://docs.astronomer.io/learn/dags


def test_catchup_disabled(generated_dags):
    violating = [
        dag_id
        for dag_id, dag in generated_dags.items()
        if getattr(dag, "catchup", True)
    ]
    assert not violating, (
        "The following DAGs have catchup=True (can create unintended backfills on first deploy): "
        + ", ".join(sorted(violating))
        + " — see Astronomer guidance: https://docs.astronomer.io/learn/dags"
    )
