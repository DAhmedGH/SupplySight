"""Static checks for the historical Power BI lineage guard."""

from pathlib import Path

ROOT = (
    Path(__file__).resolve().parents[1]
    / "powerbi/SupplySight.SemanticModel/definition"
)


def test_evaluation_history_is_scoped_to_the_current_forecast_run():
    expressions = (ROOT / "expressions.tmdl").read_text(encoding="utf-8")
    scenario = expressions.split("expression ScenarioRunId =", 1)[1].split(
        "\tlineageTag:", 1
    )[0]
    assert (
        'EvaluationHistory = Table.SelectColumns(#"DevTable"('
        '"FORECASTING", "MODEL_EVALUATION")'
    ) in scenario
    assert (
        "Evaluation = Table.SelectRows(EvaluationHistory, each [RUN_ID] = RunId)"
        in scenario
    )
    assert "EvaluationKeys = KeyRows(Evaluation)" in scenario
    assert "EvaluationValid = Table.RowCount(Evaluation) > 0" in scenario
    assert "List.Distinct(Evaluation[RUN_ID]) = {RunId}" in scenario

    partition = (ROOT / "tables/ForecastEvaluation.tmdl").read_text(
        encoding="utf-8"
    )
    assert "Filtered = Table.SelectRows(Raw, each [RUN_ID] = RunId)" in partition
