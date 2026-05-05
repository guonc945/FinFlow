from pathlib import Path
import sys

from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import models
from services.reporting_database import ReportingDatabaseService


def make_engine_with_rows():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(
            text("INSERT INTO items (id, name) VALUES (1, 'A'), (2, 'B'), (3, 'C')")
        )
    return engine


def test_execute_query_without_limit_does_not_apply_hidden_default(monkeypatch):
    engine = make_engine_with_rows()
    monkeypatch.setattr(
        ReportingDatabaseService,
        "create_engine_for",
        staticmethod(lambda connection: engine),
    )

    result = ReportingDatabaseService.execute_query(
        connection=models.ReportingDbConnection(),
        sql_text="SELECT id, name FROM items ORDER BY id",
    )

    assert result["limit"] is None
    assert result["row_count"] == 3
    assert [row["id"] for row in result["rows"]] == [1, 2, 3]


def test_execute_dataset_preview_uses_explicit_preview_limit(monkeypatch):
    engine = make_engine_with_rows()
    monkeypatch.setattr(
        ReportingDatabaseService,
        "create_engine_for",
        staticmethod(lambda connection: engine),
    )

    dataset = models.ReportingDataset(
        sql_text="SELECT id, name FROM items ORDER BY id",
        params_json=None,
        row_limit=2,
    )

    result = ReportingDatabaseService.execute_dataset(
        connection=models.ReportingDbConnection(),
        dataset=dataset,
        default_limit=dataset.row_limit,
    )

    assert result["limit"] == 2
    assert result["row_count"] == 2
    assert [row["id"] for row in result["rows"]] == [1, 2]
