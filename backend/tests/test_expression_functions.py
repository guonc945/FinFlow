from datetime import datetime

from utils.expression_functions import (
    evaluate_expression,
    get_public_expression_functions,
)


def test_date_format_supports_datetime_string():
    result = evaluate_expression(
        "DATE_FORMAT({pay_time}, 'YYYY-MM-DD')",
        {"pay_time": "2026-03-09 14:23:45"},
    )
    assert result == "2026-03-09"


def test_date_only_supports_timestamp_and_mixed_text():
    pay_time = 1741510800
    expected_date = datetime.fromtimestamp(pay_time).strftime("%Y-%m-%d")
    result = evaluate_expression(
        "收款-DATE_ONLY({pay_time})-{charge_item_name}",
        {"pay_time": pay_time, "charge_item_name": "物业费"},
    )
    assert result == f"收款-{expected_date}-物业费"


def test_public_functions_contains_date_format():
    functions = get_public_expression_functions()
    keys = {item["key"] for item in functions}
    assert "DATE_FORMAT" in keys
