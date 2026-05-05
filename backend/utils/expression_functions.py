# -*- coding: utf-8 -*-
import re
from decimal import Decimal, InvalidOperation
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional
import ast


def _translate_datetime_format(fmt: str) -> str:
    if not fmt:
        return "%Y-%m-%d"

    translated = str(fmt)
    replacements = [
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("MM", "%m"),
        ("DD", "%d"),
        ("HH", "%H"),
        ("mm", "%M"),
        ("ss", "%S"),
    ]
    for token, target in replacements:
        translated = translated.replace(token, target)
    return translated


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    if isinstance(value, (int, float)):
        timestamp = float(value)
        if abs(timestamp) > 1e12:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp)

    text = str(value).strip()
    if not text:
        return None

    if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        timestamp = float(text)
        if abs(timestamp) > 1e12:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp)

    normalized = text.replace("T", " ").replace("Z", "")
    normalized = re.sub(r"([+-]\d{2}):?(\d{2})$", "", normalized).strip()
    normalized = normalized.split(".", 1)[0].strip()

    patterns = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y%m%d",
        "%Y%m%d%H%M%S",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(normalized, pattern)
        except ValueError:
            continue

    return None


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _date_format(value: Any, fmt: str = "YYYY-MM-DD") -> str:
    dt = _parse_datetime(value)
    if dt is None:
        return ""
    return dt.strftime(_translate_datetime_format(fmt))


def _date_only(value: Any) -> str:
    return _date_format(value, "YYYY-MM-DD")


def _default(value: Any, fallback: Any = "") -> str:
    return "" if _is_blank(fallback) and _is_blank(value) else str(fallback if _is_blank(value) else value)


def _trim(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _upper(value: Any) -> str:
    return "" if value is None else str(value).upper()


def _lower(value: Any) -> str:
    return "" if value is None else str(value).lower()


def _switch(value: Any, *args: Any) -> str:
    val_str = str(value).strip() if value is not None else ""
    for i in range(0, len(args) - 1, 2):
        case_val = str(args[i]).strip() if args[i] is not None else ""
        if val_str == case_val:
            return str(args[i+1]) if args[i+1] is not None else ""
    if len(args) % 2 == 1:
        return str(args[-1]) if args[-1] is not None else ""
    return ""


def _ifeq(val1: Any, val2: Any, true_val: Any, false_val: Any = "") -> str:
    v1 = str(val1).strip() if val1 is not None else ""
    v2 = str(val2).strip() if val2 is not None else ""
    if v1 == v2:
        return str(true_val) if true_val is not None else ""
    return str(false_val) if false_val is not None else ""


_FUNCTION_SPECS: Dict[str, Dict[str, Any]] = {
    "DATE_FORMAT": {
        "key": "DATE_FORMAT",
        "category": "datetime",
        "description": "格式化日期、时间或时间戳，常用于把交易时间转换成金蝶需要的日期格式。",
        "syntax": "DATE_FORMAT(value, 'YYYY-MM-DD')",
        "example": "DATE_FORMAT({pay_time}, 'YYYY-MM-DD')",
        "insert_text": "DATE_FORMAT({pay_time}, 'YYYY-MM-DD')",
        "handler": _date_format,
    },
    "DATE_ONLY": {
        "key": "DATE_ONLY",
        "category": "datetime",
        "description": "将日期时间或时间戳直接转换为日期字符串（YYYY-MM-DD）。",
        "syntax": "DATE_ONLY(value)",
        "example": "DATE_ONLY({pay_time})",
        "insert_text": "DATE_ONLY({pay_time})",
        "handler": _date_only,
    },
    "DEFAULT": {
        "key": "DEFAULT",
        "category": "text",
        "description": "当值为空时使用默认值，避免生成空字符串。",
        "syntax": "DEFAULT(value, '默认值')",
        "example": "DEFAULT({remark}, '无备注')",
        "insert_text": "DEFAULT({remark}, '默认值')",
        "handler": _default,
    },
    "TRIM": {
        "key": "TRIM",
        "category": "text",
        "description": "去掉文本前后的空格。",
        "syntax": "TRIM(value)",
        "example": "TRIM({full_house_name})",
        "insert_text": "TRIM({full_house_name})",
        "handler": _trim,
    },
    "UPPER": {
        "key": "UPPER",
        "category": "text",
        "description": "将文本转换为大写。",
        "syntax": "UPPER(value)",
        "example": "UPPER({receipt_id})",
        "insert_text": "UPPER({receipt_id})",
        "handler": _upper,
    },
    "LOWER": {
        "key": "LOWER",
        "category": "text",
        "description": "将文本转换为小写。",
        "syntax": "LOWER(value)",
        "example": "LOWER({receipt_id})",
        "insert_text": "LOWER({receipt_id})",
        "handler": _lower,
    },
    "SWITCH": {
        "key": "SWITCH",
        "category": "logic",
        "description": "多分支条件判断。根据第一个参数的值，匹配后续的 (判断值, 结果值) 对。如果没有匹配的，可返回最后的默认结果。",
        "syntax": "SWITCH(value, case1, result1, case2, result2, default_result)",
        "example": "SWITCH({pay_type}, '1', '微信支付', '2', '支付宝', '其他支付方式')",
        "insert_text": "SWITCH({pay_type}, '1', '微信', '2', '支付宝', '其他')",
        "handler": _switch,
    },
    "IF": {
        "key": "IF",
        "category": "logic",
        "description": "判断两个值是否相等。如果相等则返回第三个参数结果，否则返回第四个参数结果。",
        "syntax": "IF(value1, value2, true_result, false_result)",
        "example": "IF({status}, '1', '成功', '失败')",
        "insert_text": "IF({status}, '1', '成功', '失败')",
        "handler": _ifeq,
    },
}

_FUNCTION_CALL_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\(([^()]*)\)")
_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")


def get_public_expression_functions() -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for key in sorted(_FUNCTION_SPECS.keys()):
        spec = _FUNCTION_SPECS[key]
        items.append({
            "key": spec["key"],
            "category": spec["category"],
            "description": spec["description"],
            "syntax": spec["syntax"],
            "example": spec["example"],
            "insert_text": spec["insert_text"],
        })
    return items


def get_public_expression_function_names() -> List[str]:
    return sorted(_FUNCTION_SPECS.keys())


def extract_expression_function_names(text: Any) -> List[str]:
    if text is None:
        return []
    content = str(text)
    names = []
    for match in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", content):
        names.append(match.group(1).upper())
    return names


def _split_args(raw_args: str) -> List[str]:
    args: List[str] = []
    current: List[str] = []
    quote_char: Optional[str] = None
    escape = False
    paren_depth = 0
    brace_depth = 0

    for char in raw_args:
        if quote_char:
            current.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote_char:
                quote_char = None
            continue

        if char in ("'", '"'):
            quote_char = char
            current.append(char)
            continue

        if char == "(":
            paren_depth += 1
            current.append(char)
            continue

        if char == ")":
            if paren_depth > 0:
                paren_depth -= 1
            current.append(char)
            continue

        if char == "{":
            brace_depth += 1
            current.append(char)
            continue

        if char == "}":
            if brace_depth > 0:
                brace_depth -= 1
            current.append(char)
            continue

        if char == "," and paren_depth == 0 and brace_depth == 0:
            args.append("".join(current).strip())
            current = []
            continue

        current.append(char)

    trailing = "".join(current).strip()
    if trailing or raw_args.strip() == "":
        args.append(trailing)
    return args


def _replace_placeholders(text: str, data: Dict[str, Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        value = data.get(key, "")
        return "" if value is None else str(value)

    return _PLACEHOLDER_RE.sub(repl, text)


def _safe_eval_arithmetic(expr: str) -> Optional[str]:
    """
    Evaluate simple arithmetic expressions safely.
    Allowed tokens: digits, decimal point, whitespace, parentheses, + - * /
    """
    if not isinstance(expr, str):
        return None

    stripped = expr.strip()
    if not stripped:
        return None

    if not re.fullmatch(r"[0-9\.\+\-\*\/\(\)\s]+", stripped):
        return None

    if "+" not in stripped and "*" not in stripped and "/" not in stripped:
        return None

    try:
        tree = ast.parse(stripped, mode="eval")
    except SyntaxError:
        return None

    def _eval(node: ast.AST) -> Decimal:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return Decimal(str(node.value))
            raise ValueError("Unsupported constant")
        if isinstance(node, ast.UnaryOp):
            value = _eval(node.operand)
            if isinstance(node.op, ast.UAdd):
                return value
            if isinstance(node.op, ast.USub):
                return -value
            raise ValueError("Unsupported unary operator")
        if isinstance(node, ast.BinOp):
            left = _eval(node.left)
            right = _eval(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            raise ValueError("Unsupported binary operator")
        raise ValueError("Unsupported expression")

    try:
        value = _eval(tree)
    except (ValueError, ZeroDivisionError, InvalidOperation):
        return None

    normalized = format(value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


def _resolve_functions_once(text: str, data: Dict[str, Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        fn_name = match.group(1).upper()
        spec = _FUNCTION_SPECS.get(fn_name)
        if not spec:
            return match.group(0)

        raw_args = _split_args(match.group(2))
        resolved_args = [evaluate_expression(arg, data) for arg in raw_args]
        handler: Callable[..., Any] = spec["handler"]
        try:
            result = handler(*resolved_args)
        except TypeError:
            return match.group(0)
        return "" if result is None else str(result)

    return _FUNCTION_CALL_RE.sub(repl, text)


def evaluate_expression(expr: Any, data: Dict[str, Any]) -> str:
    if expr is None:
        return ""

    content = str(expr).strip()
    if not content:
        return ""

    if len(content) >= 2 and content[0] == "'" and content[-1] == "'":
        return content[1:-1]

    previous = None
    while previous != content:
        previous = content
        content = _resolve_functions_once(content, data)

    resolved = _replace_placeholders(content, data)
    calculated = _safe_eval_arithmetic(resolved)
    return calculated if calculated is not None else resolved
