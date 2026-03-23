import argparse
import json
import os
import sys
import traceback


def main():
    parser = argparse.ArgumentParser(description="Run a sync schedule target in an isolated process.")
    parser.add_argument("--payload-file", required=True, help="Path to the JSON payload file.")
    args = parser.parse_args()

    with open(args.payload_file, "r", encoding="utf-8") as fp:
        payload = json.load(fp)

    target_code = payload.get("target_code")
    schedule_data = payload.get("schedule_data") or {}
    user_context = payload.get("user_context") or {}

    try:
        backend_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if backend_root not in sys.path:
            sys.path.insert(0, backend_root)
        from main import run_sync_target_handler

        result = run_sync_target_handler(target_code, schedule_data, user_context) or {}
        if not isinstance(result, dict):
            result = {"status": "success", "message": str(result), "logs": []}
        result.setdefault("code", target_code)
        result.setdefault("status", "success")
        result.setdefault("message", "")
        result.setdefault("logs", [])
        result.setdefault("task_id", None)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:
        failure = {
            "code": target_code,
            "status": "failed",
            "message": str(exc),
            "logs": [{"type": "error", "message": str(exc)}],
            "task_id": None,
            "traceback": traceback.format_exc(limit=8),
        }
        print(json.dumps(failure, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())
