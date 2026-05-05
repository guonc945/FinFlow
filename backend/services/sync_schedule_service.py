# -*- coding: utf-8 -*-
import concurrent.futures
import hashlib
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import models
from sqlalchemy import text

WEEKDAY_CODES = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
DEFAULT_TIMEZONE = "Asia/Shanghai"
RECEIPT_BILL_REQUIRED_TARGET_CODES = ("bills", "deposit_records", "prepayment_records")


def utcnow_naive() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def parse_json_list(raw_value: Optional[str]) -> List[Any]:
    if not raw_value:
        return []
    try:
        data = json.loads(raw_value)
    except (TypeError, ValueError):
        return []
    return data if isinstance(data, list) else []


def serialize_json_list(values: Optional[List[Any]]) -> str:
    return json.dumps(values or [], ensure_ascii=False)


def normalize_sync_target_codes(values: Optional[List[Any]], valid_codes: Optional[set[str]] = None) -> List[str]:
    normalized: List[str] = []
    seen = set()

    for value in values or []:
        code = str(value or "").strip()
        if not code or code in seen:
            continue
        if valid_codes is not None and code not in valid_codes:
            continue
        seen.add(code)
        normalized.append(code)

    if "receipt_bills" in seen:
        for dependency_code in RECEIPT_BILL_REQUIRED_TARGET_CODES:
            if valid_codes is not None and dependency_code not in valid_codes:
                continue
            if dependency_code in seen:
                continue
            seen.add(dependency_code)
            normalized.append(dependency_code)

    return normalized


def normalize_weekdays(values: Optional[List[str]]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for value in values or []:
        code = str(value or "").strip().upper()
        if code not in WEEKDAY_CODES or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _parse_time_parts(time_value: Optional[str]) -> tuple[int, int]:
    text = (time_value or "").strip()
    if not text:
        return 0, 0
    try:
        hour_str, minute_str = text.split(":", 1)
        hour = max(0, min(23, int(hour_str)))
        minute = max(0, min(59, int(minute_str)))
        return hour, minute
    except (TypeError, ValueError):
        return 0, 0


def compute_next_run_at(
    schedule_type: str,
    interval_minutes: Optional[int] = None,
    daily_time: Optional[str] = None,
    weekly_days: Optional[List[str]] = None,
    timezone_name: Optional[str] = None,
    now_utc: Optional[datetime] = None,
) -> Optional[datetime]:
    tz_name = (timezone_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        tzinfo = ZoneInfo(tz_name)
    except Exception:
        tzinfo = ZoneInfo(DEFAULT_TIMEZONE)

    current_utc = (now_utc or utcnow_naive()).replace(tzinfo=timezone.utc)
    current_local = current_utc.astimezone(tzinfo)

    if schedule_type == "interval":
        minutes = max(5, int(interval_minutes or 0))
        return (current_utc + timedelta(minutes=minutes)).replace(tzinfo=None)

    hour, minute = _parse_time_parts(daily_time)

    if schedule_type == "daily":
        candidate_local = current_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate_local <= current_local:
            candidate_local += timedelta(days=1)
        return candidate_local.astimezone(timezone.utc).replace(tzinfo=None)

    if schedule_type == "weekly":
        normalized_days = normalize_weekdays(weekly_days)
        if not normalized_days:
            normalized_days = ["MON"]
        target_indexes = [WEEKDAY_CODES.index(code) for code in normalized_days]
        base_local = current_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        for offset in range(8):
            candidate = base_local + timedelta(days=offset)
            if candidate.weekday() not in target_indexes:
                continue
            if candidate <= current_local:
                continue
            return candidate.astimezone(timezone.utc).replace(tzinfo=None)
        return (base_local + timedelta(days=7)).astimezone(timezone.utc).replace(tzinfo=None)

    return None


class SyncScheduleService:
    def __init__(self, session_factory, engine, poll_interval_seconds: int = 30):
        self._session_factory = session_factory
        self._engine = engine
        self._poll_interval_seconds = max(10, int(poll_interval_seconds))
        self._supported_targets: set[str] = set()
        self._thread: Optional[threading.Thread] = None
        self._leader_conn = None
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._started = False
        self.logger = logging.getLogger("sync_schedule")

    def register_handler(self, code: str, _handler):
        self._supported_targets.add(code)

    def start(self):
        with self._state_lock:
            if self._started:
                return
            if not self._try_acquire_leader_lock():
                self.logger.info("Sync scheduler not started in this worker because leader lock is held elsewhere.")
                return
            self._stop_event.clear()
            try:
                self._recover_running_state()
                self._thread = threading.Thread(target=self._loop, name="sync-schedule-worker", daemon=True)
                self._thread.start()
                self._started = True
            except Exception:
                self._thread = None
                self._stop_event.set()
                self._release_leader_lock()
                self.logger.exception("Failed to start sync scheduler")

    def stop(self):
        with self._state_lock:
            if not self._started:
                return
            self._started = False
            self._stop_event.set()
            thread = self._thread
            self._thread = None
        if thread and thread.is_alive():
            thread.join(timeout=3)
        self._release_leader_lock()

    def _advisory_lock_key(self, scope: str, name: str) -> int:
        digest = hashlib.sha1(f"{scope}:{name}".encode("utf-8")).digest()[:8]
        return int.from_bytes(digest, byteorder="big", signed=False) & 0x7FFFFFFFFFFFFFFF

    def _try_acquire_db_lock(self, scope: str, name: str):
        if self._engine.dialect.name != "postgresql":
            return object()
        lock_key = self._advisory_lock_key(scope, name)
        conn = self._engine.connect()
        acquired = conn.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": lock_key}).scalar()
        if acquired:
            return conn
        conn.close()
        return None

    def _release_db_lock(self, handle, scope: str, name: str):
        if handle is None:
            return
        if self._engine.dialect.name != "postgresql":
            return
        lock_key = self._advisory_lock_key(scope, name)
        try:
            handle.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})
        finally:
            handle.close()

    def _try_acquire_leader_lock(self) -> bool:
        leader_conn = self._try_acquire_db_lock("sync_scheduler", "leader")
        if leader_conn is None:
            return False
        self._leader_conn = leader_conn
        return True

    def _release_leader_lock(self):
        leader_conn = self._leader_conn
        self._leader_conn = None
        if leader_conn is None:
            return
        self._release_db_lock(leader_conn, "sync_scheduler", "leader")

    def _loop(self):
        while not self._stop_event.is_set():
            try:
                self.scan_due_schedules()
            except Exception:
                self.logger.exception("Failed to scan due sync schedules")
            if self._stop_event.wait(self._poll_interval_seconds):
                break

    def _recover_running_state(self):
        db = self._session_factory()
        try:
            now = utcnow_naive()
            running_schedules = db.query(models.SyncSchedule).filter(models.SyncSchedule.is_running == True).all()
            for schedule in running_schedules:
                schedule.is_running = False
                schedule.current_execution_id = None
                if schedule.enabled and schedule.next_run_at is None:
                    schedule.next_run_at = compute_next_run_at(
                        schedule_type=schedule.schedule_type,
                        interval_minutes=schedule.interval_minutes,
                        daily_time=schedule.daily_time,
                        weekly_days=parse_json_list(schedule.weekly_days),
                        timezone_name=schedule.timezone,
                        now_utc=now,
                    )

            running_executions = (
                db.query(models.SyncScheduleExecution)
                .filter(models.SyncScheduleExecution.status == "running")
                .all()
            )
            for execution in running_executions:
                execution.status = "failed"
                execution.finished_at = now
                execution.error_message = "Scheduler restarted before child processes completed."
                execution.summary = execution.summary or "Execution interrupted by scheduler restart."
            db.commit()
        except Exception:
            db.rollback()
            self.logger.exception("Failed to recover sync schedule state")
        finally:
            db.close()

    def scan_due_schedules(self):
        db = self._session_factory()
        try:
            now = utcnow_naive()
            due_ids = [
                row[0]
                for row in db.query(models.SyncSchedule.id)
                .filter(
                    models.SyncSchedule.enabled == True,
                    models.SyncSchedule.is_running == False,
                    models.SyncSchedule.next_run_at.isnot(None),
                    models.SyncSchedule.next_run_at <= now,
                )
                .order_by(models.SyncSchedule.next_run_at.asc(), models.SyncSchedule.id.asc())
                .all()
            ]
        finally:
            db.close()

        for schedule_id in due_ids:
            try:
                self.trigger_execution(schedule_id, trigger_type="auto", user_id=None, advance_schedule=True)
            except RuntimeError:
                continue
            except Exception:
                self.logger.exception("Failed to trigger due sync schedule %s", schedule_id)

    def trigger_execution(
        self,
        schedule_id: int,
        trigger_type: str,
        user_id: Optional[int],
        advance_schedule: bool,
    ) -> Dict[str, Any]:
        payload = self._claim_execution(schedule_id, trigger_type, user_id)
        execution_id = payload["execution_id"]
        worker = threading.Thread(
            target=self._run_execution,
            args=(execution_id, advance_schedule),
            name=f"sync-schedule-exec-{execution_id}",
            daemon=True,
        )
        worker.start()
        return payload

    def _claim_execution(self, schedule_id: int, trigger_type: str, user_id: Optional[int]) -> Dict[str, Any]:
        db = self._session_factory()
        try:
            now = utcnow_naive()
            query = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id)
            if self._engine.dialect.name == "postgresql":
                query = query.with_for_update()

            schedule = query.first()
            if not schedule:
                raise RuntimeError("Schedule not found")
            if schedule.is_running:
                raise RuntimeError("Schedule is already running")
            if trigger_type == "auto":
                if not schedule.enabled:
                    raise RuntimeError("Schedule is disabled")
                if schedule.next_run_at is None or schedule.next_run_at > now:
                    raise RuntimeError("Schedule is not due")

            target_codes = normalize_sync_target_codes(
                parse_json_list(schedule.target_codes),
                valid_codes=self._supported_targets,
            )
            if not target_codes:
                raise RuntimeError("Schedule has no targets configured")

            consumes_due_slot = bool(schedule.enabled and schedule.next_run_at and schedule.next_run_at <= now)

            execution = models.SyncScheduleExecution(
                schedule_id=schedule.id,
                trigger_type=trigger_type,
                triggered_by=user_id,
                status="running",
                started_at=now,
                total_targets=len(target_codes),
                success_targets=0,
                failed_targets=0,
            )
            db.add(execution)
            db.flush()

            schedule.is_running = True
            schedule.current_execution_id = execution.id
            if consumes_due_slot:
                schedule.next_run_at = None
            db.commit()

            return {"execution_id": execution.id, "schedule_id": schedule.id, "status": execution.status}
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _build_schedule_context(self, db, schedule: models.SyncSchedule) -> tuple[Dict[str, Any], Dict[str, str]]:
        created_by_user = None
        if schedule.created_by:
            created_by_user = db.query(models.User).filter(models.User.id == schedule.created_by).first()

        org_name = ""
        if created_by_user and created_by_user.organization:
            org_name = created_by_user.organization.name or ""

        account_book_number = (schedule.account_book_number or "").strip()
        account_book_name = (schedule.account_book_name or "").strip()

        schedule_data = {
            "id": schedule.id,
            "name": schedule.name,
            "description": schedule.description or "",
            "target_codes": normalize_sync_target_codes(
                parse_json_list(schedule.target_codes),
                valid_codes=self._supported_targets,
            ),
            "community_ids": parse_json_list(schedule.community_ids),
            "account_book_number": account_book_number,
            "account_book_name": account_book_name,
            "schedule_type": schedule.schedule_type,
            "interval_minutes": schedule.interval_minutes,
            "daily_time": schedule.daily_time,
            "weekly_days": parse_json_list(schedule.weekly_days),
            "timezone": schedule.timezone or DEFAULT_TIMEZONE,
            "created_by": schedule.created_by,
            "updated_by": schedule.updated_by,
        }
        user_context = {
            "current_user_id": str(created_by_user.id) if created_by_user else "",
            "current_username": created_by_user.username if created_by_user else "scheduler",
            "current_user_realname": (created_by_user.real_name or created_by_user.username) if created_by_user else "Scheduler",
            "current_org_id": str(created_by_user.org_id) if created_by_user and created_by_user.org_id else "",
            "current_org_name": org_name,
            "current_account_book_number": account_book_number,
            "current_account_book_name": account_book_name,
        }
        return schedule_data, user_context

    def _runner_script_path(self) -> str:
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts", "run_sync_schedule_target.py"))

    def _run_target_subprocess(self, target_code: str, schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
        if target_code not in self._supported_targets:
            return {
                "code": target_code,
                "status": "failed",
                "message": f"No handler registered for target '{target_code}'.",
                "logs": [],
                "task_id": None,
            }

        lock_handle = self._try_acquire_db_lock("sync_target", target_code)
        if lock_handle is None:
            return {
                "code": target_code,
                "status": "failed",
                "message": f"Target '{target_code}' is already running in another process.",
                "logs": [{"type": "warning", "message": "Skipped because the same target is already running."}],
                "task_id": None,
            }

        payload_path = None
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as fp:
                payload_path = fp.name
                json.dump(
                    {
                        "target_code": target_code,
                        "schedule_data": schedule_data,
                        "user_context": user_context,
                    },
                    fp,
                    ensure_ascii=False,
                )

            command = [sys.executable, self._runner_script_path(), "--payload-file", payload_path]
            completed = subprocess.run(
                command,
                cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )

            stdout = (completed.stdout or "").strip()
            stderr = (completed.stderr or "").strip()

            result: Dict[str, Any] = {}
            if stdout:
                for line in reversed(stdout.splitlines()):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        parsed = json.loads(line)
                        if isinstance(parsed, dict):
                            result = parsed
                            break
                    except (TypeError, ValueError):
                        continue

            if not result:
                result = {
                    "code": target_code,
                    "status": "failed" if completed.returncode else "success",
                    "message": stderr or stdout or f"Target '{target_code}' finished without structured output.",
                    "logs": [],
                    "task_id": None,
                }

            result.setdefault("code", target_code)
            result.setdefault("status", "success" if completed.returncode == 0 else "failed")
            result.setdefault("message", stderr or stdout or "")
            result.setdefault("logs", [])
            result.setdefault("task_id", None)

            if completed.returncode != 0 and not result.get("traceback") and stderr:
                result["traceback"] = stderr[-4000:]

            return result
        except Exception as exc:
            return {
                "code": target_code,
                "status": "failed",
                "message": str(exc),
                "logs": [{"type": "error", "message": str(exc)}],
                "task_id": None,
            }
        finally:
            if payload_path and os.path.exists(payload_path):
                try:
                    os.remove(payload_path)
                except OSError:
                    pass
            self._release_db_lock(lock_handle, "sync_target", target_code)

    def _run_execution(self, execution_id: int, advance_schedule: bool):
        db = self._session_factory()
        try:
            execution = db.query(models.SyncScheduleExecution).filter(models.SyncScheduleExecution.id == execution_id).first()
            if not execution or not execution.schedule:
                return
            schedule_data, user_context = self._build_schedule_context(db, execution.schedule)
        finally:
            db.close()

        target_codes = list(schedule_data.get("target_codes") or [])
        results_map: Dict[str, Dict[str, Any]] = {}
        fatal_error: Optional[str] = None

        try:
            max_workers = max(1, len(target_codes))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(self._run_target_subprocess, code, schedule_data, user_context): code
                    for code in target_codes
                }
                for future in concurrent.futures.as_completed(future_map):
                    code = future_map[future]
                    try:
                        results_map[code] = future.result()
                    except Exception as exc:
                        results_map[code] = {
                            "code": code,
                            "status": "failed",
                            "message": str(exc),
                            "logs": [{"type": "error", "message": str(exc)}],
                            "task_id": None,
                        }
        except Exception as exc:
            fatal_error = str(exc)
            self.logger.exception("Sync schedule execution %s failed", execution_id)

        results = [results_map.get(code, {"code": code, "status": "failed", "message": "Target did not return a result.", "logs": [], "task_id": None}) for code in target_codes]
        success_count = sum(1 for item in results if item.get("status") == "success")
        failed_count = len(results) - success_count
        finished_at = utcnow_naive()

        if fatal_error:
            overall_status = "failed"
            summary = fatal_error
        elif failed_count == 0:
            overall_status = "success"
            summary = f"All {success_count} target(s) completed successfully in independent processes."
        elif success_count == 0:
            overall_status = "failed"
            summary = f"All {failed_count} target(s) failed."
        else:
            overall_status = "partial"
            summary = f"{success_count} target(s) succeeded, {failed_count} target(s) failed."

        db = self._session_factory()
        try:
            execution = db.query(models.SyncScheduleExecution).filter(models.SyncScheduleExecution.id == execution_id).first()
            if not execution or not execution.schedule:
                return

            schedule = execution.schedule
            execution.status = overall_status
            execution.finished_at = finished_at
            execution.success_targets = success_count
            execution.failed_targets = failed_count
            execution.summary = summary
            execution.error_message = fatal_error
            execution.result_payload = json.dumps(results, ensure_ascii=False)

            schedule.is_running = False
            schedule.current_execution_id = None
            schedule.last_run_at = finished_at
            schedule.last_status = overall_status
            schedule.last_message = summary

            should_advance_next_run = schedule.enabled and (advance_schedule or schedule.next_run_at is None)

            if should_advance_next_run:
                schedule.next_run_at = compute_next_run_at(
                    schedule_type=schedule.schedule_type,
                    interval_minutes=schedule.interval_minutes,
                    daily_time=schedule.daily_time,
                    weekly_days=parse_json_list(schedule.weekly_days),
                    timezone_name=schedule.timezone,
                    now_utc=finished_at,
                )
            elif not schedule.enabled:
                schedule.next_run_at = None

            db.commit()
        except Exception:
            db.rollback()
            self.logger.exception("Failed to finalize sync schedule execution %s", execution_id)
        finally:
            db.close()
