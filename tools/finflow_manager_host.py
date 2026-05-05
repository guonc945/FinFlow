# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import traceback
from pathlib import Path


def _fallback_log_path(name: str) -> Path:
    return Path.cwd() / "deploy" / "windows" / "runtime" / "logs" / name


def _append_bootstrap_line(path: Path, text: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8", errors="ignore") as handle:
            handle.write(text.rstrip() + "\n")
    except Exception:
        pass


def main() -> int:
    try:
        from finflow_manager import (
            FinFlowRuntimeHost,
            FRONTEND_STDERR_LOG,
            SERVICE_HOST_LOG,
            append_text_log,
            build_cli_parser,
            clear_stop_request,
            create_stop_request,
            run_cli,
        )
    except BaseException:
        _append_bootstrap_line(_fallback_log_path("manager.service.log"), "===== Host Helper Import Failed =====")
        _append_bootstrap_line(_fallback_log_path("manager.service.log"), traceback.format_exc())
        raise

    parser = build_cli_parser()
    args = parser.parse_args()

    if args.windows_service:
        try:
            import servicemanager
            import win32service
            import win32serviceutil
        except BaseException:
            append_text_log(SERVICE_HOST_LOG, "===== Windows Service Bootstrap Failed =====")
            append_text_log(SERVICE_HOST_LOG, traceback.format_exc())
            raise

        class FinFlowManagerWindowsService(win32serviceutil.ServiceFramework):
            _svc_name_ = "FinFlowManagerHost"
            _svc_display_name_ = "FinFlow Manager Host"
            _svc_description_ = "FinFlow production runtime host service"

            def __init__(self, svc_args):
                super().__init__(svc_args)
                self.runtime_host = None

            def SvcStop(self):
                self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
                append_text_log(SERVICE_HOST_LOG, "===== Windows Service Stop Requested =====")
                create_stop_request()
                runtime_host = self.runtime_host
                if runtime_host is not None:
                    try:
                        runtime_host.request_stop()
                    except Exception:
                        append_text_log(SERVICE_HOST_LOG, traceback.format_exc())

            def SvcDoRun(self):
                append_text_log(SERVICE_HOST_LOG, "===== Windows Service Entry =====")
                clear_stop_request()
                self.runtime_host = FinFlowRuntimeHost()
                try:
                    exit_code = self.runtime_host.run()
                    append_text_log(SERVICE_HOST_LOG, f"===== Windows Service Exit code={exit_code} =====")
                except BaseException:
                    append_text_log(SERVICE_HOST_LOG, traceback.format_exc())
                    raise

        try:
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(FinFlowManagerWindowsService)
            servicemanager.StartServiceCtrlDispatcher()
            return 0
        except BaseException:
            append_text_log(SERVICE_HOST_LOG, "===== Windows Service Dispatcher Failed =====")
            append_text_log(SERVICE_HOST_LOG, traceback.format_exc())
            raise

    if args.host:
        append_text_log(SERVICE_HOST_LOG, "===== Host Helper Bootstrap =====")
        append_text_log(SERVICE_HOST_LOG, f"argv={sys.argv}")
    elif args.frontend_run:
        append_text_log(FRONTEND_STDERR_LOG, "===== Frontend Helper Bootstrap =====")
        append_text_log(FRONTEND_STDERR_LOG, f"argv={sys.argv}")

    try:
        return run_cli(args)
    except BaseException:
        target_log = SERVICE_HOST_LOG if args.host else FRONTEND_STDERR_LOG
        append_text_log(target_log, traceback.format_exc())
        raise


if __name__ == "__main__":
    raise SystemExit(main())
