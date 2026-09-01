"""BigQuery 后台下载脚本的 worker 生命周期回归测试。"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENTRY_SCRIPT = REPO_ROOT / "scripts" / "download-push-events.sh"
COMMON_SCRIPT = REPO_ROOT / "scripts" / "lib" / "bigquery-download-common.sh"


def test_status_json_reports_persisted_last_exit(tmp_path: Path) -> None:
    """状态查询必须暴露最近退出码，而不依赖已被清理的 PID 文件。"""

    log_file = tmp_path / "push.log"
    last_exit = {
        "schema_version": 1,
        "event": "PushEvent",
        "command": "download-push-events",
        "started_at": "2026-09-01T21:55:45Z",
        "ended_at": "2026-09-01T21:59:10Z",
        "exit_code": 143,
        "signal": 15,
    }
    Path(f"{log_file}.last-exit.json").write_text(json.dumps(last_exit), encoding="utf-8")

    completed = subprocess.run(
        [ENTRY_SCRIPT, "status", "--json", "--skip-quota"],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "TMPDIR": str(tmp_path),
            "STARCAT_PUSH_VOLUME_ROOT": str(tmp_path),
            "STARCAT_PUSH_WORKSPACE": str(tmp_path / "workspace"),
            "STARCAT_PUSH_LOG_FILE": str(log_file),
        },
        check=True,
        capture_output=True,
        text=True,
    )

    document = json.loads(completed.stdout)
    assert document["state"] == "stopped"
    assert document["screen_alive"] is False
    assert document["last_exit"] == last_exit
    assert document["last_exit_error"] is None


def test_worker_readiness_does_not_treat_screen_as_running(tmp_path: Path) -> None:
    """screen 存活但 worker PID 无效时，就绪检查必须失败。"""

    harness = tmp_path / "readiness-harness.sh"
    harness.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR={str(REPO_ROOT / "scripts")!r}
EVENT_LABEL=PushEvent
COMMAND_NAME=download-push-events
SESSION_NAME=starcat-push-events-test
VOLUME_ROOT={str(tmp_path)!r}
WORKSPACE={str(tmp_path / "workspace")!r}
LOG_FILE={str(tmp_path / "push.log")!r}
START_DATE=2016-01-01
END_DATE=2016-01-01
MAXIMUM_BYTES_PER_DAY=1
MAXIMUM_TOTAL_BYTES=1
RUN_ID=test
APPEND_EXISTING=false
STATE_RELATIVE_PATH=raw/state.json
PID_BASENAME=starcat-push-events-test
source {str(COMMON_SCRIPT)!r}
worker_pid() {{ return 1; }}
has_screen_session() {{ return 0; }}
if wait_for_worker_ready; then
  echo unexpected-ready
  exit 9
fi
echo not-ready
""",
        encoding="utf-8",
    )

    completed = subprocess.run(
        ["/bin/bash", harness],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "TMPDIR": str(tmp_path),
            "STARCAT_DOWNLOAD_STARTUP_READY_ATTEMPTS": "2",
            "STARCAT_DOWNLOAD_STARTUP_READY_INTERVAL_SECONDS": "0",
        },
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout == "not-ready\n"


def test_record_worker_exit_persists_structured_reason(tmp_path: Path) -> None:
    """worker 结束后必须同时留下可读日志和结构化退出状态。"""

    log_file = tmp_path / "push.log"
    last_exit_file = tmp_path / "push.last-exit.json"
    harness = tmp_path / "exit-harness.sh"
    harness.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR={str(REPO_ROOT / "scripts")!r}
EVENT_LABEL=PushEvent
COMMAND_NAME=download-push-events
SESSION_NAME=starcat-push-events-test
VOLUME_ROOT={str(tmp_path)!r}
WORKSPACE={str(tmp_path / "workspace")!r}
LOG_FILE={str(log_file)!r}
START_DATE=2016-01-01
END_DATE=2016-01-01
MAXIMUM_BYTES_PER_DAY=1
MAXIMUM_TOTAL_BYTES=1
RUN_ID=test
APPEND_EXISTING=false
STATE_RELATIVE_PATH=raw/state.json
PID_BASENAME=starcat-push-events-test
source {str(COMMON_SCRIPT)!r}
record_worker_exit 2026-09-01T21:55:45Z 137 TERM
""",
        encoding="utf-8",
    )

    subprocess.run(
        ["/bin/bash", harness],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "TMPDIR": str(tmp_path),
            "STARCAT_DOWNLOAD_LAST_EXIT_FILE": str(last_exit_file),
        },
        check=True,
        capture_output=True,
        text=True,
    )

    document = json.loads(last_exit_file.read_text(encoding="utf-8"))
    assert document == {
        "schema_version": 1,
        "event": "PushEvent",
        "command": "download-push-events",
        "started_at": "2026-09-01T21:55:45Z",
        "ended_at": document["ended_at"],
        "exit_code": 137,
        "signal": 9,
        "wrapper_signal": "TERM",
    }
    assert "下载 worker 异常退出（exit_code=137, signal=9）" in log_file.read_text(encoding="utf-8")


def test_run_worker_tracks_real_cli_pid_and_exit_code(tmp_path: Path) -> None:
    """PID 文件必须指向 CLI，而不是包裹 CLI 的 caffeinate 进程。"""

    fake_repo = tmp_path / "repo"
    script_directory = fake_repo / "scripts"
    cli_directory = fake_repo / ".venv" / "bin"
    script_directory.mkdir(parents=True)
    cli_directory.mkdir(parents=True)
    worker_pid_file = tmp_path / "fake-worker.pid"
    fake_cli = cli_directory / "starcat-recsys"
    fake_cli.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$$" >"$FAKE_WORKER_PID_FILE"
printf 'fake worker started\n'
if [[ "${FAKE_SIGNAL_WRAPPER:-false}" == "true" ]]; then
  /bin/sleep 0.1
  /bin/kill -TERM "$PPID"
  /bin/sleep 10
fi
/bin/sleep 0.2
exit 7
""",
        encoding="utf-8",
    )
    fake_cli.chmod(0o755)
    # 共享脚本使用仓库虚拟环境里的 Python 原子写入退出状态；测试仓库只替换 CLI。
    (cli_directory / "python").symlink_to(REPO_ROOT / ".venv" / "bin" / "python")

    log_file = tmp_path / "worker.log"
    last_exit_file = tmp_path / "worker.last-exit.json"
    harness = script_directory / "download-test-events.sh"
    harness.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR={str(script_directory)!r}
EVENT_LABEL=TestEvent
COMMAND_NAME=download-test-events
SESSION_NAME=starcat-test-events
VOLUME_ROOT={str(tmp_path)!r}
WORKSPACE={str(tmp_path / "workspace")!r}
LOG_FILE={str(log_file)!r}
START_DATE=2016-01-01
END_DATE=2016-01-01
MAXIMUM_BYTES_PER_DAY=1
MAXIMUM_TOTAL_BYTES=1
RUN_ID=test
APPEND_EXISTING=false
STATE_RELATIVE_PATH=raw/state.json
PID_BASENAME=starcat-test-events
source {str(COMMON_SCRIPT)!r}
require_external_mount() {{ :; }}
bigquery_download_main "$@"
""",
        encoding="utf-8",
    )
    harness.chmod(0o755)

    completed = subprocess.run(
        [harness, "_run"],
        cwd=fake_repo,
        env={
            **os.environ,
            "TMPDIR": str(tmp_path),
            "FAKE_WORKER_PID_FILE": str(worker_pid_file),
            "STARCAT_DOWNLOAD_LAST_EXIT_FILE": str(last_exit_file),
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )

    actual_worker_pid = worker_pid_file.read_text(encoding="utf-8").strip()
    log_text = log_file.read_text(encoding="utf-8")
    last_exit = json.loads(last_exit_file.read_text(encoding="utf-8"))
    assert completed.returncode == 7
    assert f"下载 worker 已就绪（PID {actual_worker_pid}）" in log_text
    assert "fake worker started" in log_text
    assert last_exit["exit_code"] == 7
    assert last_exit["signal"] is None
    assert last_exit["wrapper_signal"] is None

    signaled = subprocess.run(
        [harness, "_run"],
        cwd=fake_repo,
        env={
            **os.environ,
            "TMPDIR": str(tmp_path),
            "FAKE_WORKER_PID_FILE": str(worker_pid_file),
            "FAKE_SIGNAL_WRAPPER": "true",
            "STARCAT_DOWNLOAD_LAST_EXIT_FILE": str(last_exit_file),
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )

    signal_exit = json.loads(last_exit_file.read_text(encoding="utf-8"))
    assert signaled.returncode == 143
    assert signal_exit["exit_code"] == 143
    assert signal_exit["signal"] == 15
    assert signal_exit["wrapper_signal"] == "TERM"
    assert "下载 wrapper 收到 TERM" in log_file.read_text(encoding="utf-8")
