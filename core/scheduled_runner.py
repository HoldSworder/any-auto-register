"""定时任务执行器

支持两种任务类型：
  - register: 定时注册（复用 api.tasks 流程）
  - cpa_clean: CPA 检测清理（check → disable → delete）

负责计算 next_run_at、到期检查、手动触发、完成回调。
"""

import json as _json
import re
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlmodel import Session, select

from core.db import ScheduledJobModel, engine

# CPA 清理任务的内存日志（与注册任务的 _tasks 类似）
_cpa_clean_tasks: dict[str, dict] = {}
_cpa_clean_lock = threading.Lock()


def compute_next_run(job: ScheduledJobModel) -> Optional[datetime]:
    """根据配置计算下次执行时间。"""
    now = datetime.now(timezone.utc)

    if job.interval_minutes and job.interval_minutes > 0:
        base = job.last_run_at or now
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
        nxt = base + timedelta(minutes=job.interval_minutes)
        if nxt <= now:
            nxt = now + timedelta(minutes=job.interval_minutes)
        return nxt

    if job.cron_expr:
        return _next_cron_time(job.cron_expr, now)

    return None


def _recover_stuck_jobs():
    """重启后将卡在 running 但内存中已无对应 task 的 job 恢复为 failed。"""
    from api.tasks import _tasks, _tasks_lock

    with Session(engine) as s:
        stuck = s.exec(
            select(ScheduledJobModel).where(
                ScheduledJobModel.last_status == "running"
            )
        ).all()
        for job in stuck:
            task_id = job.last_task_id
            alive = False
            if task_id:
                with _tasks_lock:
                    alive = task_id in _tasks
                if not alive:
                    with _cpa_clean_lock:
                        alive = task_id in _cpa_clean_tasks
            if not alive:
                print(f"[ScheduledRunner] 恢复卡住的任务: job_id={job.id} task_id={task_id}")
                job.last_status = "failed"
                job.next_run_at = compute_next_run(job)
                s.add(job)
        s.commit()


_recovered = False


def tick():
    """由 Scheduler 定期调用，检查到期任务并执行。"""
    global _recovered
    if not _recovered:
        _recovered = True
        try:
            _recover_stuck_jobs()
        except Exception as e:
            print(f"[ScheduledRunner] 恢复卡住任务失败: {e}")

    now = datetime.now(timezone.utc)

    with Session(engine) as s:
        jobs = s.exec(
            select(ScheduledJobModel).where(
                ScheduledJobModel.enabled == True,
                ScheduledJobModel.next_run_at != None,
                ScheduledJobModel.next_run_at <= now,
            )
        ).all()

        for job in jobs:
            if job.last_status == "running":
                task_id = job.last_task_id
                still_alive = False
                if task_id:
                    from api.tasks import _tasks, _tasks_lock
                    with _tasks_lock:
                        still_alive = task_id in _tasks
                    if not still_alive:
                        with _cpa_clean_lock:
                            still_alive = task_id in _cpa_clean_tasks
                if not still_alive:
                    job.last_status = "failed"
                    job.next_run_at = compute_next_run(job)
                    s.add(job)
                    s.commit()
                continue
            try:
                task_id = _dispatch_job(job)
                job.last_run_at = now
                job.last_task_id = task_id
                job.last_status = "running"
                job.next_run_at = compute_next_run(job)
                s.add(job)
            except Exception as e:
                print(f"[ScheduledRunner] 任务 {job.id} 执行失败: {e}")
                job.last_status = "failed"
                job.last_run_at = now
                job.next_run_at = compute_next_run(job)
                s.add(job)

        s.commit()


def run_job_now(job_id: int) -> str:
    """手动立即触发一个任务。"""
    with Session(engine) as s:
        job = s.get(ScheduledJobModel, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        task_id = _dispatch_job(job)
        job.last_run_at = datetime.now(timezone.utc)
        job.last_task_id = task_id
        job.last_status = "running"
        s.add(job)
        s.commit()
        return task_id


def update_job_status(task_id: str, success: bool):
    """注册任务完成后回调更新状态并持久化日志。"""
    from api.tasks import _tasks, _tasks_lock

    logs: list[str] = []
    with _tasks_lock:
        task = _tasks.get(task_id)
        if task:
            logs = list(task.get("logs", []))

    _persist_job_result(task_id, success, logs)


def _persist_job_result(task_id: str, success: bool, logs: list[str]):
    with Session(engine) as s:
        job = s.exec(
            select(ScheduledJobModel).where(
                ScheduledJobModel.last_task_id == task_id
            )
        ).first()
        if job:
            job.last_status = "success" if success else "failed"
            job.last_logs_json = _json.dumps(logs, ensure_ascii=False)
            s.add(job)
            s.commit()


# ── 任务分发 ──────────────────────────────────────────────────────

def _dispatch_job(job: ScheduledJobModel) -> str:
    if job.job_type == "cpa_clean":
        return _execute_cpa_clean(job)
    return _execute_register(job)


def _execute_register(job: ScheduledJobModel) -> str:
    """构建 RegisterTaskRequest 并入队执行。"""
    from api.tasks import RegisterTaskRequest, enqueue_register_task
    from core.config_store import config_store

    cfg = config_store.get_all()

    req = RegisterTaskRequest(
        platform=job.platform,
        count=job.count,
        concurrency=job.concurrency,
        register_delay_seconds=job.register_delay_seconds,
        proxy=cfg.get("default_proxy") or None,
        executor_type=cfg.get("default_executor") or "protocol",
        captcha_solver=cfg.get("default_captcha_solver") or "yescaptcha",
        extra=cfg,
    )

    task_id = enqueue_register_task(
        req,
        source="scheduled",
        meta={"scheduled_job_id": job.id},
    )
    print(f"[ScheduledRunner] 触发注册任务: job_id={job.id} task_id={task_id}")
    return task_id


def _execute_cpa_clean(job: ScheduledJobModel) -> str:
    """启动 CPA 检测清理后台线程。"""
    from core.config_store import config_store

    cfg = config_store.get_all()
    api_url = cfg.get("cpa_api_url") or ""
    api_key = cfg.get("cpa_api_key") or ""

    if not api_url:
        raise RuntimeError("CPA API URL 未配置")
    if not api_key:
        raise RuntimeError("CPA API Key 未配置")

    task_id = f"cpa_clean_{int(time.time() * 1000)}"

    with _cpa_clean_lock:
        _cpa_clean_tasks[task_id] = {
            "status": "running",
            "logs": [],
        }

    concurrency = max(1, job.concurrency)

    thread = threading.Thread(
        target=_run_cpa_clean_thread,
        args=(task_id, api_url, api_key, concurrency, job.id),
        daemon=True,
    )
    thread.start()
    print(f"[ScheduledRunner] 触发 CPA 清理: job_id={job.id} task_id={task_id}")
    return task_id


def _run_cpa_clean_thread(task_id: str, api_url: str, api_key: str, concurrency: int, job_id: int):
    logs: list[str] = []

    def _log(msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        logs.append(line)
        with _cpa_clean_lock:
            if task_id in _cpa_clean_tasks:
                _cpa_clean_tasks[task_id]["logs"] = list(logs)
        print(f"[CPA清理] {msg}")

    try:
        from services.cpa_cleaner import run_cpa_clean

        result = run_cpa_clean(
            api_url=api_url,
            api_key=api_key,
            concurrency=concurrency,
            log_fn=_log,
        )
        success = result.get("ok", False)
        with _cpa_clean_lock:
            if task_id in _cpa_clean_tasks:
                _cpa_clean_tasks[task_id]["status"] = "done" if success else "failed"
        _persist_job_result(task_id, success, logs)

    except Exception as e:
        _log(f"清理异常: {e}")
        with _cpa_clean_lock:
            if task_id in _cpa_clean_tasks:
                _cpa_clean_tasks[task_id]["status"] = "failed"
        _persist_job_result(task_id, False, logs)


# ── Cron 表达式解析 ──────────────────────────────────────────────

def _next_cron_time(expr: str, after: datetime) -> Optional[datetime]:
    parts = expr.strip().split()
    if len(parts) != 5:
        return None

    try:
        minute_set = _parse_cron_field(parts[0], 0, 59)
        hour_set = _parse_cron_field(parts[1], 0, 23)
        dom_set = _parse_cron_field(parts[2], 1, 31)
        month_set = _parse_cron_field(parts[3], 1, 12)
        dow_set = _parse_cron_field(parts[4], 0, 6)
    except ValueError:
        return None

    candidate = after + timedelta(minutes=1)
    candidate = candidate.replace(second=0, microsecond=0)

    for _ in range(366 * 24 * 60):
        if (
            candidate.month in month_set
            and candidate.day in dom_set
            and candidate.weekday() in dow_set
            and candidate.hour in hour_set
            and candidate.minute in minute_set
        ):
            return candidate
        candidate += timedelta(minutes=1)

    return None


def _parse_cron_field(field: str, lo: int, hi: int) -> set[int]:
    result: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        step_match = re.match(r"^(\*|\d+-\d+)/(\d+)$", part)
        if step_match:
            rng, step = step_match.group(1), int(step_match.group(2))
            if rng == "*":
                start, end = lo, hi
            else:
                start, end = map(int, rng.split("-"))
            result.update(range(start, end + 1, step))
        elif part == "*":
            result.update(range(lo, hi + 1))
        elif "-" in part:
            a, b = map(int, part.split("-"))
            result.update(range(a, b + 1))
        else:
            result.add(int(part))
    return result
