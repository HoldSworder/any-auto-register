"""定时任务 API

支持两种任务类型：
  - register: 定时注册（邮箱/执行器/代理等走全局配置）
  - cpa_clean: CPA 检测清理（检查 → 禁用 → 删除）
"""

import json as _json
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from core.db import ScheduledJobModel, engine

router = APIRouter(prefix="/scheduled-jobs", tags=["scheduled-jobs"])


class JobCreate(BaseModel):
    name: str = ""
    enabled: bool = True
    job_type: str = "register"    # register / cpa_clean
    platform: str = "chatgpt"
    count: int = 1
    concurrency: int = 1
    register_delay_seconds: float = 0.0
    cron_expr: str = ""
    interval_minutes: int = 0


class JobUpdate(BaseModel):
    name: Optional[str] = None
    enabled: Optional[bool] = None
    job_type: Optional[str] = None
    platform: Optional[str] = None
    count: Optional[int] = None
    concurrency: Optional[int] = None
    register_delay_seconds: Optional[float] = None
    cron_expr: Optional[str] = None
    interval_minutes: Optional[int] = None


def _to_dict(job: ScheduledJobModel) -> dict:
    return {
        "id": job.id,
        "name": job.name,
        "enabled": job.enabled,
        "job_type": job.job_type,
        "platform": job.platform,
        "count": job.count,
        "concurrency": job.concurrency,
        "register_delay_seconds": job.register_delay_seconds,
        "cron_expr": job.cron_expr,
        "interval_minutes": job.interval_minutes,
        "last_run_at": job.last_run_at.isoformat() if job.last_run_at else None,
        "next_run_at": job.next_run_at.isoformat() if job.next_run_at else None,
        "last_task_id": job.last_task_id,
        "last_status": job.last_status,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }


def _default_name(body: JobCreate) -> str:
    if body.name:
        return body.name
    if body.job_type == "cpa_clean":
        return "CPA 检测清理"
    return f"{body.platform} 定时注册"


@router.get("")
def list_jobs():
    with Session(engine) as s:
        jobs = s.exec(
            select(ScheduledJobModel).order_by(ScheduledJobModel.id)
        ).all()
        return [_to_dict(j) for j in jobs]


@router.post("")
def create_job(body: JobCreate):
    job = ScheduledJobModel(
        name=_default_name(body),
        enabled=body.enabled,
        job_type=body.job_type,
        platform=body.platform,
        count=body.count,
        concurrency=body.concurrency,
        register_delay_seconds=body.register_delay_seconds,
        cron_expr=body.cron_expr,
        interval_minutes=body.interval_minutes,
    )
    _compute_next_run(job)
    with Session(engine) as s:
        s.add(job)
        s.commit()
        s.refresh(job)
        return _to_dict(job)


@router.put("/{job_id}")
def update_job(job_id: int, body: JobUpdate):
    with Session(engine) as s:
        job = s.get(ScheduledJobModel, job_id)
        if not job:
            raise HTTPException(404, "Job not found")
        if body.name is not None:
            job.name = body.name
        if body.enabled is not None:
            job.enabled = body.enabled
        if body.job_type is not None:
            job.job_type = body.job_type
        if body.platform is not None:
            job.platform = body.platform
        if body.count is not None:
            job.count = body.count
        if body.concurrency is not None:
            job.concurrency = body.concurrency
        if body.register_delay_seconds is not None:
            job.register_delay_seconds = body.register_delay_seconds
        if body.cron_expr is not None:
            job.cron_expr = body.cron_expr
        if body.interval_minutes is not None:
            job.interval_minutes = body.interval_minutes
        job.updated_at = datetime.now(timezone.utc)
        _compute_next_run(job)
        s.add(job)
        s.commit()
        s.refresh(job)
        return _to_dict(job)


@router.delete("/{job_id}")
def delete_job(job_id: int):
    with Session(engine) as s:
        job = s.get(ScheduledJobModel, job_id)
        if not job:
            raise HTTPException(404, "Job not found")
        s.delete(job)
        s.commit()
        return {"ok": True}


@router.post("/{job_id}/trigger")
def trigger_job(job_id: int):
    """手动触发一次定时任务。"""
    with Session(engine) as s:
        job = s.get(ScheduledJobModel, job_id)
        if not job:
            raise HTTPException(404, "Job not found")

    from core.scheduled_runner import run_job_now
    task_id = run_job_now(job_id)
    return {"ok": True, "task_id": task_id}


@router.get("/{job_id}/logs")
def get_job_logs(job_id: int):
    """获取定时任务上次执行的日志。优先从内存读取（实时），回退到持久化日志。"""
    with Session(engine) as s:
        job = s.get(ScheduledJobModel, job_id)
        if not job:
            raise HTTPException(404, "Job not found")

        task_id = job.last_task_id
        result = {
            "task_id": task_id,
            "status": job.last_status,
            "logs": [],
            "error": "",
            "success": None,
            "errors": [],
            "source": "persisted",
        }

        if task_id:
            from api.tasks import _tasks, _tasks_lock
            with _tasks_lock:
                task = _tasks.get(task_id)
            if task:
                result["logs"] = task.get("logs", [])
                result["status"] = task.get("status", job.last_status)
                result["error"] = task.get("error", "")
                result["success"] = task.get("success")
                result["errors"] = task.get("errors", [])
                result["source"] = "live"
                return result

            from core.scheduled_runner import _cpa_clean_tasks, _cpa_clean_lock
            with _cpa_clean_lock:
                cpa_task = _cpa_clean_tasks.get(task_id)
            if cpa_task:
                result["logs"] = cpa_task.get("logs", [])
                result["status"] = cpa_task.get("status", job.last_status)
                result["source"] = "live"
                return result

        try:
            result["logs"] = _json.loads(job.last_logs_json or "[]")
        except Exception:
            result["logs"] = []

        if job.last_status == "running":
            result["status"] = "failed"
            if not result["logs"]:
                result["logs"] = ["任务状态异常（可能因后端重启丢失），已自动恢复"]

        return result


def _compute_next_run(job: ScheduledJobModel):
    from core.scheduled_runner import compute_next_run
    job.next_run_at = compute_next_run(job)
