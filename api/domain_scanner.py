"""域名可用性扫描 API"""

from fastapi import APIRouter
from pydantic import BaseModel

from core.config_store import config_store
from core.domain_scanner import scanner, get_scan_results, get_accepted_domains

router = APIRouter(prefix="/domain-scanner", tags=["domain-scanner"])


class ScanRequest(BaseModel):
    delay: float = 3.0


def _parse_preferred_domains() -> set[str]:
    """解析当前配置的偏好白名单域名。"""
    raw = config_store.get("maliapi_preferred_domains") or ""
    result: set[str] = set()
    for item in raw.replace("\n", ",").split(","):
        d = item.strip()
        if d:
            result.add(d)
    return result


@router.post("/scan")
def start_scan(body: ScanRequest):
    """启动域名扫描（后台执行）。"""
    if scanner.state == "running":
        return {"ok": False, "message": "扫描正在进行中"}

    cfg = config_store.get_all()
    base_url = cfg.get("maliapi_base_url", "https://maliapi.215.im/v1")
    api_key = cfg.get("maliapi_api_key", "")
    proxy = cfg.get("default_proxy", "")

    if not api_key:
        return {"ok": False, "message": "未配置 maliapi_api_key"}

    excluded = _parse_preferred_domains()

    scanner.start_scan(
        maliapi_base_url=base_url,
        maliapi_api_key=api_key,
        proxy=proxy or None,
        delay=body.delay,
        excluded_domains=excluded,
    )
    return {"ok": True, "message": "扫描已启动", "excluded_count": len(excluded)}


@router.get("/status")
def scan_status():
    """获取扫描状态和进度。"""
    return {
        "state": scanner.state,
        "progress": scanner.progress,
        "logs": scanner.logs[-50:],
    }


@router.get("/results")
def scan_results(status: str = ""):
    """获取扫描结果列表。"""
    return get_scan_results(status_filter=status)


@router.post("/apply")
def apply_accepted():
    """将 accepted 域名追加到 maliapi_preferred_domains（不覆盖已有的）。"""
    new_domains = get_accepted_domains()
    if not new_domains:
        return {"ok": False, "message": "没有可用域名", "count": 0}

    existing = _parse_preferred_domains()
    merged = sorted(existing | set(new_domains))

    config_store.set("maliapi_preferred_domains", ",".join(merged))
    added = len(merged) - len(existing)
    return {
        "ok": True,
        "added": added,
        "total": len(merged),
        "domains": merged,
    }
