"""CPA 凭证检测清理：检查 → 禁用 → 删除 合并流程

参考 script/CPA测活/clean_codex.py 实现，
通过 CPA management API 检查 codex auth files 的有效性，
对 401 的执行禁用，再删除所有已禁用的。
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable

import requests


def run_cpa_clean(
    api_url: str,
    api_key: str,
    concurrency: int = 20,
    log_fn: Callable[[str], None] | None = None,
) -> dict:
    """
    执行完整的 CPA 清理流程:
      1. 拉取所有 auth files
      2. 筛选 provider=codex 且未禁用的
      3. 并发检查 quota（通过 api-call 代理请求）
      4. 对 HTTP 401 的禁用
      5. 删除所有已禁用的 codex files

    返回统计摘要 dict。
    """
    log = log_fn or print
    base = api_url.rstrip("/")
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {api_key}",
        "Cache-Control": "no-cache",
    }

    # ── 步骤 1: 拉取 auth files ──
    log("步骤 1/5: 获取 CPA auth files...")
    try:
        resp = requests.get(f"{base}/v0/management/auth-files", headers=headers, timeout=30, verify=False)
        resp.raise_for_status()
        all_files = resp.json().get("files", [])
    except Exception as e:
        log(f"获取 auth files 失败: {e}")
        return {"ok": False, "error": str(e)}

    log(f"  总计 {len(all_files)} 个 auth files")

    # ── 步骤 2: 筛选活跃 codex files ──
    codex_active = [
        f for f in all_files
        if f.get("provider") == "codex" and not f.get("disabled")
    ]
    codex_disabled_before = [
        f for f in all_files
        if f.get("provider") == "codex" and f.get("disabled") is True
    ]
    log(f"步骤 2/5: 筛选出 {len(codex_active)} 个活跃 codex files，{len(codex_disabled_before)} 个已禁用")

    if not codex_active and not codex_disabled_before:
        log("无 codex auth files，流程结束")
        return {"ok": True, "total": len(all_files), "checked": 0, "found_401": 0, "disabled": 0, "deleted": 0}

    # ── 步骤 3: 并发检查 quota ──
    found_401: list[str] = []
    checked = 0

    if codex_active:
        log(f"步骤 3/5: 并发检查 {len(codex_active)} 个文件 (并发={concurrency})...")

        def _check_one(file_info: dict) -> tuple[str, int]:
            fid = file_info["id"]
            auth_index = file_info.get("auth_index", 0)
            account_id = file_info.get("id_token", {}).get("chatgpt_account_id", "")
            payload = {
                "authIndex": auth_index,
                "method": "GET",
                "url": "https://chatgpt.com/backend-api/wham/usage",
                "header": {
                    "Authorization": "Bearer $TOKEN$",
                    "Content-Type": "application/json",
                    "User-Agent": "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal",
                    "Chatgpt-Account-Id": account_id,
                },
            }
            try:
                r = requests.post(
                    f"{base}/v0/management/api-call",
                    headers={**headers, "Content-Type": "application/json"},
                    json=payload,
                    timeout=30,
                    verify=False,
                )
                r.raise_for_status()
                status_code = r.json().get("status_code", -1)
                return fid, status_code
            except Exception:
                return fid, -1

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(_check_one, f): f for f in codex_active}
            for future in as_completed(futures):
                fid, code = future.result()
                checked += 1
                if code == 401:
                    found_401.append(fid)
                    log(f"  [{checked}/{len(codex_active)}] {fid} → 401 (无效)")
                else:
                    log(f"  [{checked}/{len(codex_active)}] {fid} → {code}")

        log(f"  检查完成: {len(found_401)}/{len(codex_active)} 个返回 401")
    else:
        log("步骤 3/5: 无活跃 codex files 需要检查")

    # ── 步骤 4: 禁用 401 files ──
    disabled_count = 0
    if found_401:
        log(f"步骤 4/5: 禁用 {len(found_401)} 个无效文件...")
        for fid in found_401:
            try:
                r = requests.patch(
                    f"{base}/v0/management/auth-files/status",
                    headers={**headers, "Content-Type": "application/json"},
                    json={"name": fid, "disabled": True},
                    timeout=15,
                    verify=False,
                )
                r.raise_for_status()
                if r.json().get("status") == "ok":
                    disabled_count += 1
                    log(f"  禁用成功: {fid}")
                else:
                    log(f"  禁用异常: {fid} → {r.text[:200]}")
            except Exception as e:
                log(f"  禁用失败: {fid} → {e}")
        log(f"  禁用完成: {disabled_count}/{len(found_401)}")
    else:
        log("步骤 4/5: 无需禁用")

    # ── 步骤 5: 删除所有已禁用的 codex files ──
    all_to_delete = [f["id"] for f in codex_disabled_before] + found_401
    unique_to_delete = list(dict.fromkeys(all_to_delete))
    deleted_count = 0

    if unique_to_delete:
        log(f"步骤 5/5: 删除 {len(unique_to_delete)} 个已禁用文件...")
        for fid in unique_to_delete:
            try:
                r = requests.delete(
                    f"{base}/v0/management/auth-files",
                    headers=headers,
                    params={"name": fid},
                    timeout=15,
                    verify=False,
                )
                r.raise_for_status()
                if r.json().get("status") == "ok":
                    deleted_count += 1
                    log(f"  删除成功: {fid}")
                else:
                    log(f"  删除异常: {fid} → {r.text[:200]}")
            except Exception as e:
                log(f"  删除失败: {fid} → {e}")
        log(f"  删除完成: {deleted_count}/{len(unique_to_delete)}")
    else:
        log("步骤 5/5: 无需删除")

    summary = {
        "ok": True,
        "total": len(all_files),
        "checked": checked,
        "found_401": len(found_401),
        "disabled": disabled_count,
        "deleted": deleted_count,
    }
    log(f"═══ 清理完成: 检查 {checked}, 发现401 {len(found_401)}, 禁用 {disabled_count}, 删除 {deleted_count} ═══")
    return summary
