"""
域名可用性扫描器

两阶段探测：
  阶段 1（轻量）：仅走 OAuth 步骤 1-5（提交邮箱），过滤 OpenAI 直接拒绝的域名。
                  不创建邮箱、不收验证码、不创建账号。
  阶段 2（完整）：对阶段 1 通过的域名，走完整 12 步注册流程。
                  注册成功 → 标记 accepted + 保存账号；失败 → 标记具体失败原因。
"""

import json
import random
import string
import time
import threading
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Callable

from curl_cffi import requests as curl_requests
from sqlmodel import Field, SQLModel, Session, select

from .db import engine

AUTH_BASE = "https://auth.openai.com"
SENTINEL_API = "https://sentinel.openai.com/backend-api/sentinel/req"
SENTINEL_REFERER = (
    "https://sentinel.openai.com/backend-api/sentinel/frame.html?sv=20260219f9f6"
)


# ── 数据模型 ──────────────────────────────────────────────────────

class DomainScanResult(SQLModel, table=True):
    __tablename__ = "domain_scan_results"

    id: Optional[int] = Field(default=None, primary_key=True)
    domain: str = Field(index=True)
    status: str = ""          # accepted / rejected / register_failed / error
    error_message: str = ""
    fail_step: int = 0        # 失败发生在第几步 (0=未知)
    response_page_type: str = ""
    scanned_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class ScanState(str, Enum):
    IDLE = "idle"
    RUNNING = "running"


# ── 阶段 1：轻量探测（步骤 1-5） ─────────────────────────────────

def _random_prefix(length: int = 10) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def _fetch_sentinel_token(
    device_id: str, proxies: dict | None = None
) -> str:
    req_body = json.dumps({"p": "", "id": device_id, "flow": "authorize_continue"})
    resp = curl_requests.post(
        SENTINEL_API,
        headers={
            "origin": "https://sentinel.openai.com",
            "referer": SENTINEL_REFERER,
            "content-type": "text/plain;charset=UTF-8",
        },
        data=req_body,
        proxies=proxies,
        impersonate="chrome",
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Sentinel HTTP {resp.status_code}")
    c_value = resp.json().get("token", "")
    if not c_value:
        raise RuntimeError("Sentinel 响应缺少 token")
    return json.dumps(
        {"p": "", "t": "", "c": c_value, "id": device_id, "flow": "authorize_continue"},
        separators=(",", ":"),
    )


def _light_probe(
    domain: str,
    proxy: str | None = None,
    log_fn: Callable[[str], None] | None = None,
) -> tuple[bool, str]:
    """轻量探测：步骤 2-5，返回 (是否通过, 错误信息)。"""
    log = log_fn or (lambda _: None)
    proxies = {"http": proxy, "https": proxy} if proxy else None
    fake_email = f"{_random_prefix()}@{domain}"

    try:
        session = curl_requests.Session(proxies=proxies, impersonate="chrome")
        from platforms.chatgpt.oauth import generate_oauth_url

        oauth = generate_oauth_url()
        session.get(oauth.auth_url, timeout=15)
        device_id = session.cookies.get("oai-did") or ""
        if not device_id:
            return False, "未获取到 oai-did"

        sentinel = _fetch_sentinel_token(device_id, proxies)

        payload = json.dumps({
            "username": {"value": fake_email, "kind": "email"},
            "screen_hint": "signup",
        })
        resp = session.post(
            f"{AUTH_BASE}/api/accounts/authorize/continue",
            headers={
                "referer": f"{AUTH_BASE}/create-account",
                "accept": "application/json",
                "content-type": "application/json",
                "openai-sentinel-token": sentinel,
            },
            data=payload,
            timeout=30,
        )

        if resp.status_code == 200:
            log(f"  [探测] {domain} → 步骤5通过")
            return True, ""
        else:
            error_text = resp.text[:300]
            log(f"  [探测] {domain} → 被拒 (HTTP {resp.status_code})")
            return False, f"HTTP {resp.status_code}: {error_text}"

    except Exception as e:
        log(f"  [探测] {domain} → 异常: {e}")
        return False, str(e)[:500]


# ── 阶段 2：完整注册 ──────────────────────────────────────────────

def _full_register(
    domain: str,
    maliapi_base_url: str,
    maliapi_api_key: str,
    proxy: str | None = None,
    log_fn: Callable[[str], None] | None = None,
) -> tuple[bool, str, int]:
    """
    对指定域名执行完整注册。
    返回 (是否成功, 错误信息, 失败步骤号)。
    成功时账号已保存到数据库。
    """
    log = log_fn or (lambda _: None)

    try:
        from core.base_mailbox import MaliAPIMailbox
        from core.base_platform import RegisterConfig
        from platforms.chatgpt.plugin import ChatGPTPlatform

        mailbox = MaliAPIMailbox(
            api_url=maliapi_base_url,
            api_key=maliapi_api_key,
            preferred_domains=domain,
            proxy=None,
        )
        mailbox._log_fn = log

        config = RegisterConfig(
            executor_type="protocol",
            captcha_solver="",
            proxy=proxy,
            extra={"register_max_retries": 1},
        )
        platform = ChatGPTPlatform(config=config, mailbox=mailbox)
        platform._log_fn = log
        if getattr(platform, "mailbox", None) is not None:
            platform.mailbox._log_fn = log

        account = platform.register(email=None, password="AAb1234567890!")

        from core.db import save_account
        save_account(account)

        log(f"  [注册] {domain} → 成功: {account.email}")
        return True, "", 0

    except Exception as e:
        error = str(e)
        fail_step = _guess_fail_step(error)
        log(f"  [注册] {domain} → 失败(步骤{fail_step}): {error[:200]}")
        return False, error[:500], fail_step


def _guess_fail_step(error: str) -> int:
    """从错误信息推断失败步骤。"""
    e = error.lower()
    if "ip" in e and ("地区" in e or "region" in e):
        return 1
    if "邮箱" in e and ("创建" in e or "mailapi" in e.lower()):
        return 2
    if "oai-did" in e or "oauth" in e:
        return 3
    if "sentinel" in e:
        return 4
    if "提交邮箱" in e or "email" in e and "support" in e:
        return 5
    if "密码" in e or "password" in e:
        return 6
    if "验证码" in e or "otp" in e:
        return 8
    if "registration_disallowed" in e or "create_account" in e or "创建账户" in e:
        return 9
    if "workspace" in e:
        return 11
    if "redirect" in e or "code=" in e or "token" in e.lower():
        return 12
    return 0


# ── 批量扫描器 ────────────────────────────────────────────────────

class DomainScanner:
    """管理扫描状态、进度和结果持久化。"""

    def __init__(self):
        self._state = ScanState.IDLE
        self._lock = threading.Lock()
        self._progress: dict = {
            "total": 0, "done": 0,
            "accepted": 0, "rejected": 0, "register_failed": 0, "error": 0,
            "phase": "",
        }
        self._logs: list[str] = []
        self._thread: threading.Thread | None = None

    @property
    def state(self) -> str:
        return self._state.value

    @property
    def progress(self) -> dict:
        return dict(self._progress)

    @property
    def logs(self) -> list[str]:
        return list(self._logs)

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self._logs.append(line)
        print(f"[DomainScanner] {msg}")

    def start_scan(
        self,
        maliapi_base_url: str,
        maliapi_api_key: str,
        proxy: str | None = None,
        delay: float = 3.0,
        excluded_domains: set[str] | None = None,
    ) -> bool:
        with self._lock:
            if self._state == ScanState.RUNNING:
                return False
            self._state = ScanState.RUNNING
            self._progress = {
                "total": 0, "done": 0,
                "accepted": 0, "rejected": 0, "register_failed": 0, "error": 0,
                "phase": "init",
            }
            self._logs = []

        self._thread = threading.Thread(
            target=self._run_scan,
            args=(maliapi_base_url, maliapi_api_key, proxy, delay, excluded_domains or set()),
            daemon=True,
        )
        self._thread.start()
        return True

    def _run_scan(
        self,
        maliapi_base_url: str,
        maliapi_api_key: str,
        proxy: str | None,
        delay: float,
        excluded_domains: set[str],
    ):
        try:
            # ── 获取域名列表 ──
            self._log("获取 YYDS Mail 域名列表...")
            all_domains = self._fetch_yyds_domains(maliapi_base_url, maliapi_api_key)
            if not all_domains:
                self._log("未获取到可用域名，扫描终止")
                return

            # ── 排除已有偏好域名 ──
            if excluded_domains:
                before = len(all_domains)
                all_domains = [d for d in all_domains if d not in excluded_domains]
                skipped = before - len(all_domains)
                if skipped:
                    self._log(f"跳过 {skipped} 个已在偏好白名单中的域名")

            if not all_domains:
                self._log("排除后无待扫描域名")
                return

            self._progress["total"] = len(all_domains)

            # ── 阶段 1：轻量探测 ──
            self._progress["phase"] = "probe"
            self._log(f"═══ 阶段 1: 轻量探测 ({len(all_domains)} 个域名) ═══")

            probe_passed: list[str] = []
            results: list[DomainScanResult] = []

            for i, domain in enumerate(all_domains):
                passed, error = _light_probe(domain, proxy=proxy, log_fn=self._log)

                r = DomainScanResult(
                    domain=domain,
                    scanned_at=datetime.now(timezone.utc),
                )
                if passed:
                    r.status = "probe_passed"
                    probe_passed.append(domain)
                else:
                    r.status = "rejected"
                    r.error_message = error
                    r.fail_step = 5
                    self._progress["rejected"] += 1

                results.append(r)
                self._progress["done"] = i + 1

                if i < len(all_domains) - 1:
                    time.sleep(delay)

            self._log(
                f"阶段 1 完成: {len(probe_passed)}/{len(all_domains)} 个通过, "
                f"{self._progress['rejected']} 个被拒"
            )

            if not probe_passed:
                self._log("无域名通过轻量探测，扫描结束")
                self._save_results(results)
                return

            # ── 阶段 2：完整注册验证 ──
            self._progress["phase"] = "register"
            self._progress["done"] = 0
            self._progress["total"] = len(probe_passed)
            self._log(f"═══ 阶段 2: 完整注册验证 ({len(probe_passed)} 个域名) ═══")

            for i, domain in enumerate(probe_passed):
                self._log(f"[{i+1}/{len(probe_passed)}] 尝试注册: {domain}")
                success, error, fail_step = _full_register(
                    domain=domain,
                    maliapi_base_url=maliapi_base_url,
                    maliapi_api_key=maliapi_api_key,
                    proxy=proxy,
                    log_fn=self._log,
                )

                r = next((x for x in results if x.domain == domain), None)
                if r:
                    r.scanned_at = datetime.now(timezone.utc)
                    if success:
                        r.status = "accepted"
                        r.error_message = ""
                        r.fail_step = 0
                        self._progress["accepted"] += 1
                    else:
                        r.status = "register_failed"
                        r.error_message = error
                        r.fail_step = fail_step
                        self._progress["register_failed"] += 1

                self._progress["done"] = i + 1

                if i < len(probe_passed) - 1:
                    time.sleep(delay)

            self._save_results(results)
            self._log(
                f"扫描完成: "
                f"{self._progress['accepted']} 个注册成功, "
                f"{self._progress['register_failed']} 个注册失败, "
                f"{self._progress['rejected']} 个探测被拒"
            )

        except Exception as e:
            self._log(f"扫描异常终止: {e}")
        finally:
            with self._lock:
                self._state = ScanState.IDLE

    def _fetch_yyds_domains(self, base_url: str, api_key: str) -> list[str]:
        import requests

        headers = {"accept": "application/json"}
        if api_key:
            headers["X-API-Key"] = api_key

        resp = requests.get(
            f"{base_url.rstrip('/')}/domains",
            headers=headers,
            timeout=15,
        )
        if resp.status_code != 200:
            self._log(f"获取域名列表失败: HTTP {resp.status_code}")
            return []

        data = resp.json()
        raw_list = data if isinstance(data, list) else data.get("data", [])
        names = []
        for d in raw_list:
            if isinstance(d, str):
                names.append(d)
            elif isinstance(d, dict):
                name = str(d.get("domain") or d.get("name") or "").strip()
                if name:
                    names.append(name)
        return names

    def _save_results(self, results: list[DomainScanResult]):
        with Session(engine) as session:
            for r in results:
                existing = session.exec(
                    select(DomainScanResult).where(
                        DomainScanResult.domain == r.domain
                    )
                ).first()
                if existing:
                    existing.status = r.status
                    existing.error_message = r.error_message
                    existing.fail_step = r.fail_step
                    existing.response_page_type = r.response_page_type
                    existing.scanned_at = r.scanned_at
                    session.add(existing)
                else:
                    session.add(r)
            session.commit()


# 全局单例
scanner = DomainScanner()


def get_scan_results(status_filter: str = "") -> list[dict]:
    """从数据库读取扫描结果。"""
    with Session(engine) as session:
        stmt = select(DomainScanResult).order_by(
            DomainScanResult.status, DomainScanResult.domain
        )
        if status_filter:
            stmt = stmt.where(DomainScanResult.status == status_filter)
        rows = session.exec(stmt).all()
        return [
            {
                "domain": r.domain,
                "status": r.status,
                "error_message": r.error_message,
                "fail_step": r.fail_step,
                "page_type": r.response_page_type,
                "scanned_at": r.scanned_at.isoformat() if r.scanned_at else "",
            }
            for r in rows
        ]


def get_accepted_domains() -> list[str]:
    """返回所有 accepted 状态的域名列表。"""
    with Session(engine) as session:
        rows = session.exec(
            select(DomainScanResult).where(DomainScanResult.status == "accepted")
        ).all()
        return [r.domain for r in rows]
