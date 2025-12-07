from collections import deque
import os
import random
import re
import time
import html as _html
from datetime import datetime
from typing import Iterable, Optional, Tuple

import requests
from requests.cookies import create_cookie

import undetected_chromedriver as uc

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from storage.profile import Profile


DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///vk.sqlite")
CHROME_VERSION_MAIN = 140
CF_WAIT_TIMEOUT = 20
PER_PAGE_TIMEOUT = 2
PAGE_LOAD_TIMEOUT = 20
DATE_FORMATS = ["%d.%m.%Y", "%d.%m.%y", "%d-%m-%Y", "%d/%m/%Y"]
CONTENT_SELECTORS = [
    ".profile",
    "#account",
    "main",
    "article",
    "#content",
    ".page-content",
]
COMMIT_EVERY = 10
REQUEST_TIMEOUT = 20
REQUEST_MAX_RETRIES = 4
REQUEST_BACKOFF_FACTOR = 0.8
REQUEST_JITTER = 0.25
REQUEST_MIN_INTERVAL = 0.25
BASE_REQUEST_MIN_INTERVAL = 0.5

MAX_REQUEST_MIN_INTERVAL = 5.0
MIN_REQUEST_MIN_INTERVAL = 0.1

BACKOFF_MULTIPLIER = 1.5

DECAY_AFTER = 60.0

DECAY_FACTOR = 0.85

THROTTLE_JITTER = 0.15


_current_min_interval = BASE_REQUEST_MIN_INTERVAL
_last_backoff_time = 0.0
_last_request_time = 0.0


engine = sa.create_engine(DATABASE_URL, future=True)
Session = sessionmaker(bind=engine)

FIND_JS = """
const sels = arguments[0];
for (let i = 0; i < sels.length; ++i) {
    try {
        const el = document.querySelector(sels[i]);
        if (el) {
            return {selector: sels[i], outer: el.outerHTML, text: el.innerText || el.textContent || ""};
        }
    } catch(e) { /* ignore invalid selector */ }
}
return null;
"""


def normalize_text_for_search(s: str) -> str:
    if not s:
        return ""
    s = _html.unescape(s)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()


_bot_regexes = {
    "is_bot": re.compile(r"получить\s+карточку\s+бота", flags=re.I | re.U),
    "not_bot": re.compile(
        r"этот\s+аккаунт\s+не\s+помечен\s+как\s+бот", flags=re.I | re.U
    ),
}


def determine_is_bot_from_content(raw_html: str, normalized_text: str) -> Optional[int]:
    if "/card/" in (raw_html or ""):
        if "карточк" in normalized_text or "бот" in normalized_text:
            return 1
    if _bot_regexes["is_bot"].search(normalized_text):
        return 1
    if _bot_regexes["not_bot"].search(normalized_text):
        return 0
    return None


def _increase_min_interval():
    global _current_min_interval, _last_backoff_time
    new_val = min(MAX_REQUEST_MIN_INTERVAL, _current_min_interval * BACKOFF_MULTIPLIER)
    if new_val > _current_min_interval:
        _current_min_interval = new_val
        _last_backoff_time = time.perf_counter()
        print(f"[throttle] increased min_interval -> {_current_min_interval:.2f}s")


def _decay_min_interval():
    global _current_min_interval, _last_backoff_time
    now = time.perf_counter()
    if (
        _current_min_interval > BASE_REQUEST_MIN_INTERVAL
        and (now - _last_backoff_time) > DECAY_AFTER
    ):
        new_val = max(BASE_REQUEST_MIN_INTERVAL, _current_min_interval * DECAY_FACTOR)
        if new_val < _current_min_interval - 1e-6:
            _current_min_interval = new_val
            _last_backoff_time = now
            print(f"[throttle] decayed min_interval -> {_current_min_interval:.2f}s")


def requests_get_with_retries(
    session: requests.Session,
    url: str,
    max_retries: int = REQUEST_MAX_RETRIES,
    backoff_factor: float = REQUEST_BACKOFF_FACTOR,
    jitter: float = REQUEST_JITTER,
    timeout: int = REQUEST_TIMEOUT,
) -> Optional[requests.Response]:
    last_exc = None
    for attempt in range(1, max_retries + 1):
        throttle_requests()
        try:
            resp = session.get(url, timeout=timeout)
            body = resp.text or ""

            if resp.status_code == 200 and not (
                "Checking your browser" in body
                or "Just a moment" in body
                or ("Cloudflare" in body and "Checking" in body)
            ):
                return resp

            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    wait_sec = int(float(ra))
                except Exception:
                    wait_sec = 5
                print(f"[requests] server asked Retry-After={wait_sec}s for url={url}")

                _increase_min_interval()
                time.sleep(wait_sec)
                continue

            if resp.status_code in (429, 503, 500, 502, 504, 403):
                _increase_min_interval()
                wait = backoff_factor * (2 ** (attempt - 1)) + random.uniform(0, jitter)
                wait = min(wait, 30.0)
                print(
                    f"[requests retry] url={url} status={resp.status_code} attempt={attempt}/{max_retries}, sleeping {wait:.2f}s"
                )
                time.sleep(wait)
                continue

            return resp

        except Exception as e:
            last_exc = e

            _increase_min_interval()
            wait = backoff_factor * (2 ** (attempt - 1)) + random.uniform(0, jitter)
            wait = min(wait, 30.0)
            print(
                f"[requests error] attempt={attempt}/{max_retries} url={url} exc={e} sleeping {wait:.2f}s"
            )
            time.sleep(wait)
            continue

    print(
        f"[requests fail] all {max_retries} attempts failed for url={url}; last_exc={last_exc}"
    )
    return None


def throttle_requests():
    global _last_request_time

    _decay_min_interval()

    now = time.perf_counter()
    wait_for = (
        _current_min_interval
        + random.uniform(0, THROTTLE_JITTER)
        - (now - _last_request_time)
    )
    if wait_for > 0:
        max_wait = max(3.0, _current_min_interval * 4)
        wait_for = min(wait_for, max_wait)
        time.sleep(wait_for)
    _last_request_time = time.perf_counter()


def parse_date_string_to_date(s: Optional[str]):
    if not s:
        return None
    s = s.strip().replace("\u00a0", " ")
    s = _html.unescape(s)
    s = re.sub(r"\s+", " ", s)
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date()
        except Exception:
            continue
    return None


def wait_for_cf_clearance(driver, timeout=CF_WAIT_TIMEOUT) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            names = {c["name"] for c in driver.get_cookies()}
            if "cf_clearance" in names or "__cf_bm" in names:
                return True
            src = driver.page_source or ""
            if "Checking your browser" not in src and "Just a moment" not in src:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def find_content_fast(
    driver, selectors: Iterable[str], timeout=PER_PAGE_TIMEOUT, poll_interval=0.12
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    end = time.time() + timeout
    while time.time() < end:
        try:
            state = driver.execute_script("return document.readyState;")
            if state in ("interactive", "complete"):
                res = driver.execute_script(FIND_JS, list(selectors))
                if res:
                    return res["outer"], res["text"], res["selector"]
        except Exception:
            pass
        time.sleep(poll_interval)
    try:
        outer = driver.execute_script("return document.documentElement.outerHTML;")
        return outer, None, None
    except Exception:
        return None, None, None


def extract_registration_from_dom(driver) -> Optional[str]:
    js = r"""
    const nodes = document.querySelectorAll('div, span, p');
    for (const n of nodes) {
        try {
            const txt = (n.innerText || n.textContent || "").trim();
            if (!txt) continue;
            if (/Регистрация/i.test(txt)) {
                const b = n.querySelector('b');
                if (b && (b.innerText||b.textContent).trim()) {
                    return (b.innerText || b.textContent).trim();
                }
                const m = txt.match(/(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4})/);
                if (m) return m[1];
            }
        } catch(e) { /* ignore */ }
    }
    return null;
    """
    try:
        return driver.execute_script(js)
    except Exception:
        return None


def create_requests_session_from_driver(driver) -> requests.Session:
    s = requests.Session()

    s.cookies.clear()
    try:
        for c in driver.get_cookies():
            name = c.get("name")
            value = c.get("value")
            domain = c.get("domain") or None
            path = c.get("path") or "/"
            secure = bool(c.get("secure"))
            http_only = bool(c.get("httpOnly"))
            cookie = create_cookie(
                name=name,
                value=value,
                domain=domain,
                path=path,
                secure=secure,
                rest={"HttpOnly": http_only},
            )
            s.cookies.set_cookie(cookie)
    except Exception:
        pass

    try:
        ua = driver.execute_script("return navigator.userAgent;")
    except Exception:
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140 Safari/537.36"

    try:
        lang = driver.execute_script(
            "return navigator.language || navigator.languages && navigator.languages[0] || ''"
        )
    except Exception:
        lang = "ru-RU"
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": lang,
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://botnadzor.org/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
    }
    s.headers.update(headers)

    try:
        ls = driver.execute_script(
            "return window.localStorage ? JSON.stringify(window.localStorage) : null;"
        )
        if ls:
            s.headers.setdefault("X-LocalStorage", ls[:2000])
    except Exception:
        pass

    return s


def update_session_cookies_from_driver(driver, session: requests.Session):
    session.cookies.clear()
    for c in driver.get_cookies():
        try:
            cookie = create_cookie(
                name=c.get("name"),
                value=c.get("value"),
                domain=c.get("domain") or None,
                path=c.get("path") or "/",
                secure=bool(c.get("secure")),
                rest={"HttpOnly": bool(c.get("httpOnly"))},
            )
            session.cookies.set_cookie(cookie)
        except Exception:
            session.cookies.set(
                c.get("name"), c.get("value"), path=c.get("path") or "/"
            )


def process_profiles():
    print("Initializing Chrome driver...")
    driver = uc.Chrome(version_main=CHROME_VERSION_MAIN, use_subprocess=True)
    session_db = Session()
    updated = 0

    processed_timestamps = deque()
    start_time = time.time()
    total_done = 0

    try:
        q = (
            session_db.query(Profile)
            .filter(Profile.user_id != None)
            .filter(Profile.is_bot == None)
        )

        base = "https://botnadzor.org/"
        print("Opening base URL to solve Cloudflare...")
        driver.get(base)
        if not wait_for_cf_clearance(driver):
            print("Warning: timeout waiting CF clearance, continuing anyway.")

        reqs = create_requests_session_from_driver(driver)
        print("Created requests.Session with cookies:", reqs.cookies)

        total = 0
        for profile in q.yield_per(50):
            uid = profile.user_id
            if uid is None:
                continue
            total += 1
            t0 = time.perf_counter()
            url = f"https://botnadzor.org/account/id{uid}"
            print(f"[{total}] Trying requests for uid={uid} ...")
            r = requests_get_with_retries(
                reqs,
                url,
                max_retries=REQUEST_MAX_RETRIES,
                backoff_factor=REQUEST_BACKOFF_FACTOR,
                jitter=REQUEST_JITTER,
                timeout=REQUEST_TIMEOUT,
            )

            need_selenium = False
            raw_content = ""
            text_content = ""

            if r is None:
                need_selenium = True
            else:
                status = r.status_code
                body = r.text or ""

                if status != 200:
                    need_selenium = True
                elif (
                    ("Checking your browser" in body)
                    or ("Just a moment" in body)
                    or ("Cloudflare" in body and "Checking" in body)
                ):
                    need_selenium = True
                else:
                    raw_content = body

                    text_content = normalize_text_for_search(
                        re.sub(r"<[^>]+>", " ", body)
                    )

            if need_selenium:
                print(f"[{uid}] requests blocked or non-200 -> using Selenium fallback")
                try:
                    driver.get(url)
                except Exception as e:
                    print(f"[{uid}] driver.get warning: {e}")

                wait_for_cf_clearance(driver, timeout=6)

                html, text, sel = find_content_fast(
                    driver, CONTENT_SELECTORS, timeout=PER_PAGE_TIMEOUT
                )
                raw_content = html or ""

                text_content = normalize_text_for_search(text or raw_content)

                update_session_cookies_from_driver(driver, reqs)
                print(
                    f"[{uid}] updated requests.Session cookies after selenium fallback"
                )
            else:
                sel = None

            reg_str = None

            if need_selenium:
                reg_str = extract_registration_from_dom(driver)

            if not reg_str:
                reg_str = None

                if raw_content:
                    s = _html.unescape(raw_content)
                    s = s.replace("\u00a0", " ")
                    m = re.search(
                        r"[Рр]егистрация[^\d]*(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4})", s
                    )
                    if m:
                        reg_str = m.group(1)

            reg_date = parse_date_string_to_date(reg_str)

            if reg_date:
                if getattr(profile, "registered_at", None) != reg_date:
                    profile.registered_at = reg_date
                    profile.updated_at = int(time.time())
                    profile.collected_at = int(time.time())

            norm_for_search = normalize_text_for_search(
                text_content or raw_content or ""
            )
            new_flag = determine_is_bot_from_content(raw_content or "", norm_for_search)

            changed = False
            if new_flag is not None:
                if profile.is_bot != new_flag:
                    profile.is_bot = new_flag
                    profile.updated_at = int(time.time())
                    changed = True

            if changed:
                session_db.add(profile)
                updated += 1
                if updated % COMMIT_EVERY == 0:
                    session_db.commit()
                    print(f"[{uid}] committed (updates so far: {updated})")

            now_ts = time.time()
            processed_timestamps.append(now_ts)
            cutoff = now_ts - 60.0
            while processed_timestamps and processed_timestamps[0] < cutoff:
                processed_timestamps.popleft()
            total_done += 1
            elapsed = now_ts - start_time if (now_ts - start_time) > 0 else 1.0
            rpm = len(processed_timestamps)
            avg_per_min = (total_done / elapsed) * 60.0

            dt = time.perf_counter() - t0
            print(
                f"uid={uid} sel={sel!s} new_flag={new_flag!s} reg={reg_date} changed={changed} time={dt:.2f}s rate={rpm}/min avg={avg_per_min:.1f}/min"
            )

            time.sleep(0.3)

        session_db.commit()
        print(f"Done. Total updated profiles: {updated}")

    finally:
        try:
            session_db.close()
        except Exception:
            pass
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    process_profiles()
