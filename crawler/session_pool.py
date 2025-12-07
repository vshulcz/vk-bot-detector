import random
import time
import logging
from typing import Optional
from collections import deque
from threading import Lock
import requests

from crawler.config import HEADERS


class SessionPool:
    def __init__(self, pool_size: int = 10):
        self.pool_size = pool_size
        self.sessions: deque = deque()
        self.lock = Lock()
        self.logger = logging.getLogger("vk_crawler.session_pool")
        self._init_pool()
    
    def _init_pool(self):
        self.logger.info(f"Initializing session pool with {self.pool_size} sessions")
        for i in range(self.pool_size):
            session = self._create_unique_session(i)
            self.sessions.append(session)
    
    def _create_unique_session(self, idx: int) -> requests.Session:
        session = requests.Session()
        
        resolutions = [
            (1920, 1080), (1680, 1050), (1536, 864), (1440, 900),
            (1366, 768), (2560, 1440), (1600, 900), (1280, 720)
        ]
        w, h = random.choice(resolutions)
        dpr = random.choice([1.0, 1.25, 1.5, 1.75, 2.0])
        
        session_cookies = {
            "remixua": f"{random.randint(50, 60)}%7C{random.randint(600, 700)}%7C{random.randint(300, 400)}%7C{random.randint(170000000, 180000000)}",
            "remixscreen_width": str(w),
            "remixscreen_height": str(h),
            "remixscreen_dpr": str(dpr),
            "remixscreen_depth": random.choice(["24", "30", "32"]),
            "remixscreen_winzoom": "1",
            "remixdark_color_scheme": random.choice(["0", "1"]),
            "remixcolor_scheme_mode": random.choice(["auto", "dark", "light"]),
            "remixrt": "0",
            "remixsf": "1",
            "remixdt": "0",
            "remixlang": "3",
            "remixsuc": f"{random.randint(1, 9)}%3A",
            "remixmdevice": f"{w}/{h}/1/!!-!!!!!!!!/{max(w, h)}",
            "adblock": random.choice(["0", "1"]),
            "remixscreen_orient": "1",
            "remixvkcom": "1",
            "remixcurr_audio": "null",
            "remixmaudio": "null",
            "remixff": "".join([random.choice(["0", "1"]) for _ in range(14)]),
            "remixmvk-fp": "".join([random.choice("0123456789abcdef") for _ in range(32)]),
        }
        
        for k, v in session_cookies.items():
            session.cookies.set(k, v)
        
        browsers = [
            "Chrome/127.0.0.0",
            "Chrome/126.0.0.0",
            "Chrome/125.0.0.0",
            "Firefox/128.0",
            "Firefox/127.0",
            "Safari/17.5",
        ]
        os_versions = [
            "Windows NT 10.0; Win64; x64",
            "Windows NT 11.0; Win64; x64",
            "Macintosh; Intel Mac OS X 10_15_7",
            "X11; Linux x86_64",
        ]
        
        browser = random.choice(browsers)
        os_ver = random.choice(os_versions)
        
        session_headers = HEADERS.copy()
        if "Chrome" in browser:
            session_headers["User-Agent"] = (
                f"Mozilla/5.0 ({os_ver}) "
                f"AppleWebKit/537.36 (KHTML, like Gecko) "
                f"{browser} Safari/537.36"
            )
        elif "Firefox" in browser:
            session_headers["User-Agent"] = (
                f"Mozilla/5.0 ({os_ver}; rv:128.0) "
                f"Gecko/20100101 Firefox/{browser.split('/')[1]}"
            )
        else:
            session_headers["User-Agent"] = (
                f"Mozilla/5.0 ({os_ver}) "
                f"AppleWebKit/605.1.15 (KHTML, like Gecko) "
                f"Version/17.5 Safari/605.1.15"
            )
        
        session.headers.update(session_headers)
        
        self.logger.debug(f"Session {idx}: UA={browser}, Resolution={w}x{h}")
        return session
    
    def get_session(self) -> requests.Session:
        with self.lock:
            session = self.sessions[0]
            self.sessions.rotate(-1)
            return session
    
    def return_session(self, session: requests.Session):
        pass
    
    def close_all(self):
        with self.lock:
            for session in self.sessions:
                try:
                    session.close()
                except Exception:
                    pass


class AdaptiveRateLimiter:
    def __init__(self, initial_delay: float = 0.1, max_delay: float = 5.0):
        self.current_delay = initial_delay
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.min_delay = 0.01
        self.lock = Lock()
        self.error_count = 0
        self.success_count = 0
        self.logger = logging.getLogger("vk_crawler.rate_limiter")
        self.last_request_time = 0
    
    def wait(self):
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.current_delay:
                sleep_time = self.current_delay - elapsed
                time.sleep(sleep_time)
            self.last_request_time = time.time()
    
    def report_success(self):
        with self.lock:
            self.success_count += 1
            self.error_count = 0
            
            if self.success_count >= 10:
                old_delay = self.current_delay
                self.current_delay = max(
                    self.min_delay,
                    self.current_delay * 0.9
                )
                if old_delay != self.current_delay:
                    self.logger.info(
                        f"Rate limit decreased: {old_delay:.3f}s -> {self.current_delay:.3f}s "
                        f"(success_streak={self.success_count})"
                    )
                self.success_count = 0
    
    def report_error(self, status_code: Optional[int] = None):
        with self.lock:
            self.error_count += 1
            self.success_count = 0
            
            old_delay = self.current_delay
            
            if status_code == 429:
                self.current_delay = min(
                    self.max_delay,
                    self.current_delay * 3.0
                )
                self.logger.warning(
                    f"Rate limit hit (429)! Increased delay: "
                    f"{old_delay:.3f}s -> {self.current_delay:.3f}s"
                )
            elif status_code in (403, 503):
                self.current_delay = min(
                    self.max_delay,
                    self.current_delay * 2.0
                )
                self.logger.warning(
                    f"Access error ({status_code})! Increased delay: "
                    f"{old_delay:.3f}s -> {self.current_delay:.3f}s"
                )
            else:
                self.current_delay = min(
                    self.max_delay,
                    self.current_delay * 1.5
                )
                self.logger.warning(
                    f"Request error! Increased delay: "
                    f"{old_delay:.3f}s -> {self.current_delay:.3f}s"
                )
            
            if self.error_count >= 3:
                wait_time = self.current_delay * 2
                self.logger.warning(
                    f"Multiple errors detected! Waiting {wait_time:.1f}s..."
                )
                time.sleep(wait_time)
    
    def get_current_delay(self) -> float:
        with self.lock:
            return self.current_delay
    
    def reset(self):
        with self.lock:
            self.current_delay = self.initial_delay
            self.error_count = 0
            self.success_count = 0
            self.logger.info("Rate limiter reset")


_session_pool: Optional[SessionPool] = None
_rate_limiter: Optional[AdaptiveRateLimiter] = None


def get_session_pool(pool_size: int = 10) -> SessionPool:
    global _session_pool
    if _session_pool is None:
        _session_pool = SessionPool(pool_size=pool_size)
    return _session_pool


def get_rate_limiter(initial_delay: float = 0.1) -> AdaptiveRateLimiter:
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = AdaptiveRateLimiter(initial_delay=initial_delay)
    return _rate_limiter


def close_session_pool():
    global _session_pool
    if _session_pool:
        _session_pool.close_all()
        _session_pool = None
