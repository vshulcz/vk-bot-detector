import random


DEFAULT_TZ = "Europe/Moscow"
DEFAULT_BASE_URL = "https://m.vk.com"
REQUEST_TIMEOUT = 15
REQUEST_SLEEP = 0.1
OFFSET_STEP = 10
JITTER = 0.05

FAST_MODE_TIMEOUT = 10
FAST_MODE_SLEEP = 0.02
FAST_MODE_JITTER = 0.0
FAST_MODE_WORKERS = 12

w, h = random.choice([(1920, 1080), (1680, 1050), (1536, 864)])
dpr = random.choice([1.25, 1.5, 1.75, 2.0])
orient = "1"

COOKIES = {
    "remixua": "52%7C628%7C333%7C178607458",
    "remixscreen_width": str(w),
    "remixscreen_height": str(h),
    "remixscreen_dpr": str(dpr),
    "remixscreen_depth": random.choice(["24", "30"]),
    "remixscreen_winzoom": "1",
    "remixdark_color_scheme": random.choice(["0", "1"]),
    "remixcolor_scheme_mode": random.choice(["auto", "dark", "light"]),
    "remixrt": "0",
    "remixsf": "1",
    "remixdt": "0",
    "remixlang": "3",
    "remixsuc": "1%3A",
    "remixmdevice": f"{w}/{h}/1/!!-!!!!!!!!/{max(w, h)}",
    "adblock": random.choice(["0", "1"]),
    "remixscreen_orient": "1",
    "remixvkcom": "1",
    "remixcurr_audio": "null",
    "remixmaudio": "null",
    "remixff": "10111111111101",
    "remixmvk-fp": "79338606fc2288bf81b69584b426565d",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Prefer": "safe",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "DNT": "1",
    "Priority": "u=0, i",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Origin": DEFAULT_BASE_URL,
    "Referer": DEFAULT_BASE_URL,
}
