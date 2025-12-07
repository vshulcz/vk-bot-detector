import json
import re
import pytz
import html
import requests

from urllib3 import Retry
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta

from crawler.config import HEADERS, COOKIES


TAG_RE = re.compile(r"<[^>]+>")
BR_RE = re.compile(r"<\s*br\s*/?\s*>", re.I)

HASHTAG_RE = re.compile(r"(?<!\w)#(\w{2,})", re.U)
MENTION_RE = re.compile(r"(?<!\w)@([A-Za-z0-9_.]{2,})")
PLAIN_URL_RE = re.compile(r"https?://[^\s)]+")

IMG_SRC_RE = re.compile(r'<img[^>]+src="(?P<src>https?://[^"]+)"', re.I)
VIDEO_HREF_RE = re.compile(r'<a[^>]+href="(?P<href>/video[-\d_]+)"', re.I)
OUTLINK_RE = re.compile(
    r'<a[^>]+href="(?P<href>https?://[^"]+)"[^>]*rel="[^"]*\bnofollow\b[^"]*"', re.I
)


def make_session(use_pool: bool = False):
    if use_pool:
        from crawler.session_pool import get_session_pool
        return get_session_pool().get_session()
    
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=20))
    s.headers.update(HEADERS)
    s.cookies.update(COOKIES)
    return s


def strip_tags_keep_breaks(s: str) -> str:
    s = BR_RE.sub("\n", s)
    s = TAG_RE.sub("", s)
    s = html.unescape(s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


MONTHS_EN = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def to_int_safe(s: str) -> int:
    if not s:
        return 0
    s = s.strip().replace(" ", "").replace(",", "")
    m = re.match(r"^(\d+(?:\.\d+)?)([kKmM]?)$", s)
    if not m:
        return int(re.sub(r"\D+", "", s) or 0)
    num, suffix = m.groups()
    val = float(num)
    if suffix.lower() == "k":
        val *= 1_000
    if suffix.lower() == "m":
        val *= 1_000_000
    return int(val)


def normalize_date(
    date_text: str | None, now: datetime | None = None, tz: str = "Europe/Moscow"
) -> int:
    if not date_text:
        return 0
    if now is None:
        now = datetime.now(pytz.timezone(tz))

    dtz = pytz.timezone(tz)
    now = now.astimezone(dtz)
    s = date_text.strip().lower()
    m = re.match(
        r"^(\d{1,2})\s+([a-z]{3,5})[a-z]*\s+(\d{4})(?:\s+at\s+(\d{1,2}):(\d{2})\s*(am|pm)?)?$",
        s,
    )
    if m:
        d, mon, y, hh, mm, ap = m.groups()
        h = int(hh) if hh else 0
        mmin = int(mm) if mm else 0
        if ap:
            if ap == "pm" and h != 12:
                h += 12
            if ap == "am" and h == 12:
                h = 0
        mon_i = MONTHS_EN.get(mon[:5], MONTHS_EN.get(mon[:3], 1))
        dt = dtz.localize(datetime(int(y), mon_i, int(d), h, mmin))
        return int(dt.timestamp())

    m = re.match(
        r"^(\d{1,2})\s+([a-z]{3,5})[a-z]*\s+at\s+(\d{1,2}):(\d{2})\s*(am|pm)?$", s
    )
    if m:
        d, mon, hh, mm, ap = m.groups()
        h = int(hh)
        if ap:
            if ap == "pm" and h != 12:
                h += 12
            if ap == "am" and h == 12:
                h = 0
        mon_i = MONTHS_EN.get(mon[:5], MONTHS_EN.get(mon[:3], 1))
        dt = dtz.localize(datetime(now.year, mon_i, int(d), h, int(mm)))
        if dt - now > timedelta(days=1):
            dt = dtz.localize(datetime(now.year - 1, mon_i, int(d), h, int(mm)))
        return int(dt.timestamp())

    m = re.match(r"^(\d{1,2})\s+([a-z]{3,5})[a-z]*$", s)
    if m:
        d, mon = m.groups()
        mon_i = MONTHS_EN.get(mon[:5], MONTHS_EN.get(mon[:3], 1))
        dt = dtz.localize(datetime(now.year, mon_i, int(d), 0, 0))
        if dt - now > timedelta(days=1):
            dt = dtz.localize(datetime(now.year - 1, mon_i, int(d), 0, 0))
        return int(dt.timestamp())

    m = re.match(r"(yesterday|today)\s+at\s+(\d{1,2}):(\d{2})\s*(am|pm)?", s)
    if m:
        day, hh, mm, ap = m.groups()
        h = int(hh)
        if ap:
            if ap == "pm" and h != 12:
                h += 12
            if ap == "am" and h == 12:
                h = 0
        base = now.replace(hour=h, minute=int(mm), second=0, microsecond=0)
        if day == "yesterday":
            base -= timedelta(days=1)
        return int(base.timestamp())

    words = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }
    m = re.match(
        r"(?:(\d+)|("
        + "|".join(words.keys())
        + r"))\s+(hour|hours|minute|minutes)\s+ago",
        s,
    )
    if m:
        num_d, word, unit = m.groups()
        n = int(num_d) if num_d else words.get(word, 0)
        delta = timedelta(hours=n) if unit.startswith("hour") else timedelta(minutes=n)
        dt = now - delta
        return int(dt.timestamp())

    return 0


def extract_text_features(text: str) -> dict:
    hashtags = HASHTAG_RE.findall(text or "")
    mentions = MENTION_RE.findall(text or "")
    urls = PLAIN_URL_RE.findall(text or "")
    return {"hashtags": hashtags, "mentions": mentions, "urls": urls}


def extract_attachments(body_html: str) -> dict:
    images = [m.group("src") for m in IMG_SRC_RE.finditer(body_html)]
    videos = [m.group("href") for m in VIDEO_HREF_RE.finditer(body_html)]
    outlinks = [m.group("href") for m in OUTLINK_RE.finditer(body_html)]
    return {"images": images, "videos": videos, "outlinks": outlinks}


def unwrap_ajax_html(payload: str) -> str:
    t = (payload or "").strip()
    if not t or not t.startswith("{"):
        return t
    try:
        obj = json.loads(t)
        data = obj.get("data", [])
        return "".join(s for s in data if isinstance(s, str))
    except Exception:
        return t
