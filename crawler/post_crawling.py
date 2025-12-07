import logging
import random
import re
import html
import time
import pytz
import requests
from datetime import datetime

from crawler.helpers import (
    extract_attachments,
    extract_text_features,
    normalize_date,
    strip_tags_keep_breaks,
    to_int_safe,
    unwrap_ajax_html,
)
from crawler.logger import log_timer

LOGGER_NAME = "vk_crawler.posts"
_log = logging.getLogger(LOGGER_NAME)

POST_HEADER_START_RE = re.compile(
    r'<div[^>]+class="[^"]*\bPostHeader__contentWrapper\b[^"]*"[^>]*>', re.I
)
POST_ID_RE = re.compile(r'data-post-id="(?P<owner>-?\d+)_(?P<pid>\d+)"')
DATE_LINK_RE = re.compile(
    r'<a[^>]+href="(?P<href>/wall-[-\d_]+)"[^>]*class="[^"]*PostHeaderTime[^"]*"[^>]*>(?P<date>.*?)</a>',
    re.I | re.S,
)
WI_BODY_OPEN_RE = re.compile(r'<div[^>]+class="[^"]*\bwi_body\b[^"]*"[^>]*>', re.I)
PI_TEXT_BLOCK_RE = re.compile(
    r'<div[^>]+class="[^"]*\bpi_text\b[^"]*"[^>]*>(?P<html>.*?)</div>', re.I | re.S
)
POST_MORE_LINK_RE = re.compile(
    r'<a[^>]+class="[^"]*\bPostTextMore\b[^"]*"[^>]*>.*?</a>', re.I | re.S
)
HIDDEN_SPAN_RE = re.compile(
    r'<span[^>]+style="[^"]*display\s*:\s*none[^"]*"[^>]*>(?P<hidden>.*?)</span>',
    re.I | re.S,
)

REACTION_LIKES_RE = re.compile(
    r'PostBottomButtonReaction__label"[^>]*>(?P<num>[^<]+)<', re.I
)
COMMENTS_ATTR_RE = re.compile(r'data-replies-count="(?P<num>[\d\s,.KkMm]+)"', re.I)
SHARE_COUNT_RE = re.compile(
    r'<a[^>]+href="/like\?act=publish[^"]*"[^>]*>.*?PostBottomButton__label"[^>]*>(?P<num>[\d\s,.KkMm]+)<',
    re.I | re.S,
)
VIEWS_COUNT_NEW_RE = re.compile(r'Socials__viewsCount"[^>]*>(?P<num>[^<]+)<', re.I)

DATA_EXEC_JSON_RE = re.compile(
    r'PostContextMenuReactMVK__root"[^>]+data-exec="(?P<json>\{.+?\})"', re.I
)
IS_COMMENTS_CLOSED_RE = re.compile(r'"isCommentsClosed"\s*:\s*(true|false)', re.I)
PINNED_VISUALLY_HIDDEN_RE = re.compile(r"visually-hidden[^>]*>[^<]*post pinned", re.I)


def extract_full_post_text(block_html: str) -> str:
    m = PI_TEXT_BLOCK_RE.search(block_html)
    if not m:
        return ""
    inner = m.group("html")
    hidden_parts = [hm.group("hidden") for hm in HIDDEN_SPAN_RE.finditer(inner)]
    inner = POST_MORE_LINK_RE.sub("", inner)
    if hidden_parts:
        inner = inner + "\n\n" + "\n\n".join(hidden_parts)
    return strip_tags_keep_breaks(inner)


def extract_flags(segment_html: str) -> dict:
    pinned = bool(PINNED_VISUALLY_HIDDEN_RE.search(segment_html))
    is_comments_closed = None
    j = DATA_EXEC_JSON_RE.search(segment_html)
    if j:
        raw = html.unescape(j.group("json")).replace("&quot;", '"')
        m = IS_COMMENTS_CLOSED_RE.search(raw)
        if m:
            is_comments_closed = m.group(1).lower() == "true"
    return {"pinned": pinned, "is_comments_closed": is_comments_closed}


def extract_counters_from_segment(segment: str) -> dict[str, int]:
    likes = 0
    m = REACTION_LIKES_RE.search(segment)
    if m:
        likes = to_int_safe(m.group("num"))

    comments = 0
    m = COMMENTS_ATTR_RE.search(segment)
    if m:
        comments = to_int_safe(m.group("num"))

    reposts = 0
    m = SHARE_COUNT_RE.search(segment)
    if m:
        reposts = to_int_safe(m.group("num"))

    views = 0
    m = VIEWS_COUNT_NEW_RE.search(segment)
    if m:
        views = to_int_safe(m.group("num"))

    return {"likes": likes, "reposts": reposts, "comments": comments, "views": views}


def parse_initial_posts_from_html(html_text: str) -> list[dict]:
    headers = list(POST_HEADER_START_RE.finditer(html_text))
    _log.debug(f"parse_posts_html: headers={len(headers)}, html_bytes={len(html_text)}")

    results: list[dict] = []
    seen = set()

    for i, h in enumerate(headers):
        seg_start = h.start()
        seg_end = headers[i + 1].start() if i + 1 < len(headers) else len(html_text)
        segment = html_text[seg_start:seg_end]

        pid_m = POST_ID_RE.search(segment)
        if not pid_m:
            continue

        owner_id = int(pid_m.group("owner"))
        post_id = int(pid_m.group("pid"))
        key = (owner_id, post_id)
        if key in seen:
            continue

        seen.add(key)

        date_m = DATE_LINK_RE.search(segment)
        post_href = date_m.group("href") if date_m else None
        date_html = date_m.group("date") if date_m else ""
        date_text = strip_tags_keep_breaks(date_html) if date_html else None

        body_m = WI_BODY_OPEN_RE.search(segment)
        body_html = segment[body_m.start() :] if body_m else ""

        text_content = extract_full_post_text(body_html)
        flags = extract_flags(segment)
        counters = extract_counters_from_segment(segment)
        att = extract_attachments(body_html)
        text_feats = extract_text_features(text_content)
        timestamp = normalize_date(
            date_text, now=datetime.now(pytz.timezone("Europe/Moscow"))
        )

        results.append(
            {
                "owner_id": owner_id,
                "post_id": post_id,
                "url": ("https://m.vk.com" + post_href)
                if post_href and post_href.startswith("/")
                else post_href,
                "date_text": date_text,
                "timestamp": timestamp,
                "text": text_content,
                "counters": counters,
                "flags": flags,
                "attachments": att,
                "text_features": text_feats,
                "collected_at": int(time.time()),
            }
        )

    _log.info(f"parsed posts batch: count={len(results)} unique={len(seen)}")
    return results


def http_get_initial(
    group_slug: str,
    session: requests.Session,
    base_url: str = "https://m.vk.com",
    timeout: int = 15,
) -> str:
    url = f"{base_url.rstrip('/')}/{group_slug.lstrip('/')}"
    with log_timer("GET posts initial", logger=_log):
        resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def http_get_more(
    group_slug: str,
    session: requests.Session,
    base_url: str = "https://m.vk.com",
    offset: int = 10,
    timeout: int = 15,
) -> str:
    url = f"{base_url.rstrip('/')}/{group_slug.lstrip('/')}"
    params = {"offset": offset, "own": 1}
    data = {"_ajax": 1, "_pstatref": "group"}
    with log_timer("POST posts more", logger=_log):
        resp = session.post(url, params=params, data=data, timeout=timeout)
    resp.raise_for_status()
    return unwrap_ajax_html(resp.text)


def crawl_posts(
    group_slug: str,
    session: requests.Session,
    max_posts: int | None = 25,
    base_url: str = "https://m.vk.com",
    offset: int = 10,
    timeout: int = 15,
    request_sleep: float = 0.3,
    jitter: float = 0.1,
) -> list[dict]:
    _log.info(f"start crawling posts for group {group_slug}")

    all_posts: dict[tuple[int, int], dict] = {}

    html0 = http_get_initial(
        group_slug, base_url=base_url, timeout=timeout, session=session
    )
    batch = parse_initial_posts_from_html(html0)
    for p in batch:
        all_posts[(p["owner_id"], p["post_id"])] = p
    page_size = len(batch) or 10
    offset = page_size
    _log.info(f"after initial: total={len(all_posts)}")

    time.sleep(request_sleep + random.random() * jitter)

    while True:
        _log.info(f"paginate posts: try offset={offset}, total={len(all_posts)}")

        if max_posts and len(all_posts) >= max_posts:
            _log.info(f"reached max_posts={len(all_posts)} -> stop")
            break

        html_more = http_get_more(
            group_slug,
            session=session,
            base_url=base_url,
            offset=offset,
            timeout=timeout,
        )
        if not html_more or len(html_more) < 50:
            _log.info("empty/too short payload -> stop")
            break

        batch_more = parse_initial_posts_from_html(html_more)
        page_size = len(batch_more) or page_size

        new_cnt = 0
        for p in batch_more:
            key = (p["owner_id"], p["post_id"])
            if key not in all_posts:
                all_posts[key] = p
                new_cnt += 1

        _log.info(f"pagination page parsed: new={new_cnt}, total={len(all_posts)}")

        if new_cnt == 0:
            _log.info("no new posts -> stop")
            break

        offset += page_size
        time.sleep(request_sleep + random.random() * jitter)

    posts = list(all_posts.values())
    posts.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    if max_posts:
        posts = posts[:max_posts]
    _log.info(f"crawl posts finished: returned={len(posts)}")
    return posts
