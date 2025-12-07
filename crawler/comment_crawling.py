import logging
import random
import re
import time
import requests
from typing import Any

from crawler.helpers import (
    extract_attachments,
    extract_text_features,
    normalize_date,
    strip_tags_keep_breaks,
    to_int_safe,
    unwrap_ajax_html,
)
from crawler.logger import log_timer

LOGGER_NAME = "vk_crawler.comments"
_log = logging.getLogger(LOGGER_NAME)

COMMENT_BLOCK_ID_RE = re.compile(r'id="wall_reply-(?P<owner>-?\d+)_(?P<cid>\d+)"', re.I)
COMMENT_BLOCK_ANCHOR_RE = re.compile(
    r'<a[^>]+name="reply(?P<cid>\d+)"[^>]*class="ReplyItem__anchor"[^>]*>',
    re.I,
)
AUTHOR_ANCHOR_RE = re.compile(
    r'<a[^>]+class="[^"]*\bReplyItem__name\b[^"]*"[^>]*href="(?P<href>/[^"]+)"[^>]*>(?P<inner>.*?)</a>',
    re.I | re.S,
)
COMMENT_AUTHOR_LINK_RE = re.compile(
    r'<div class="ReplyItem__header".*?<a[^>]+href="(?P<href>/[^"]+)"[^>]*>(?P<inner>.*?)</a>',
    re.I | re.S,
)
COMMENT_BODY_RE_STRICT = re.compile(
    r'<div class="ReplyItem__body">(.*?)</div>\s*<div class="ReplyItem__date">',
    re.I | re.S,
)
COMMENT_BODY_RE_FALLBACK = re.compile(
    r'<div class="ReplyItem__body">(.*?)</div>', re.I | re.S
)
COMMENT_DATE_RE = re.compile(
    r'<a[^>]+class="item_date"[^>]+href="[^"]*?\?reply=(?P<cid>\d+)(?:&amp;|&)?(?:thread=(?P<thread>\d+))?[^"]*"[^>]*>(?P<date>.*?)</a>',
    re.I | re.S,
)
COMMENT_LIKE_RE = re.compile(
    r"ReplyItem__like[^>]*>(?:\s*<[^>]+>)*\s*(?P<num>\d+)?\s*</a>", re.I | re.S
)
THREAD_NEXT_LINK_RE = re.compile(
    r'<a[^>]+href="(?P<href>/wall-?\d+_\d+\?offset=(?P<offset>\d+)&(?:amp;)?reply=(?P<thread>\d+)[^"]*)"[^>]*class="RepliesThreadNext__link"',
    re.I,
)
REPLIES_REPLYTO_UID_RE = re.compile(
    r"Replies\.replyTo\([^,]*,\s*-?\d+\s*,\s*(?P<cid>\d+)\s*,\s*(?P<uid>-?\d+)\s*\)",
    re.I,
)
IMAGESTATUS_UID_RE = re.compile(
    r'ImageStatus\.open\(\{[^}]*"user_id"\s*:\s*(?P<uid>\d+)', re.I
)


def _iter_comment_segments(html_text: str) -> list[tuple[str, int | None]]:
    markers = list(COMMENT_BLOCK_ID_RE.finditer(html_text))
    if not markers:
        markers = list(COMMENT_BLOCK_ANCHOR_RE.finditer(html_text))

    segments: list[tuple[str, int | None]] = []
    for i, m in enumerate(markers):
        start = m.start()
        end = markers[i + 1].start() if i + 1 < len(markers) else len(html_text)
        cid_hint = int(m.group("cid")) if "cid" in m.groupdict() else None
        segments.append((html_text[start:end], cid_hint))
    return segments


def _index_reply_uids(html_text: str) -> dict[int, int]:
    idx: dict[int, int] = {}
    for m in REPLIES_REPLYTO_UID_RE.finditer(html_text):
        cid = int(m.group("cid"))
        uid = m.group("uid")
        if uid:
            idx[cid] = int(uid)
    return idx


def _extract_uid(segment_html: str, author_href: str | None) -> int | None:
    m = REPLIES_REPLYTO_UID_RE.search(segment_html)
    if m:
        return int(m.group("uid"))

    if author_href:
        m = re.match(r"^/id(\d+)$", author_href)
        if m:
            return int(m.group(1))

    m = IMAGESTATUS_UID_RE.search(segment_html)
    if m:
        return int(m.group("uid"))

    return None


def _parse_one_comment(
    segment_html: str, post_id: int, cid_hint: int | None
) -> dict[str, Any] | None:
    cid: int | None = None

    m_id = COMMENT_BLOCK_ID_RE.search(segment_html)
    if m_id:
        cid = int(m_id.group("cid"))

    if not cid and cid_hint:
        cid = cid_hint

    if not cid:
        m_date = COMMENT_DATE_RE.search(segment_html)
        if m_date:
            cid = int(m_date.group("cid"))

    if not cid:
        return None

    a_m = AUTHOR_ANCHOR_RE.search(segment_html) or COMMENT_AUTHOR_LINK_RE.search(
        segment_html
    )
    author_href = a_m.group("href") if a_m else None
    author_name = None
    if a_m:
        inner = a_m.group("inner")
        author_name = strip_tags_keep_breaks(inner).strip() or None

    from_user_id = _extract_uid(segment_html, author_href)

    b_m = COMMENT_BODY_RE_STRICT.search(
        segment_html
    ) or COMMENT_BODY_RE_FALLBACK.search(segment_html)
    body_html = b_m.group(1) if b_m else ""
    text = strip_tags_keep_breaks(body_html)
    attachments = extract_attachments(body_html)
    text_feats = extract_text_features(text)

    d_m = COMMENT_DATE_RE.search(segment_html)
    date_text = strip_tags_keep_breaks(d_m.group("date")) if d_m else None
    thread_root = int(d_m.group("thread")) if (d_m and d_m.group("thread")) else None
    ts = normalize_date(date_text) if date_text else 0

    like_m = COMMENT_LIKE_RE.search(segment_html)
    likes = to_int_safe(like_m.group("num") if like_m and like_m.group("num") else "0")

    return {
        "post_id": post_id,
        "comment_id": cid,
        "from_id": from_user_id,
        "author_name": author_name,
        "author_href": author_href,
        "text": text,
        "date_text": date_text,
        "timestamp": ts,
        "likes": likes,
        "reply_to_comment_id": thread_root,
        "attachments": attachments,
        "text_features": text_feats,
        "collected_at": int(time.time()),
    }


def parse_initial_comments_from_html(html_text: str, post_id: int) -> dict[str, Any]:
    uid_index = _index_reply_uids(html_text)
    segments = _iter_comment_segments(html_text)

    _log.debug(
        f"parse_html: markers={len(segments)}, segments={len(segments)}, html_bytes={len(html_text)}"
    )

    comments: list[dict[str, Any]] = []
    seen = set()

    for seg_html, cid_hint in segments:
        one = _parse_one_comment(seg_html, post_id=post_id, cid_hint=cid_hint)
        if one:
            cid = one["comment_id"]
            if one.get("from_id") is None:
                uid = uid_index.get(cid)
                if uid is not None:
                    one["from_id"] = uid

            if cid not in seen:
                comments.append(one)
                seen.add(cid)

    _log.info(f"parsed comments batch: count={len(comments)} unique={len(seen)}")
    return {"comments": comments}


def http_get_initial(
    post_id: int,
    owner_id: int,
    session: requests.Session,
    base_url: str = "https://m.vk.com",
    timeout: int = 15,
) -> str:
    url = f"{base_url.rstrip('/')}/wall{owner_id}_{post_id}"
    with log_timer("GET initial", logger=_log):
        resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _extract_threads_from_html(html_text: str) -> list[tuple[int, int]]:
    found: list[tuple[int, int]] = []
    for m in THREAD_NEXT_LINK_RE.finditer(html_text):
        thread = int(m.group("thread"))
        start_offset = int(m.group("offset") or 1)
        found.append((thread, start_offset))
    return found


def http_get_more(
    post_id: int,
    owner_id: int,
    session: requests.Session,
    base_url: str = "https://m.vk.com",
    offset: int = 10,
    timeout: int = 15,
) -> str:
    url = f"{base_url.rstrip('/')}/wall{owner_id}_{post_id}"
    params = {"offset": offset, "own": 1}
    data = {"_ajax": 1, "_pstatref": "group"}
    with log_timer("POST more", logger=_log):
        resp = session.post(url, params=params, data=data, timeout=timeout)
    resp.raise_for_status()
    return unwrap_ajax_html(resp.text)


def http_get_thread_page(
    post_id: int,
    owner_id: int,
    thread_id: int,
    offset: int,
    session: requests.Session,
    base_url: str = "https://m.vk.com",
    timeout: int = 15,
) -> str:
    url = f"{base_url.rstrip('/')}/wall{owner_id}_{post_id}"
    params = {"offset": offset, "reply": thread_id, "own": 1}
    data = {"_ajax": 1, "_pstatref": "group"}
    with log_timer("POST thread", logger=_log):
        resp = session.post(url, params=params, data=data, timeout=timeout)
    resp.raise_for_status()
    return unwrap_ajax_html(resp.text)


def _crawl_thread_into(
    *,
    post_id: int,
    owner_id: int,
    thread_id: int,
    offset: int,
    by_id: dict[int, dict],
    base_url: str,
    timeout: int,
    request_sleep: float,
    jitter: float,
    session: requests.Session,
) -> None:
    _log.info(f"start thread crawl: thread_id={thread_id}, start_offset={offset}")
    while True:
        html = http_get_thread_page(
            post_id=post_id,
            owner_id=owner_id,
            thread_id=thread_id,
            session=session,
            offset=offset,
            base_url=base_url,
            timeout=timeout,
        )
        if not html or len(html) < 30:
            _log.info("thread page empty/short -> stop")
            break

        parsed = parse_initial_comments_from_html(html, post_id)
        new_count = 0
        for c in parsed["comments"]:
            cid = c["comment_id"]
            if cid not in by_id:
                c["owner_id"] = owner_id
                by_id[cid] = c
                new_count += 1

        _log.info(f"thread page parsed: new={new_count}, total={len(by_id)}")

        if new_count == 0:
            break

        time.sleep(request_sleep + random.random() * jitter)


def crawl_comments_for_post(
    post_id: int,
    owner_id: int,
    session: requests.Session,
    max_comments: int | None = 25,
    base_url: str = "https://m.vk.com",
    offset: int = 10,
    timeout: int = 15,
    request_sleep: float = 0.3,
    jitter: float = 0.1,
) -> list[dict]:
    _log.info(f"start crawling comments for post {post_id} owner {owner_id}")

    all_comments: dict[int, dict] = {}

    html0 = http_get_initial(
        post_id,
        owner_id=owner_id,
        session=session,
        base_url=base_url,
        timeout=timeout,
    )
    parsed0 = parse_initial_comments_from_html(html0, post_id)
    for c in parsed0["comments"]:
        c["owner_id"] = owner_id
        all_comments[c["comment_id"]] = c

    _log.info(f"after initial: total={len(all_comments)}")
    for thread_id, start_page in _extract_threads_from_html(html0):
        _crawl_thread_into(
            post_id=post_id,
            owner_id=owner_id,
            thread_id=thread_id,
            session=session,
            offset=start_page,
            by_id=all_comments,
            base_url=base_url,
            timeout=timeout,
            request_sleep=request_sleep,
            jitter=jitter,
        )

    time.sleep(request_sleep + random.random() * jitter)

    while True:
        _log.info(f"paginate: try offset={offset}, total={len(all_comments)}")

        if max_comments and len(all_comments) >= max_comments:
            _log.info(f"reached max_comments={len(all_comments)} -> stop")
            break

        html_more = http_get_more(
            post_id=post_id,
            owner_id=owner_id,
            session=session,
            offset=offset,
            base_url=base_url,
            timeout=timeout,
        )
        if not html_more or len(html_more) < 50:
            _log.info("empty/too short payload -> stop")
            break

        batch_more = parse_initial_comments_from_html(html_more, post_id)
        page_size = len(batch_more["comments"]) or 10

        new_cnt = 0
        for c in batch_more["comments"]:
            cid = c["comment_id"]
            if cid not in all_comments:
                c["owner_id"] = owner_id
                all_comments[cid] = c
                new_cnt += 1

        _log.info(f"pagination page parsed: new={new_cnt}, total={len(all_comments)}")

        for thread_id, start_page in _extract_threads_from_html(html_more):
            _crawl_thread_into(
                post_id=post_id,
                owner_id=owner_id,
                thread_id=thread_id,
                session=session,
                offset=start_page,
                by_id=all_comments,
                base_url=base_url,
                timeout=timeout,
                request_sleep=request_sleep,
                jitter=jitter,
            )

        if new_cnt == 0:
            _log.info("no new comments and no threads -> stop")
            break

        offset += page_size
        time.sleep(request_sleep + random.random() * jitter)

    comments = list(all_comments.values())
    comments.sort(
        key=lambda x: (x.get("reply_to_comment_id") is not None, x.get("timestamp", 0))
    )
    if all_comments:
        comments = comments[:max_comments]

    _log.info(f"crawl finished: returned={len(comments)}")
    return comments
