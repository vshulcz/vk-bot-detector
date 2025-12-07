import json
import logging
import re
from typing import Any
import requests

from crawler.logger import log_timer

LOGGER_NAME = "vk_crawler.profile"
_log = logging.getLogger(LOGGER_NAME)

API_PREFETCH_KEY_RE = re.compile(r'"apiPrefetchCache"\s*:\s*\[', re.I)


def _extract_json_array_after(text: str, start_pos: int) -> str | None:
    i = text.find("[", start_pos)
    if i == -1:
        return None

    depth = 0
    j = i
    in_str = False
    esc = False
    while j < len(text):
        ch = text[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    return text[i : j + 1]
        j += 1
    return None


def _to_int(x):
    try:
        return int(x)
    except Exception:
        return None


def parse_api_prefetch_cache(html: str) -> list[dict[str, Any]]:
    m = API_PREFETCH_KEY_RE.search(html)
    if not m:
        _log.debug("apiPrefetchCache not found in html")
        return []
    arr_txt = _extract_json_array_after(html, m.end() - 1)
    if not arr_txt:
        _log.debug("apiPrefetchCache bracket parse failed")
        return []
    try:
        data = json.loads(arr_txt)
        if isinstance(data, list):
            return data
    except Exception as e:
        _log.exception("json decode of apiPrefetchCache failed: %s", e)
    return []


def build_profile_bundle(prefetch: list[dict[str, Any]]) -> dict[str, Any]:
    by_method: dict[str, dict[str, Any]] = {}
    for it in prefetch:
        method = it.get("method")
        if method:
            by_method[method] = it

    users_get = by_method.get("users.get", {}).get("response") or []
    u = (users_get[0] if users_get else {}) or {}

    bundle: dict[str, Any] = {
        "profile": {},
        "counters": {},
        "personal": {},
        "languages": [],
        "universities": [],
        "schools": [],
        "careers": [],
        "friends_sample": [],
        "followers_sample": [],
        "subscriptions_sample": [],
        "photos": [],
        "videos_sample": [],
    }

    cover_imgs = (u.get("cover") or {}).get("images") or []
    prof = bundle["profile"]
    prof.update(
        {
            "user_id": u.get("id"),
            "screen_name": u.get("screen_name"),
            "domain": u.get("domain"),
            "first_name": u.get("first_name"),
            "last_name": u.get("last_name"),
            "nickname": u.get("nickname"),
            "sex": u.get("sex"),
            "bdate": u.get("bdate"),
            "bdate_visibility": u.get("bdate_visibility"),
            "city_id": (u.get("city") or {}).get("id"),
            "city_title": (u.get("city") or {}).get("title"),
            "country_id": (u.get("country") or {}).get("id"),
            "country_title": (u.get("country") or {}).get("title"),
            "home_town": u.get("home_town"),
            "verified": bool(u.get("verified")),
            "is_sber_verified": bool(u.get("is_sber_verified")),
            "is_tinkoff_verified": bool(u.get("is_tinkoff_verified")),
            "is_esia_verified": bool(u.get("is_esia_verified")),
            "is_nft": bool(u.get("is_nft")),
            "is_followers_mode_on": bool(u.get("is_followers_mode_on")),
            "no_index": u.get("no_index"),
            "wall_default": u.get("wall_default"),
            "status": u.get("status") or "",
            "activity": u.get("activity") or "",
            "about": u.get("about") or "",
            "interests": u.get("interests") or "",
            "books": u.get("books") or "",
            "tv": u.get("tv") or "",
            "quotes": u.get("quotes") or "",
            "games": u.get("games") or "",
            "movies": u.get("movies") or "",
            "music": u.get("music") or "",
            "site": u.get("site") or "",
            "mobile_phone": (u.get("contacts") or {}).get("mobile_phone")
            or u.get("mobile_phone")
            or "",
            "home_phone": (u.get("contacts") or {}).get("home_phone")
            or u.get("home_phone")
            or "",
            "photo_50": u.get("photo_rec"),
            "photo_100": u.get("photo_medium_rec"),
            "photo_200": u.get("photo_200"),
            "photo_400": u.get("photo_400"),
            "photo_max": u.get("photo_max"),
            "photo_base": u.get("photo_base"),
            "photo_avg_color": u.get("photo_avg_color"),
            "photo_id": u.get("photo_id"),
            "cover_photo_url": cover_imgs[0]["url"] if cover_imgs else None,
            "online": bool(u.get("online")),
            "last_seen_ts": (u.get("last_seen") or {}).get("time")
            or (u.get("online_info") or {}).get("last_seen"),
            "last_seen_platform": (u.get("last_seen") or {}).get("platform"),
            "online_app_id": (u.get("online_info") or {}).get("app_id"),
            "followers_count": u.get("followers_count"),
        }
    )

    ug_cnt = u.get("counters") or {}

    counter_keys = [
        "albums",
        "audios",
        "followers",
        "friends",
        "groups",
        "online_friends",
        "pages",
        "photos",
        "subscriptions",
        "videos",
        "video_playlists",
        "mutual_friends",
        "clips_followers",
        "clips_views",
        "clips_likes",
    ]
    for k in counter_keys:
        v = ug_cnt.get(k)
        if v is not None:
            iv = _to_int(v)
            if iv is not None:
                bundle["counters"][k] = iv

    fr_resp = by_method.get("friends.get", {}).get("response") or {}
    if isinstance(fr_resp, dict) and fr_resp.get("count") is not None:
        bundle["counters"]["friends"] = _to_int(fr_resp.get("count"))

    fol_resp = by_method.get("users.getFollowers", {}).get("response") or {}
    if isinstance(fol_resp, dict):
        if fol_resp.get("count") is not None:
            followers_total = _to_int(fol_resp.get("count"))
            bundle["counters"]["followers"] = followers_total
            prof["followers_count"] = followers_total

    subs_resp = by_method.get("users.getSubscriptions", {}).get("response") or {}
    if isinstance(subs_resp, dict) and subs_resp.get("count") is not None:
        bundle["counters"]["subscriptions"] = _to_int(subs_resp.get("count"))

    photos_resp = by_method.get("photos.get", {}).get("response") or {}
    if isinstance(photos_resp, dict) and photos_resp.get("count") is not None:
        bundle["counters"]["photos"] = _to_int(photos_resp.get("count"))

    video_resp = by_method.get("video.get", {}).get("response") or {}
    if isinstance(video_resp, dict) and video_resp.get("count") is not None:
        bundle["counters"]["videos"] = _to_int(video_resp.get("count"))

    personal = u.get("personal") or {}
    bundle["personal"] = {
        "alcohol": personal.get("alcohol"),
        "inspired_by": personal.get("inspired_by"),
        "life_main": personal.get("life_main"),
        "people_main": personal.get("people_main"),
        "religion": personal.get("religion"),
        "religion_id": personal.get("religion_id"),
        "smoking": personal.get("smoking"),
    }
    bundle["languages"] = [*(personal.get("langs") or [])]
    bundle["universities"] = u.get("universities") or []
    bundle["schools"] = u.get("schools") or []
    bundle["careers"] = u.get("career") or []

    for it in fr_resp.get("items") or []:
        bundle["friends_sample"].append(
            {
                "friend_user_id": it.get("id"),
                "domain": it.get("domain"),
                "first_name": it.get("first_name"),
                "last_name": it.get("last_name"),
                "sex": it.get("sex"),
                "online": bool(it.get("online")),
                "photo_50": it.get("photo_50"),
                "photo_100": it.get("photo_100"),
                "photo_200": it.get("photo_200"),
            }
        )

    for it in fol_resp.get("items") or []:
        bundle["followers_sample"].append(
            {
                "follower_user_id": it.get("id"),
                "first_name": it.get("first_name"),
                "last_name": it.get("last_name"),
                "sex": it.get("sex"),
                "online": bool(it.get("online")),
                "photo_50": it.get("photo_50"),
                "photo_100": it.get("photo_100"),
                "photo_200": it.get("photo_200"),
            }
        )

    for it in subs_resp.get("items") or []:
        bundle["subscriptions_sample"].append(
            {
                "group_id": it.get("id"),
                "name": it.get("name"),
                "screen_name": it.get("screen_name"),
                "type": it.get("type"),
                "description": it.get("description"),
                "status": it.get("status"),
                "photo_100": it.get("photo_100"),
                "photo_200": it.get("photo_200"),
            }
        )

    pg = by_method.get("photos.get", {}).get("response") or {}
    for it in pg.get("items") or []:
        bundle["photos"].append(
            {
                "photo_id": it.get("id"),
                "album_id": it.get("album_id"),
                "owner_id": it.get("owner_id"),
                "date": it.get("date"),
                "square_crop": it.get("square_crop"),
                "url_base": (it.get("orig_photo") or {}).get("url"),
                "sizes_json": json.dumps(it.get("sizes") or [], ensure_ascii=False),
            }
        )

    vg = by_method.get("video.get", {}).get("response") or {}
    for it in vg.get("items") or []:
        img = it.get("image") or []
        img0 = img[0] if img else {}
        bundle["videos_sample"].append(
            {
                "owner_id": it.get("owner_id"),
                "video_id": it.get("id"),
                "date": it.get("date"),
                "title": it.get("title"),
                "description": it.get("description"),
                "duration": it.get("duration"),
                "views": it.get("views"),
                "comments": it.get("comments"),
                "likes": (it.get("likes") or {}).get("count"),
                "reposts": (it.get("reposts") or {}).get("count"),
                "player_url": it.get("player"),
                "image_url": img0.get("url"),
                "direct_url": it.get("direct_url"),
                "share_url": it.get("share_url"),
            }
        )

    return bundle


def http_get_initial(
    user_id: int,
    session: requests.Session,
    base_url: str = "https://m.vk.com",
    timeout: int = 15,
) -> str:
    url = f"{base_url.rstrip('/')}/id{abs(user_id)}"
    with log_timer("GET posts initial", logger=_log):
        resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def crawl_profile_by_user_id(
    user_id: int,
    session: requests.Session,
    base_url: str = "https://m.vk.com",
    timeout: int = 15,
) -> dict[str, Any]:
    html0 = http_get_initial(
        user_id=user_id,
        session=session,
        base_url=base_url,
        timeout=timeout,
    )

    parsed0 = parse_api_prefetch_cache(html0)
    if not parsed0:
        _log.info("no prefetch cache for user_id=%s", user_id)
        return {}

    bundle = build_profile_bundle(parsed0)
    _log.info("crawl profile finished for user_id=%s", user_id)
    return bundle
