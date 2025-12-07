import logging
import random
import statistics
import time
from typing import Any, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

from crawler.config import (
    DEFAULT_BASE_URL,
    OFFSET_STEP,
    REQUEST_TIMEOUT,
    REQUEST_SLEEP,
    JITTER,
)
from crawler.helpers import make_session
from crawler.post_crawling import crawl_posts
from crawler.comment_crawling import crawl_comments_for_post
from crawler.profile_crawling import crawl_profile_by_user_id
from storage.post import save_posts, Post
from storage.comment import save_comments, Comment
from storage.profile import save_profile, Profile
from storage.models import make_session as make_db_session


def run_pipeline_for_group(
    *,
    group_name: str | list[str] | None = None,
    max_posts: int = 100,
    max_comments_per_post: int = 300,
    db_path: str = "vk.sqlite",
    max_workers: int = 4,
    fast_mode: bool = False,
    use_session_pool: bool = True,
    session_pool_size: int = 15,
    collect_from_db: bool = False,
) -> None:
    log = logging.getLogger("vk_crawler.pipeline")
    
    if use_session_pool:
        from crawler.session_pool import get_session_pool, get_rate_limiter
        get_session_pool(pool_size=session_pool_size)
        get_rate_limiter(initial_delay=REQUEST_SLEEP)
        log.info("Session pool: size=%d, adaptive rate limiting ON", session_pool_size)
    
    if fast_mode:
        max_workers = max(max_workers, 8)
        jitter = 0.0
        log.info("FAST MODE: workers=%d, minimal delays", max_workers)
    else:
        jitter = JITTER
    
    posts = []
    if collect_from_db:
        log.info("step1: skipped (collect_from_db=True, will use posts from DB)")
    elif group_name is None:
        log.info("step1: skipped (no group_name provided)")
    else:
        log.info("step1: crawl posts for %s (max_posts=%s)", group_name, max_posts)
    
    def fetch_posts_for_group(gname: str) -> dict[str, Any]:
        s = make_session(use_pool=use_session_pool)
        t0 = time.perf_counter()
        try:
            posts = crawl_posts(
                gname,
                session=s,
                max_posts=max_posts,
                base_url=DEFAULT_BASE_URL,
                offset=OFFSET_STEP,
                timeout=REQUEST_TIMEOUT,
                request_sleep=REQUEST_SLEEP,
                jitter=JITTER,
            )
            dt = time.perf_counter() - t0
            return {
                "group": gname,
                "posts": posts,
                "elapsed": dt,
                "count": len(posts),
            }
        finally:
            try:
                s.close()
            except Exception:
                pass
    
    if isinstance(group_name, (list, tuple)):
        groups = list(group_name)
        log.info("step1: crawling posts from %d groups in parallel", len(groups))
        step1_t0 = time.perf_counter()
        results = parallel_map(
            groups,
            fetch_posts_for_group,
            max_workers=max_workers,
            jitter_range=(0.0, jitter) if jitter > 0 else None,
            log=log,
        )
        step1_dt = time.perf_counter() - step1_t0
        
        posts = []
        for r in results:
            if r and r["posts"]:
                posts.extend(r["posts"])
                log.info("  group=%s: fetched %d posts in %.2fs", 
                        r["group"], r["count"], r["elapsed"])
        
        log.info("step1 done: total groups=%d, posts=%d, wall=%.2fs", 
                len(groups), len(posts), step1_dt)
    else:
        if group_name:
            result = fetch_posts_for_group(group_name)
            posts = result["posts"]
            log.info("step1 done: saved posts = %d (%.2fs)", len(posts), result["elapsed"])
    
    if posts:
        save_posts(posts, db_path=db_path)
    
    if collect_from_db:
        log.info("step2: collecting comments from DB posts without comments")
        db_session = make_db_session(db_path)
        try:
            from sqlalchemy import and_
            
            subq = (
                db_session.query(Comment.owner_id, Comment.post_id)
                .filter(
                    and_(
                        Comment.owner_id == Post.owner_id,
                        Comment.post_id == Post.post_id
                    )
                )
                .exists()
            )
            
            posts_without_comments = (
                db_session.query(Post.owner_id, Post.post_id)
                .filter(~subq)
                .all()
            )
            
            post_keys = [(owner_id, post_id) for owner_id, post_id in posts_without_comments]
            log.info("Found %d posts without comments in DB", len(post_keys))
        finally:
            db_session.close()
    else:
        all_commenters: set[int] = set()
        post_keys = list(_iter_unique_posts(posts))
        log.info("step2: crawl comments for %d posts", len(post_keys))

    def fetch_comments_for_post(key: tuple[int, int]) -> dict[str, Any]:
        owner_id, post_id = key
        s = make_session(use_pool=use_session_pool)
        
        if use_session_pool:
            from crawler.session_pool import get_rate_limiter
            get_rate_limiter().wait()
        
        t0 = time.perf_counter()
        try:
            comments = crawl_comments_for_post(
                post_id=post_id,
                owner_id=owner_id,
                session=s,
                max_comments=max_comments_per_post,
                base_url=DEFAULT_BASE_URL,
                offset=OFFSET_STEP,
                timeout=REQUEST_TIMEOUT,
                request_sleep=REQUEST_SLEEP,
                jitter=JITTER,
            )
            dt = time.perf_counter() - t0
            
            if use_session_pool:
                from crawler.session_pool import get_rate_limiter
                get_rate_limiter().report_success()
            
            return {
                "owner_id": owner_id,
                "post_id": post_id,
                "comments": comments,
                "elapsed": dt,
                "count": len(comments),
                "rate": (len(comments) / dt) if dt > 0 else 0.0,
            }
        except Exception as e:
            if use_session_pool:
                from crawler.session_pool import get_rate_limiter
                resp = getattr(e, 'response', None)
                if resp and hasattr(resp, 'status_code'):
                    get_rate_limiter().report_error(resp.status_code)
                else:
                    get_rate_limiter().report_error()
            raise
        finally:
            try:
                if not use_session_pool:
                    s.close()
            except Exception:
                pass

    step2_t0 = time.perf_counter()
    results = parallel_map(
        post_keys,
        fetch_comments_for_post,
        max_workers=max_workers,
        jitter_range=(0.0, jitter * 0.5) if jitter > 0 else None,
        log=log,
    )
    step2_dt = time.perf_counter() - step2_t0

    total_comments = 0
    all_commenters: set[int] = set()
    per_post_rates = []
    per_post_elapsed = []
    slowest = []

    all_commenters: set[int] = set()
    for r in results:
        if not r:
            continue
        comments = r["comments"]
        save_comments(comments, db_path=db_path)
        total_comments += r["count"]
        per_post_rates.append(r["rate"])
        per_post_elapsed.append(r["elapsed"])
        slowest.append((r["elapsed"], r["owner_id"], r["post_id"], r["count"]))
        all_commenters |= _collect_from_ids(comments)

    posts_done = len(results)
    overall_rate = (total_comments / step2_dt) if step2_dt > 0 else 0.0
    p50_rate = statistics.median(per_post_rates) if per_post_rates else 0.0
    p90_rate = (
        statistics.quantiles(per_post_rates, n=10)[8]
        if len(per_post_rates) >= 10
        else max(per_post_rates or [0.0])
    )
    p50_time = statistics.median(per_post_elapsed) if per_post_elapsed else 0.0
    p90_time = (
        statistics.quantiles(per_post_elapsed, n=10)[8]
        if len(per_post_elapsed) >= 10
        else max(per_post_elapsed or [0.0])
    )

    slowest.sort(reverse=True)
    top_slowest = slowest[:3]

    log.info(
        "step2 done: posts=%d, comments=%d, unique_commenters=%d, wall=%.2fs, "
        "throughput=%.1f c/s, post_rate{p50=%.1f, p90=%.1f} c/s, "
        "post_time{p50=%.2fs, p90=%.2fs}",
        posts_done,
        total_comments,
        len(all_commenters),
        step2_dt,
        overall_rate,
        p50_rate,
        p90_rate,
        p50_time,
        p90_time,
    )
    if top_slowest:
        for rank, (elapsed, owner_id, post_id, cnt) in enumerate(top_slowest, 1):
            log.info(
                "step2 slow #%d: wall=%.2fs comments=%d post=%s_%s",
                rank,
                elapsed,
                cnt,
                owner_id,
                post_id,
            )

    if collect_from_db:
        log.info("step3: collecting profiles from DB comments without profiles")
        db_session = make_db_session(db_path)
        try:
            from sqlalchemy import and_
            
            subq = (
                db_session.query(Profile.user_id)
                .filter(Profile.user_id == Comment.from_id)
                .exists()
            )
            
            user_ids_without_profiles = (
                db_session.query(Comment.from_id)
                .filter(
                    and_(
                        Comment.from_id.isnot(None),
                        Comment.from_id > 0,
                        ~subq
                    )
                )
                .distinct()
                .all()
            )
            
            uids = sorted([uid for (uid,) in user_ids_without_profiles])
            log.info("Found %d users without profiles in DB", len(uids))
        finally:
            db_session.close()
    else:
        uids = sorted([int(u) for u in all_commenters if isinstance(u, int) and u > 0])
        log.info("step3: crawl profiles for %d unique commenters", len(uids))

    def fetch_profile(uid: int) -> dict | None:
        s = make_session(use_pool=use_session_pool)
        
        if use_session_pool:
            from crawler.session_pool import get_rate_limiter
            get_rate_limiter().wait()
        
        t0 = time.perf_counter()
        try:
            prof = crawl_profile_by_user_id(
                user_id=uid,
                session=s,
                base_url=DEFAULT_BASE_URL,
                timeout=REQUEST_TIMEOUT,
            )
            
            if use_session_pool:
                from crawler.session_pool import get_rate_limiter
                get_rate_limiter().report_success()
            
            return {"uid": uid, "profile": prof, "elapsed": time.perf_counter() - t0}
        except Exception as e:
            if use_session_pool:
                from crawler.session_pool import get_rate_limiter
                resp = getattr(e, 'response', None)
                if resp and hasattr(resp, 'status_code'):
                    get_rate_limiter().report_error(resp.status_code)
                else:
                    get_rate_limiter().report_error()
            raise
        finally:
            try:
                if not use_session_pool:
                    s.close()
            except Exception:
                pass

    step3_t0 = time.perf_counter()
    prof_results = parallel_map(
        uids,
        fetch_profile,
        max_workers=max_workers,
        jitter_range=None if fast_mode else (0.0, jitter * 0.3),
        log=log,
    )
    step3_dt = time.perf_counter() - step3_t0

    saved_profiles = 0
    per_item_elapsed = []
    slowest = []

    for r in prof_results:
        if not r:
            continue
        per_item_elapsed.append(r["elapsed"])
        if r["profile"]:
            save_profile(r["profile"], db_path=db_path)
            saved_profiles += 1
        slowest.append((r["elapsed"], r["uid"], bool(r["profile"])))

    total_tasks = len(prof_results)
    overall_rate = (saved_profiles / step3_dt) if step3_dt > 0 else 0.0
    p50_time = statistics.median(per_item_elapsed) if per_item_elapsed else 0.0
    p90_time = (
        statistics.quantiles(per_item_elapsed, n=10)[8]
        if len(per_item_elapsed) >= 10
        else max(per_item_elapsed or [0.0])
    )

    slowest.sort(reverse=True)
    top_slowest = slowest[:3]

    log.info(
        "step3 done: users=%d, profiles_saved=%d, wall=%.2fs, throughput=%.2f prof/s, "
        "profile_time{p50=%.2fs, p90=%.2fs}",
        total_tasks,
        saved_profiles,
        step3_dt,
        overall_rate,
        p50_time,
        p90_time,
    )
    for rank, (elapsed, uid, ok) in enumerate(top_slowest, 1):
        log.info(
            "step3 slow #%d: wall=%.2fs uid=%s saved=%s",
            rank,
            elapsed,
            uid,
            "yes" if ok else "no",
        )

    if collect_from_db:
        log.info(
            "pipeline completed (from DB): comments=%d, profiles_saved=%d",
            total_comments,
            saved_profiles,
        )
    else:
        log.info(
            "pipeline completed: posts=%d, comments=%d, commenters=%d, profiles_saved=%d",
            len(posts),
            total_comments,
            len(uids),
            saved_profiles,
        )


def parallel_map(
    items: list[Any],
    fn: Any,
    *,
    max_workers: int = 4,
    jitter_range: tuple[float, float] | None = None,
    log: logging.Logger | None = None,
) -> list[Any]:
    logger = log or logging.getLogger(__name__)

    results: list[Any] = []
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="crawl") as ex:
        futures = []

        def wrapped(item):
            if jitter_range:
                lo, hi = jitter_range
                if hi > 0:
                    time.sleep(random.uniform(lo, hi))
            return fn(item)

        for it in items:
            futures.append(ex.submit(wrapped, it))

        for fut in as_completed(futures):
            try:
                res = fut.result()
                results.append(res)
            except Exception:
                logger.exception("parallel task failed")

    return results


def _iter_unique_posts(posts: list[dict]) -> Iterable[tuple[int, int]]:
    seen: set[tuple[int, int]] = set()
    for p in posts:
        owner_id = p.get("owner_id")
        post_id = p.get("post_id")
        if owner_id is None or post_id is None:
            continue
        key = (int(owner_id), int(post_id))
        if key not in seen:
            seen.add(key)
            yield key


def _collect_from_ids(comments: Iterable[dict]) -> set[int]:
    uids: set[int] = set()
    for c in comments:
        from_id = c.get("from_id")
        if from_id and isinstance(from_id, int) and from_id > 0:
            uids.add(from_id)
    return uids
