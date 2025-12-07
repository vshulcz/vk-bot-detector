from typing import Any, Dict, List, Optional
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    Text,
    Boolean,
    Index,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from storage.models import Base, make_session


class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, autoincrement=True)

    owner_id = Column(Integer, nullable=False)
    post_id = Column(Integer, nullable=False)
    url = Column(String(512), nullable=True)

    date_text = Column(String(128), nullable=True)
    timestamp = Column(Integer, nullable=False, default=0)

    text = Column(Text, nullable=True)

    likes = Column(Integer, nullable=False, default=0)
    reposts = Column(Integer, nullable=False, default=0)
    comments = Column(Integer, nullable=False, default=0)
    views = Column(Integer, nullable=False, default=0)

    pinned = Column(Boolean, nullable=True)
    is_comments_closed = Column(Boolean, nullable=True)

    collected_at = Column(Integer, nullable=True)

    attachments = relationship(
        "PostAttachment", cascade="all, delete-orphan", back_populates="post"
    )
    hashtags = relationship(
        "PostHashtag", cascade="all, delete-orphan", back_populates="post"
    )
    mentions = relationship(
        "PostMention", cascade="all, delete-orphan", back_populates="post"
    )
    urls = relationship("PostURL", cascade="all, delete-orphan", back_populates="post")

    __table_args__ = (
        UniqueConstraint("owner_id", "post_id", name="uq_posts_owner_post"),
        Index("ix_posts_owner_post", "owner_id", "post_id"),
        Index("ix_posts_timestamp", "timestamp"),
    )


class PostAttachment(Base):
    __tablename__ = "post_attachments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(
        Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False
    )

    kind = Column(String(16), nullable=False)
    href = Column(String(1024))
    src = Column(String(1024))
    extra_json = Column(Text)

    post = relationship("Post", back_populates="attachments")

    __table_args__ = (
        UniqueConstraint("post_id", "kind", "href", "src", name="uq_attachment_dedup"),
        Index("ix_attach_post", "post_id"),
        Index("ix_attach_kind", "kind"),
    )


class PostHashtag(Base):
    __tablename__ = "post_hashtags"
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(
        Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False
    )
    tag = Column(String(128), nullable=False)

    post = relationship("Post", back_populates="hashtags")

    __table_args__ = (
        UniqueConstraint("post_id", "tag", name="uq_post_tag"),
        Index("ix_hashtag_tag", "tag"),
    )


class PostMention(Base):
    __tablename__ = "post_mentions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(
        Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False
    )
    handle = Column(String(128), nullable=False)

    post = relationship("Post", back_populates="mentions")

    __table_args__ = (
        UniqueConstraint("post_id", "handle", name="uq_post_handle"),
        Index("ix_mention_handle", "handle"),
    )


class PostURL(Base):
    __tablename__ = "post_urls"
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(
        Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False
    )
    url = Column(String(2048), nullable=False)

    post = relationship("Post", back_populates="urls")

    __table_args__ = (
        UniqueConstraint("post_id", "url", name="uq_post_url"),
        Index("ix_url_url", "url"),
    )


def upsert_post(session, p: Dict[str, Any]) -> Post:
    owner_id, post_id = p["owner_id"], p["post_id"]
    rec: Optional[Post] = (
        session.query(Post)
        .filter(Post.owner_id == owner_id, Post.post_id == post_id)
        .one_or_none()
    )
    is_new = rec is None
    if is_new:
        rec = Post(owner_id=owner_id, post_id=post_id)
        session.add(rec)

    rec.url = p.get("url") or rec.url
    rec.date_text = p.get("date_text") or rec.date_text
    if p.get("timestamp"):
        rec.timestamp = p["timestamp"]
    if "text" in p:
        rec.text = p["text"]
    ctr = p.get("counters") or {}
    rec.likes = ctr.get("likes", rec.likes or 0)
    rec.reposts = ctr.get("reposts", rec.reposts or 0)
    rec.comments = ctr.get("comments", rec.comments or 0)
    rec.views = ctr.get("views", rec.views or 0)
    flg = p.get("flags") or {}
    if "pinned" in flg and flg["pinned"] is not None:
        rec.pinned = flg["pinned"]
    if "is_comments_closed" in flg and flg["is_comments_closed"] is not None:
        rec.is_comments_closed = flg["is_comments_closed"]
    if "collected_at" in p and p["collected_at"]:
        rec.collected_at = p["collected_at"]

    if is_new:
        session.flush()
    return rec


def replace_children(session, rec: Post, p: Dict[str, Any]):
    session.query(PostAttachment).filter_by(post_id=rec.id).delete(
        synchronize_session=False
    )
    session.query(PostHashtag).filter_by(post_id=rec.id).delete(
        synchronize_session=False
    )
    session.query(PostMention).filter_by(post_id=rec.id).delete(
        synchronize_session=False
    )
    session.query(PostURL).filter_by(post_id=rec.id).delete(synchronize_session=False)

    att = p.get("attachments") or {}
    imgs = att.get("images") or []
    vids = att.get("videos") or []
    outs = att.get("outlinks") or []

    for src in imgs:
        session.add(
            PostAttachment(
                post_id=rec.id, kind="image", src=src, href=None, extra_json=None
            )
        )
    for href in vids:
        session.add(
            PostAttachment(
                post_id=rec.id, kind="video", href=href, src=None, extra_json=None
            )
        )
    for href in outs:
        session.add(
            PostAttachment(
                post_id=rec.id, kind="outlink", href=href, src=None, extra_json=None
            )
        )

    tf = p.get("text_features") or {}
    for tag in set([t.lower() for t in (tf.get("hashtags") or [])]):
        session.add(PostHashtag(post_id=rec.id, tag=tag))
    for h in set([m for m in (tf.get("mentions") or [])]):
        session.add(PostMention(post_id=rec.id, handle=h))
    for u in set([u for u in (tf.get("urls") or [])]):
        session.add(PostURL(post_id=rec.id, url=u))


def save_posts(posts: List[Dict[str, Any]], db_path: str = "vk.sqlite"):
    session = make_session(db_path)
    try:
        with session.no_autoflush:
            for p in posts:
                if "owner_id" not in p or "post_id" not in p:
                    continue
                rec = upsert_post(session, p)
                replace_children(session, rec, p)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
