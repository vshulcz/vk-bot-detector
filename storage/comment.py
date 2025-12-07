from typing import Any
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
from storage.post import Post


class Comment(Base):
    __tablename__ = "comments"
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_row_id = Column(
        Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=True
    )
    owner_id = Column(Integer, nullable=False)
    post_id = Column(Integer, nullable=False)
    comment_id = Column(Integer, nullable=False)

    from_id = Column(Integer, nullable=True)
    author_name = Column(String(256), nullable=True)
    reply_to_comment_id = Column(Integer, nullable=True)

    date_text = Column(String(128), nullable=True)
    timestamp = Column(Integer, nullable=False, default=0)

    text = Column(Text, nullable=True)
    likes = Column(Integer, nullable=False, default=0)

    is_deleted = Column(Boolean, nullable=True)
    is_edited = Column(Boolean, nullable=True)

    collected_at = Column(Integer, nullable=True)

    attachments = relationship(
        "CommentAttachment", cascade="all, delete-orphan", back_populates="comment"
    )
    hashtags = relationship(
        "CommentHashtag", cascade="all, delete-orphan", back_populates="comment"
    )
    mentions = relationship(
        "CommentMention", cascade="all, delete-orphan", back_populates="comment"
    )
    urls = relationship(
        "CommentURL", cascade="all, delete-orphan", back_populates="comment"
    )

    __table_args__ = (
        UniqueConstraint("owner_id", "post_id", "comment_id", name="uq_comment_key"),
        Index("ix_comment_post", "owner_id", "post_id"),
        Index("ix_comment_ts", "timestamp"),
    )


class CommentAttachment(Base):
    __tablename__ = "comment_attachments"
    id = Column(Integer, primary_key=True, autoincrement=True)
    comment_id_fk = Column(
        Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=False
    )
    kind = Column(String(16), nullable=False)
    href = Column(String(1024))
    src = Column(String(1024))
    extra_json = Column(Text)

    comment = relationship("Comment", back_populates="attachments")

    __table_args__ = (
        UniqueConstraint(
            "comment_id_fk", "kind", "href", "src", name="uq_comment_attach"
        ),
        Index("ix_comment_attach_kind", "kind"),
    )


class CommentHashtag(Base):
    __tablename__ = "comment_hashtags"
    id = Column(Integer, primary_key=True, autoincrement=True)
    comment_id_fk = Column(
        Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=False
    )
    tag = Column(String(128), nullable=False)
    comment = relationship("Comment", back_populates="hashtags")
    __table_args__ = (
        UniqueConstraint("comment_id_fk", "tag", name="uq_comment_tag"),
        Index("ix_comment_tag", "tag"),
    )


class CommentMention(Base):
    __tablename__ = "comment_mentions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    comment_id_fk = Column(
        Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=False
    )
    handle = Column(String(128), nullable=False)
    comment = relationship("Comment", back_populates="mentions")
    __table_args__ = (
        UniqueConstraint("comment_id_fk", "handle", name="uq_comment_handle"),
        Index("ix_comment_handle", "handle"),
    )


class CommentURL(Base):
    __tablename__ = "comment_urls"
    id = Column(Integer, primary_key=True, autoincrement=True)
    comment_id_fk = Column(
        Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=False
    )
    url = Column(String(2048), nullable=False)
    comment = relationship("Comment", back_populates="urls")
    __table_args__ = (
        UniqueConstraint("comment_id_fk", "url", name="uq_comment_url"),
        Index("ix_comment_url", "url"),
    )


def upsert_comment(session, c: dict[str, Any]) -> Comment:
    rec: Comment | None = (
        session.query(Comment)
        .filter(
            Comment.owner_id == c["owner_id"],
            Comment.post_id == c["post_id"],
            Comment.comment_id == c["comment_id"],
        )
        .one_or_none()
    )
    is_new = rec is None
    if is_new:
        rec = Comment(
            owner_id=c["owner_id"], post_id=c["post_id"], comment_id=c["comment_id"]
        )
        session.add(rec)

    if rec.post_row_id is None:
        post_row = (
            session.query(Post)
            .filter(Post.owner_id == c["owner_id"], Post.post_id == c["post_id"])
            .one_or_none()
        )
        if post_row:
            rec.post_row_id = post_row.id

    rec.from_id = c.get("from_id")
    rec.author_name = c.get("author_name")
    rec.reply_to_comment_id = c.get("reply_to_comment_id")
    rec.date_text = c.get("date_text")
    if c.get("timestamp") is not None:
        rec.timestamp = c["timestamp"]
    rec.text = c.get("text")
    rec.likes = c.get("likes", rec.likes or 0)
    rec.is_deleted = c.get("is_deleted")
    rec.is_edited = c.get("is_edited")
    rec.collected_at = c.get("collected_at") or rec.collected_at

    if is_new:
        session.flush()
    return rec


def replace_comment_children(session, rec: Comment, c: dict[str, Any]):
    session.query(CommentAttachment).filter_by(comment_id_fk=rec.id).delete(
        synchronize_session=False
    )
    session.query(CommentHashtag).filter_by(comment_id_fk=rec.id).delete(
        synchronize_session=False
    )
    session.query(CommentMention).filter_by(comment_id_fk=rec.id).delete(
        synchronize_session=False
    )
    session.query(CommentURL).filter_by(comment_id_fk=rec.id).delete(
        synchronize_session=False
    )

    att = c.get("attachments") or {}
    for src in att.get("images") or []:
        session.add(CommentAttachment(comment_id_fk=rec.id, kind="image", src=src))
    for href in att.get("videos") or []:
        session.add(CommentAttachment(comment_id_fk=rec.id, kind="video", href=href))
    for href in att.get("outlinks") or []:
        session.add(CommentAttachment(comment_id_fk=rec.id, kind="outlink", href=href))

    tf = c.get("text_features") or {}

    def _normalize(value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        return value or None

    seen_tags: set[str] = set()
    for raw_tag in tf.get("hashtags") or []:
        norm = _normalize(raw_tag)
        if not norm or norm in seen_tags:
            continue
        seen_tags.add(norm)
        session.add(CommentHashtag(comment_id_fk=rec.id, tag=norm))
    for m in sorted(set(tf.get("mentions") or [])):
        session.add(CommentMention(comment_id_fk=rec.id, handle=m))
    for u in sorted(set(tf.get("urls") or [])):
        session.add(CommentURL(comment_id_fk=rec.id, url=u))


def save_comments(comments: list[dict[str, Any]], db_path: str = "vk.sqlite"):
    session = make_session(db_path)
    try:
        deduped: dict[tuple[int, int, int], dict[str, Any]] = {}
        for c in comments:
            if not {"owner_id", "post_id", "comment_id"} <= set(c.keys()):
                continue
            key = (int(c["owner_id"]), int(c["post_id"]), int(c["comment_id"]))
            deduped[key] = c

        with session.no_autoflush:
            for c in deduped.values():
                rec = upsert_comment(session, c)
                replace_comment_children(session, rec, c)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
