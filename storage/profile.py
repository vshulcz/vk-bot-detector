import time
from typing import Any
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Date,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from storage.models import Base, make_session


class Profile(Base):
    __tablename__ = "profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)

    user_id = Column(Integer, nullable=False, unique=True)
    screen_name = Column(String(255), nullable=True)
    domain = Column(String(255), nullable=True)

    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    nickname = Column(String(255), nullable=True)
    sex = Column(Integer, nullable=True)
    bdate = Column(String(32), nullable=True)
    bdate_visibility = Column(Integer, nullable=True)

    city_id = Column(Integer, nullable=True)
    city_title = Column(String(255), nullable=True)
    country_id = Column(Integer, nullable=True)
    country_title = Column(String(255), nullable=True)
    home_town = Column(String(255), nullable=True)

    verified = Column(Boolean, nullable=True)
    is_sber_verified = Column(Boolean, nullable=True)
    is_tinkoff_verified = Column(Boolean, nullable=True)
    is_esia_verified = Column(Boolean, nullable=True)
    is_nft = Column(Boolean, nullable=True)
    is_followers_mode_on = Column(Boolean, nullable=True)
    no_index = Column(String(64), nullable=True)
    wall_default = Column(String(16), nullable=True)

    status = Column(Text, nullable=True)
    activity = Column(Text, nullable=True)
    about = Column(Text, nullable=True)
    interests = Column(Text, nullable=True)
    books = Column(Text, nullable=True)
    tv = Column(Text, nullable=True)
    quotes = Column(Text, nullable=True)
    games = Column(Text, nullable=True)
    movies = Column(Text, nullable=True)
    music = Column(Text, nullable=True)

    site = Column(String(1024), nullable=True)
    mobile_phone = Column(String(64), nullable=True)
    home_phone = Column(String(64), nullable=True)

    photo_50 = Column(String(1024), nullable=True)
    photo_100 = Column(String(1024), nullable=True)
    photo_200 = Column(String(1024), nullable=True)
    photo_400 = Column(String(1024), nullable=True)
    photo_max = Column(String(1024), nullable=True)
    photo_base = Column(String(1024), nullable=True)
    photo_avg_color = Column(String(16), nullable=True)
    photo_id = Column(String(64), nullable=True)

    cover_photo_url = Column(String(1024), nullable=True)

    online = Column(Boolean, nullable=True)
    last_seen_ts = Column(Integer, nullable=True)
    last_seen_platform = Column(Integer, nullable=True)
    online_app_id = Column(Integer, nullable=True)
    followers_count = Column(Integer, nullable=True)

    collected_at = Column(Integer, nullable=True)
    updated_at = Column(Integer, nullable=True)
    registered_at = Column(Date, nullable=True)

    is_bot = Column(Integer, nullable=True)  # 0 - not bot, 1 - bot

    counters = relationship(
        "ProfileCounters",
        uselist=False,
        cascade="all, delete-orphan",
        back_populates="profile",
    )
    personal = relationship(
        "ProfilePersonal",
        uselist=False,
        cascade="all, delete-orphan",
        back_populates="profile",
    )

    languages = relationship(
        "ProfileLanguage", cascade="all, delete-orphan", back_populates="profile"
    )
    universities = relationship(
        "ProfileUniversity", cascade="all, delete-orphan", back_populates="profile"
    )
    schools = relationship(
        "ProfileSchool", cascade="all, delete-orphan", back_populates="profile"
    )
    careers = relationship(
        "ProfileCareer", cascade="all, delete-orphan", back_populates="profile"
    )

    friends_sample = relationship(
        "ProfileFriendSample", cascade="all, delete-orphan", back_populates="profile"
    )
    followers_sample = relationship(
        "ProfileFollowerSample", cascade="all, delete-orphan", back_populates="profile"
    )
    subscriptions_sample = relationship(
        "ProfileSubscriptionSample",
        cascade="all, delete-orphan",
        back_populates="profile",
    )

    photos = relationship(
        "ProfilePhoto", cascade="all, delete-orphan", back_populates="profile"
    )
    videos_sample = relationship(
        "ProfileVideoSample", cascade="all, delete-orphan", back_populates="profile"
    )

    __table_args__ = (
        Index("ix_profiles_user_id", "user_id"),
        Index("ix_profiles_screen_name", "screen_name"),
    )


class ProfileCounters(Base):
    __tablename__ = "profile_counters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    albums = Column(Integer, nullable=True)
    audios = Column(Integer, nullable=True)
    followers = Column(Integer, nullable=True)
    friends = Column(Integer, nullable=True)
    groups = Column(Integer, nullable=True)
    online_friends = Column(Integer, nullable=True)
    pages = Column(Integer, nullable=True)
    photos = Column(Integer, nullable=True)
    subscriptions = Column(Integer, nullable=True)
    videos = Column(Integer, nullable=True)
    video_playlists = Column(Integer, nullable=True)
    mutual_friends = Column(Integer, nullable=True)
    clips_followers = Column(Integer, nullable=True)
    clips_views = Column(Integer, nullable=True)
    clips_likes = Column(Integer, nullable=True)

    profile = relationship("Profile", back_populates="counters")

    __table_args__ = (UniqueConstraint("profile_id", name="uq_profile_counters_one"),)


class ProfilePersonal(Base):
    __tablename__ = "profile_personal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    alcohol = Column(Integer, nullable=True)
    inspired_by = Column(Text, nullable=True)
    life_main = Column(Integer, nullable=True)
    people_main = Column(Integer, nullable=True)
    religion = Column(String(255), nullable=True)
    religion_id = Column(Integer, nullable=True)
    smoking = Column(Integer, nullable=True)

    profile = relationship("Profile", back_populates="personal")

    __table_args__ = (UniqueConstraint("profile_id", name="uq_profile_personal_one"),)


class ProfileLanguage(Base):
    __tablename__ = "profile_languages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )
    lang = Column(String(128), nullable=False)

    profile = relationship("Profile", back_populates="languages")

    __table_args__ = (
        UniqueConstraint("profile_id", "lang", name="uq_profile_lang"),
        Index("ix_profile_lang_lang", "lang"),
    )


class ProfileUniversity(Base):
    __tablename__ = "profile_universities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    university_id = Column(Integer, nullable=True)
    name = Column(String(255), nullable=True)
    faculty_id = Column(Integer, nullable=True)
    faculty_name = Column(String(255), nullable=True)
    chair_id = Column(Integer, nullable=True)
    chair_name = Column(String(255), nullable=True)
    graduation = Column(Integer, nullable=True)

    profile = relationship("Profile", back_populates="universities")

    __table_args__ = (Index("ix_university_profile", "profile_id"),)


class ProfileSchool(Base):
    __tablename__ = "profile_schools"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    school_id = Column(Integer, nullable=True)
    name = Column(String(255), nullable=True)
    year_from = Column(Integer, nullable=True)
    year_to = Column(Integer, nullable=True)
    year_graduated = Column(Integer, nullable=True)
    clazz = Column(String(64), nullable=True)
    speciality = Column(String(255), nullable=True)

    profile = relationship("Profile", back_populates="schools")

    __table_args__ = (Index("ix_school_profile", "profile_id"),)


class ProfileCareer(Base):
    __tablename__ = "profile_careers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    company = Column(String(255), nullable=True)
    position = Column(String(255), nullable=True)
    city_id = Column(Integer, nullable=True)
    city_title = Column(String(255), nullable=True)
    from_year = Column(Integer, nullable=True)
    until_year = Column(Integer, nullable=True)

    profile = relationship("Profile", back_populates="careers")

    __table_args__ = (Index("ix_career_profile", "profile_id"),)


class ProfileFriendSample(Base):
    __tablename__ = "profile_friends_sample"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    friend_user_id = Column(Integer, nullable=False)
    domain = Column(String(255), nullable=True)
    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    sex = Column(Integer, nullable=True)
    online = Column(Boolean, nullable=True)
    photo_50 = Column(String(1024), nullable=True)
    photo_100 = Column(String(1024), nullable=True)
    photo_200 = Column(String(1024), nullable=True)

    profile = relationship("Profile", back_populates="friends_sample")

    __table_args__ = (
        UniqueConstraint("profile_id", "friend_user_id", name="uq_friend_sample"),
        Index("ix_friend_sample_profile", "profile_id"),
    )


class ProfileFollowerSample(Base):
    __tablename__ = "profile_followers_sample"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    follower_user_id = Column(Integer, nullable=False)
    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    sex = Column(Integer, nullable=True)
    photo_50 = Column(String(1024), nullable=True)
    photo_100 = Column(String(1024), nullable=True)
    photo_200 = Column(String(1024), nullable=True)
    online = Column(Boolean, nullable=True)

    profile = relationship("Profile", back_populates="followers_sample")

    __table_args__ = (
        UniqueConstraint("profile_id", "follower_user_id", name="uq_follower_sample"),
        Index("ix_follower_sample_profile", "profile_id"),
    )


class ProfileSubscriptionSample(Base):
    __tablename__ = "profile_subscriptions_sample"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    group_id = Column(Integer, nullable=False)
    name = Column(String(255), nullable=True)
    screen_name = Column(String(255), nullable=True)
    type = Column(String(32), nullable=True)
    description = Column(Text, nullable=True)
    status = Column(Text, nullable=True)
    photo_100 = Column(String(1024), nullable=True)
    photo_200 = Column(String(1024), nullable=True)

    profile = relationship("Profile", back_populates="subscriptions_sample")

    __table_args__ = (
        UniqueConstraint("profile_id", "group_id", name="uq_sub_sample"),
        Index("ix_sub_sample_profile", "profile_id"),
    )


class ProfilePhoto(Base):
    __tablename__ = "profile_photos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    photo_id = Column(Integer, nullable=True)
    album_id = Column(Integer, nullable=True)
    owner_id = Column(Integer, nullable=True)
    date = Column(Integer, nullable=True)
    square_crop = Column(String(64), nullable=True)

    url_base = Column(String(2048), nullable=True)
    sizes_json = Column(Text, nullable=True)

    profile = relationship("Profile", back_populates="photos")

    __table_args__ = (
        UniqueConstraint("profile_id", "photo_id", name="uq_profile_photo"),
        Index("ix_profile_photo_profile", "profile_id"),
    )


class ProfileVideoSample(Base):
    __tablename__ = "profile_videos_sample"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(
        Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False
    )

    owner_id = Column(Integer, nullable=False)
    video_id = Column(Integer, nullable=False)
    date = Column(Integer, nullable=True)
    title = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    duration = Column(Integer, nullable=True)
    views = Column(Integer, nullable=True)
    comments = Column(Integer, nullable=True)
    likes = Column(Integer, nullable=True)
    reposts = Column(Integer, nullable=True)

    player_url = Column(String(2048), nullable=True)
    image_url = Column(String(2048), nullable=True)
    direct_url = Column(String(2048), nullable=True)
    share_url = Column(String(2048), nullable=True)

    profile = relationship("Profile", back_populates="videos_sample")

    __table_args__ = (
        UniqueConstraint("profile_id", "owner_id", "video_id", name="uq_video_sample"),
        Index("ix_video_sample_profile", "profile_id"),
    )


def upsert_profile(session, p: dict[str, Any]) -> Profile:
    user_id = p["user_id"]
    rec: Profile | None = (
        session.query(Profile).filter(Profile.user_id == user_id).one_or_none()
    )
    is_new = rec is None
    if is_new:
        rec = Profile(user_id=user_id)
        session.add(rec)

    for k, v in p.items():
        if k == "user_id":
            continue
        setattr(rec, k, v)

    rec.updated_at = int(time.time())
    if is_new:
        session.flush()
    return rec


def replace_profile_children(session, profile: Profile, bundle: dict[str, Any]) -> None:
    session.query(ProfileCounters).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfilePersonal).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfileLanguage).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfileUniversity).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfileSchool).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfileCareer).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfileFriendSample).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfileFollowerSample).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfileSubscriptionSample).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfilePhoto).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )
    session.query(ProfileVideoSample).filter_by(profile_id=profile.id).delete(
        synchronize_session=False
    )

    cnt = bundle.get("counters") or {}
    if cnt:
        session.add(ProfileCounters(profile_id=profile.id, **cnt))

    pers = bundle.get("personal") or {}
    if pers:
        session.add(ProfilePersonal(profile_id=profile.id, **pers))

    for lang in set(bundle.get("languages") or []):
        if not lang:
            continue
        session.add(ProfileLanguage(profile_id=profile.id, lang=lang))

    for u in bundle.get("universities") or []:
        session.add(
            ProfileUniversity(
                profile_id=profile.id,
                university_id=u.get("id"),
                name=u.get("name"),
                faculty_id=u.get("faculty"),
                faculty_name=u.get("faculty_name"),
                chair_id=u.get("chair"),
                chair_name=u.get("chair_name"),
                graduation=u.get("graduation"),
            )
        )

    for s in bundle.get("schools") or []:
        session.add(
            ProfileSchool(
                profile_id=profile.id,
                school_id=s.get("id"),
                name=s.get("name"),
                year_from=s.get("year_from"),
                year_to=s.get("year_to"),
                year_graduated=s.get("year_graduated"),
                clazz=s.get("class"),
                speciality=s.get("speciality"),
            )
        )

    for c in bundle.get("careers") or []:
        session.add(
            ProfileCareer(
                profile_id=profile.id,
                company=c.get("company"),
                position=c.get("position"),
                city_id=c.get("city_id"),
                city_title=c.get("city_name"),
                from_year=c.get("from"),
                until_year=c.get("until"),
            )
        )

    for f in bundle.get("friends_sample") or []:
        session.add(ProfileFriendSample(profile_id=profile.id, **f))

    for f in bundle.get("followers_sample") or []:
        session.add(ProfileFollowerSample(profile_id=profile.id, **f))

    for g in bundle.get("subscriptions_sample") or []:
        session.add(ProfileSubscriptionSample(profile_id=profile.id, **g))

    for ph in bundle.get("photos") or []:
        session.add(ProfilePhoto(profile_id=profile.id, **ph))

    for v in bundle.get("videos_sample") or []:
        session.add(ProfileVideoSample(profile_id=profile.id, **v))


def save_profile(bundle: dict[str, Any], db_path: str = "vk.sqlite"):
    session = make_session(db_path)
    p = bundle.get("profile") or {}
    try:
        if not p.get("user_id"):
            raise
        with session.no_autoflush:
            rec = upsert_profile(session, p)
            replace_profile_children(session, rec, bundle)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
