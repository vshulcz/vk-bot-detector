import json
from typing import Any, Dict, Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


def init_db(db_path: str = "vk.sqlite"):
    eng = create_engine(f"sqlite:///{db_path}", future=True)
    Base.metadata.create_all(eng)
    return eng


def make_session(db_path: str = "vk_posts.sqlite"):
    eng = init_db(db_path)
    return sessionmaker(bind=eng, future=True)()


def json_or_none(obj: Optional[Dict[str, Any]]) -> Optional[str]:
    if obj is None:
        return None
    return json.dumps(obj, ensure_ascii=False)
