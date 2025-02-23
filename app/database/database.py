from config import settings
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from typing import Any, Callable

# DATABASE_URL = settings.get_db_postgres_url()
DATABASE_URL = settings.get_db_sqlite_url()

engine = create_engine(DATABASE_URL)


class BaseModel(DeclarativeBase): ...


SessionLocal = sessionmaker(autoflush=False, bind=engine)


BaseModel.metadata.create_all(bind=engine)


def context_session2(func):
    def wrapper(*args, **kwargs):
        try:
            session = SessionLocal()
            func(session, *args, **kwargs)
            session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()

    return wrapper


def context_session(func: Callable[..., Any]) -> Callable[..., Any]:
    """Декоратор для управления сессией SQLAlchemy"""

    def wrapper(*args, **kwargs):
        session = SessionLocal()
        try:
            result = func(session, *args, **kwargs)
            session.commit()
            return result
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    return wrapper
