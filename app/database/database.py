from config import settings
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# DATABASE_URL = settings.get_db_postgres_url()
DATABASE_URL = settings.get_db_sqlite_url()

engine = create_engine(DATABASE_URL)


class BaseModel(DeclarativeBase): ...


SessionLocal = sessionmaker(autoflush=False, bind=engine)


BaseModel.metadata.create_all(bind=engine)
