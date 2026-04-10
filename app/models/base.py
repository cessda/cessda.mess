"""SQLAlchemy declarative base shared by all ORM models.

All models must inherit from `Base` so Alembic can discover them and so SQLAlchemy
knows which tables belong to this application.

Import pattern inside a model file:
    from app.models.base import Base

    class MyModel(Base):
        __tablename__ = "my_table"
        ...
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
