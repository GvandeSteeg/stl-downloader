from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Collection(Base):
    __tablename__ = "collection"

    url = Column(String, primary_key=True)
    name = Column(String, nullable=True)
    skip = Column(Boolean, default=False)

    files = relationship("File", backref="collection", cascade="all, delete-orphan")


class File(Base):
    __tablename__ = "file"

    name = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    path = Column(String, nullable=False)
    changed = Column(DateTime)
    downloaded = Column(Boolean, default=False)
    uploaded = Column(Boolean, default=False)
    collection_name = Column(String, ForeignKey(Collection.name), primary_key=True)
