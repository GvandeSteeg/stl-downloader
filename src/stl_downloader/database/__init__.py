from sqlalchemy import create_engine

from .tables import Base, Collection, File

engine = create_engine("sqlite:///loot_studios.db")


def initializer():
    """ensure the parent proc's database connections are not touched
    in the new connection pool"""
    engine.dispose(close=False)
