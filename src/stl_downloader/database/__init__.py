from sqlalchemy.pool import NullPool

from .tables import File, Collection, Base
from sqlalchemy import create_engine

engine = create_engine("sqlite:///loot_studios.db")
