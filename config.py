import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


DB_URL = os.environ.get('DATABASE_URL', 'postgresql://srp:0000@localhost:5432/srp')
LINE_TOKEN = os.environ.get('LINE_NOTIFY_ACCESS_TOKEN', None)
REDIS_URL = os.environ.get('REDIS_URL', 'redis://@localhost:6379/0')

engine = create_engine(DB_URL, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()
