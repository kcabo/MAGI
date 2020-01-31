import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

db_url = os.environ.get('DATABASE_URL', 'postgresql://srp:0000@localhost:5432/srp')
line_token = os.environ.get('LINE_NOTIFY_ACCESS_TOKEN', None)

engine = create_engine(db_url, echo=False)
Session = sessionmaker(bind=engine)
