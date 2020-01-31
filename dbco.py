from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy.sql import select, table

import os

url = 'postgresql://srp:0000@localhost:5432/srp'
engine = create_engine(url, echo=False)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

class Hoge(Base):
    __tablename__ = 'hoge'
    id = Column(Integer, primary_key=True)                      # 連番で振られるid
    name = Column(String, nullable = False)                     # 大会名
    time = Column(String, nullable = False)
    filter = Column(String)
    filter2 = Column(String)

    def __init__(self, name, time):
        self.name = name
        self.time = time

    def __repr__(self):
        return f'<id:{self.id} name:{self.name} time:{self.time} f={self.filter},{self.filter2}>'

# Base.metadata.create_all(bind=engine)
#
# session.add_all([Hoge('c','12'),Hoge('c','3'),Hoge('b','10')])
# session.commit()

# subq = (session.query(Hoge.id).filter(
#     TMilestone.is_done.is_(False),
#     TFolder.company_id == company_id).correlate(TFolder))
#
# query = session.query(TFolder, TMilestone).join(
#     TMilestone, TMilestone.id == subq)
#
# res = query.all()

# d = session.query(Hoge).all()
# d = session.execute(select([Hoge])).fetchall()
d = (select([Hoge]).select_from(table('hoge')))
print(d)

#select * from hoge as m where not exists (select * from hoge where m.name = hoge.name and m.time > hoge.time);
