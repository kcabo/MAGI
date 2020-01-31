import os

import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base

# import scraper
# from task_manager import Takenoko, status, notify_line

from config import Session
session = Session()
Base = declarative_base()

class Meet(Base):
    __tablename__ = 'meets'
    meet_id = db.Column(db.Integer, primary_key=True) # 7桁の大会ID 0119721など0で始まる場合は6桁になる
    meet_name = db.Column(db.String, nullable = False)
    place = db.Column(db.String, nullable = False)   # 会場
    pool = db.Column(db.Integer, nullable = False)   # 0 (短水路) or 1(長水路)
    start = db.Column(db.Integer, nullable = False)  # 大会開始日 20190924 の整数型で表す
    end = db.Column(db.Integer, nullable = False)    # 大会終了日
    area = db.Column(db.Integer, nullable = False)   # 地域(整数2桁)
    year = db.Column(db.Integer, nullable = False)   # 開催年(2桁)

class Record(Base): # 個人種目とリレーの記録
    __tablename__ = 'records'
    record_id = db.Column(db.Integer, primary_key=True)
    meet_id = db.Column(db.Integer, nullable = False)
    event = db.Column(db.Integer, nullable = False)   # 性別・スタイル・距離をつなげた整数
    relay = db.Column(db.Integer, nullable = False)   # 個人種目なら0、リレー一泳なら1,以後2,3,4泳。リレー全体記録なら5。
    rank = db.Column(db.String, nullable = False)     # 順位や棄権、失格情報など
    swimmer_id = db.Column(db.Integer, nullable = False)
    team_id = db.Column(db.Integer, nullable = False)
    time = db.Column(db.Integer, nullable = False)    # タイム。百倍秒数(hecto_seconds)。失格棄権は0。意味不明タイムは-1
    laps = db.Column(db.String, nullable = False)     # ラップタイム。百倍秒数をカンマでつなげる

class Swimmer(Base):
    __tablename__ = 'swimmers'
    swimmer_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable = False)
    sex = db.Column(db.Integer, nullable = False) # 1男子 2女子 3混合 0リレー
    awards = db.Column(db.Integer, nullable = False)
    visit = db.Column(db.Integer, nullable = False)
    read = db.Column(db.String, nullable = False)
    grade_17 = db.Column(db.Integer, nullable = False)
    grade_18 = db.Column(db.Integer, nullable = False)
    grade_19 = db.Column(db.Integer, nullable = False)
    grade_20 = db.Column(db.Integer, nullable = False)
    grade_21 = db.Column(db.Integer, nullable = False)

class Team(Base):
    __tablename__ = 'teams'
    team_id = db.Column(db.Integer, primary_key=True)
    team_name = db.Column(db.String, nullable = False)
    area = db.Column(db.Integer, nullable = False)
    formal_name = db.Column(db.Integer)
    team_read = db.Column(db.String, nullable = False)
    
